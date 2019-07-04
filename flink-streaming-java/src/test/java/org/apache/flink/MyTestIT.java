package org.apache.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** */
public class MyTestIT extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(MyTestIT.class);

	private static DataStreamSource<Tuple3<Integer, Long, String>> sample(StreamExecutionEnvironment environment) {
		return environment.fromElements(
			new Tuple3<>(1, 1L, "Hi"), new Tuple3<>(2, 2L, "Hello"));
	}

	@Test
	public void testAsyncFunction() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
			.setParallelism(1) // to keep entries ordered
			.setStateBackend(new FsStateBackend("file:///users/unk/projects/flink/tmp/checkpoints"))
			.enableCheckpointing(10);

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStreamSource<Integer> source = env.addSource(new MySource(100));
		mapFooBar(source, true)
			.map(new MyCounterMapFunction())
			.addSink(new TestSink<>());

		env.execute();
	}

	private static SingleOutputStreamOperator<String> mapFooBar(DataStreamSource<Integer> source, boolean async) {
		if (async) {
			return AsyncDataStream.orderedWait(
				source, new MyAsyncFunction(), 500L, TimeUnit.MILLISECONDS, 10);
		}
		return source.map(new MyMapFunction());
	}

	private static class MySource implements SourceFunction<Integer>, ListCheckpointed<Integer> {
		private final int limit;

		private volatile int number = 1;
		private volatile boolean cancelled = false;

		private MySource(int limit) {
			this.limit = limit;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (!cancelled && number <= limit) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(number);
					++number;
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			int value = number;
			LOG.warn("Snapshot state (source): {} {}", checkpointId, value);
			return Collections.singletonList(value);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			for (Integer value: state) {
				number = value;
			}
			LOG.warn("Snapshot state (source): {}", number);
		}
	}

	private static class MyAsyncFunction extends RichAsyncFunction<Integer, String> {
		private transient Executor executor;
		private transient MapF f;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			executor = Executors.newSingleThreadExecutor();
			f = new MapF(70);
		}

		@Override
		public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
			executor.execute(() -> {
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				try {
					resultFuture.complete(Collections.singletonList(f.apply(input)));
				} catch (Exception e) {
					resultFuture.completeExceptionally(e);
				}
			});
		}
	}

	private static class MyMapFunction extends RichMapFunction<Integer, String> {
		private transient MapF f;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			f = new MapF(-10);
		}

		@Override
		public String map(Integer value) throws Exception {
			return f.apply(value);
		}
	}

	private static class MapF {
		private final int failP;
		private boolean boom;

		private MapF(int failP) {
			this.failP = failP;
		}

		String apply(int input) {
			if (input % 10 == 0 && ThreadLocalRandom.current().nextInt(100) < failP) {
				boom = true;
				LOG.warn("✨ {}", input);
				throw new RuntimeException("The result is errored for " + input);
			}
			String result = String.valueOf(input);
			if (input % 3 == 0) {
				result += "Foo";
			}
			if (input % 5 == 0) {
				result += "Bar";
			}
			if (boom) {
				result += " ✨";
			}
			return result;
		}
	}

	private static class MyCounterMapFunction extends RichMapFunction<String, Tuple2<Integer, String>>
		implements ListCheckpointed<Integer> {

		private volatile int counter;

		@Override
		public Tuple2<Integer, String> map(String fooBar) throws Exception {
			++counter;
			return new Tuple2<>(counter, fooBar);
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			int value = counter;
			LOG.warn("Snapshot state (counter): {} {}", checkpointId, value);
			return Collections.singletonList(value);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			for (Integer value: state) {
				counter = value;
			}
			LOG.warn("Restore state (counter): {}", counter);
		}
	}

	private static class TestSink<T> extends TwoPhaseCommitSinkFunction<T, List<String>, Void> {
		public TestSink() {
			super(new ListSerializer<String>(StringSerializer.INSTANCE), VoidSerializer.INSTANCE);
		}

		@Override
		protected void invoke(List<String> transaction, T value, Context context) throws Exception {
			transaction.add(String.valueOf(value));
		}

		@Override
		protected List<String> beginTransaction() throws Exception {
			ArrayList<String> transaction = new ArrayList<>();
			String id = String.valueOf(System.identityHashCode(transaction));
			LOG.warn("{}: tx.start", id);
			transaction.add(id);
			return transaction;
		}

		@Override
		protected void preCommit(List<String> transaction) throws Exception {
			if (transaction.size() <= 1) {
				return;
			}
			String id = transaction.get(0);
			LOG.warn("{}: tx.precommit ({})", id, transaction.size() - 1);
			for (String value: transaction) {
				LOG.warn("{}: {}", id, value);
			}
		}

		@Override
		protected void commit(List<String> transaction) {
//			if (transaction.size() <= 1) {
//				return;
//			}
			String id = transaction.get(0);
			LOG.warn("{}: tx.commit ({})", id, transaction.size() - 1);
			for (String value: transaction) {
				LOG.warn("{}: ✔ {}", id, value);
			}
		}

		@Override
		protected void abort(List<String> transaction) {
			if (transaction.size() <= 1) {
				return;
			}
			String id = transaction.get(0);
			LOG.warn("{}: tx.abort ({})", id, transaction.size() - 1);
			transaction.clear();
		}
	}
}
