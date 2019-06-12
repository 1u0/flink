/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link TaskMailboxExecutorServiceImpl}.
 */
public class TaskMailboxExecutorServiceImplTest {

	private TaskMailboxExecutorServiceImpl mailboxExecutorService;
	private MailboxImpl mailbox;

	@Before
	public void setUp() throws Exception {
		this.mailbox = new MailboxImpl();
		this.mailbox.open();
		this.mailboxExecutorService = new TaskMailboxExecutorServiceImpl(mailbox);
	}

	@Test
	public void testOpsAndLifecycle() throws Exception {
		Assert.assertFalse(mailboxExecutorService.isShutdown());
		Assert.assertFalse(mailboxExecutorService.isTerminated());
		final TestRunnable testRunnable = new TestRunnable();
		Assert.assertTrue(mailboxExecutorService.tryExecute(testRunnable));
		Assert.assertEquals(testRunnable, mailbox.tryTakeMail().get());
		mailboxExecutorService.execute(testRunnable);
		Assert.assertEquals(testRunnable, mailbox.takeMail());
		final TestRunnable yieldRun = new TestRunnable();
		final TestRunnable leftoverRun = new TestRunnable();
		Assert.assertTrue(mailboxExecutorService.tryExecute(yieldRun));
		Future<?> leftoverFuture = mailboxExecutorService.submit(leftoverRun);
		mailboxExecutorService.shutdown();
		Assert.assertTrue(mailboxExecutorService.isShutdown());
		Assert.assertFalse(mailboxExecutorService.isTerminated());

		try {
			mailboxExecutorService.execute(testRunnable);
			Assert.fail("execution should not work after shutdown().");
		} catch (RejectedExecutionException expected) {
		}

		try {
			mailboxExecutorService.tryExecute(testRunnable);
			Assert.fail("execution should not work after shutdown().");
		} catch (RejectedExecutionException expected) {
		}

		Assert.assertTrue(mailboxExecutorService.tryYield());
		Assert.assertEquals(Thread.currentThread(), yieldRun.wasExecutedBy());

		Assert.assertFalse(mailboxExecutorService.awaitTermination(1L, TimeUnit.SECONDS));
		Assert.assertFalse(leftoverFuture.isDone());

		List<Runnable> leftoverTasks = mailboxExecutorService.shutdownNow();
		Assert.assertEquals(1, leftoverTasks.size());
		Assert.assertFalse(leftoverFuture.isCancelled());
		FutureUtils.cancelRunnableFutures(leftoverTasks);
		Assert.assertTrue(leftoverFuture.isCancelled());

		try {
			mailboxExecutorService.tryYield();
			Assert.fail("yielding should not work after shutdown().");
		} catch (IllegalStateException expected) {
		}

		try {
			mailboxExecutorService.yield();
			Assert.fail("yielding should not work after shutdown().");
		} catch (IllegalStateException expected) {
		}
	}

	@Test
	public void testWaitUntilCanExecute() throws Exception {
		while (mailboxExecutorService.tryExecute(() -> {})) {}

		// basic blocking
		final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
		final OneShotLatch runLatch = new OneShotLatch();
		final Runnable waiterRunnable = () -> {
			runLatch.trigger();
			try {
				mailboxExecutorService.waitUntilCanExecute();
			} catch (InterruptedException e) {
				exceptionReference.set(e);
			}
		};
		Thread waiterThread = new Thread(waiterRunnable);
		waiterThread.start();
		runLatch.await();
		Thread.sleep(1L);
		Assert.assertTrue(waiterThread.isAlive());

		mailboxExecutorService.yield();
		waiterThread.join();
		Assert.assertNull(exceptionReference.get());

		// interruption
		runLatch.reset();
		waiterThread = new Thread(waiterRunnable);
		waiterThread.start();
		waiterThread.interrupt();
		waiterThread.join();
		Assert.assertEquals(InterruptedException.class, exceptionReference.get().getClass());
	}

	@Test
	public void testTryYield() throws Exception {
		final TestRunnable testRunnable = new TestRunnable();
		mailboxExecutorService.execute(testRunnable);
		Assert.assertTrue(mailboxExecutorService.tryYield());
		Assert.assertFalse(mailbox.tryTakeMail().isPresent());
		Assert.assertEquals(Thread.currentThread(), testRunnable.wasExecutedBy());
	}

	@Test
	public void testYield() throws Exception {
		AtomicReference<Exception> exceptionReference = new AtomicReference<>();
		OneShotLatch isRunningLatch = new OneShotLatch();

		Thread mbRunner = new Thread(() -> {
			isRunningLatch.trigger();
			try {
				mailboxExecutorService.yield();
			} catch (Exception e) {
				exceptionReference.set(e);
			}
		});

		mbRunner.start();
		isRunningLatch.await();
		Thread.sleep(1L);
		Assert.assertTrue(mbRunner.isAlive());

		final TestRunnable testRunnable = new TestRunnable();
		mailboxExecutorService.execute(testRunnable);

		mbRunner.join();
		Assert.assertEquals(mbRunner, testRunnable.wasExecutedBy());
		Assert.assertNull(exceptionReference.get());
	}

	/**
	 * Test {@link Runnable} that tracks execution.
	 */
	static class TestRunnable implements Runnable {

		private Thread executedByThread = null;

		@Override
		public void run() {
			Preconditions.checkState(!isExecuted(), "Runnable was already executed before by " + executedByThread);
			executedByThread = Thread.currentThread();
		}

		boolean isExecuted() {
			return executedByThread != null;
		}

		Thread wasExecutedBy() {
			return executedByThread;
		}
	}
}
