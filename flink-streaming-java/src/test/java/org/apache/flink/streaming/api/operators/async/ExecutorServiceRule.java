/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.async;

import org.junit.rules.ExternalResource;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A JUnit rule that provides an executor service for each test case.
 *
 * <p><pre>{@code
 * @Rule
 * public ExecutorServiceRule executor = new ExecutorServiceRule(() -> Executors.newFixedThreadPool(3));
 * }
 * </pre>
 */
public class ExecutorServiceRule extends ExternalResource implements ExecutorService {
	private final Supplier<ExecutorService> executorServiceSupplier;
	private ExecutorService executor;
	private long terminationTimeoutInMs = 10000L;

	public ExecutorServiceRule(Supplier<ExecutorService> executorServiceSupplier) {
		this.executorServiceSupplier = executorServiceSupplier;
	}

	@Override
	public void shutdown() {
		getExecutor().shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return getExecutor().shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return getExecutor().isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return getExecutor().isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return getExecutor().awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return getExecutor().submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return getExecutor().submit(task, result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return getExecutor().submit(task);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return getExecutor().invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit) throws InterruptedException {
		return getExecutor().invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return getExecutor().invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(
			Collection<? extends Callable<T>> tasks,
			long timeout,
			TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return getExecutor().invokeAny(tasks, timeout, unit);
	}

	@Override
	public void execute(Runnable command) {
		getExecutor().execute(command);
	}

	@Override
	protected void before() throws Throwable {
		executor = executorServiceSupplier.get();
	}

	@Override
	protected void after() {
		executor.shutdown();

		try {
			if (!executor.awaitTermination(terminationTimeoutInMs, TimeUnit.MILLISECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException interrupted) {
			executor.shutdownNow();

			Thread.currentThread().interrupt();
		}
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void setTerminationTimeoutInMs(long terminationTimeoutInMs) {
		this.terminationTimeoutInMs = terminationTimeoutInMs;
	}

	public long getTerminationTimeoutInMs() {
		return terminationTimeoutInMs;
	}
}
