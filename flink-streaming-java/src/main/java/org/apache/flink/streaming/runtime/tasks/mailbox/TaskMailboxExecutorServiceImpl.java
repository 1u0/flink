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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of an executor service build around a mailbox-based execution model.
 */
public class TaskMailboxExecutorServiceImpl extends AbstractExecutorService implements TaskMailboxExecutorService {

	/** Optional reference to the thread that executes the mailbox letters. For debugging. */
	@Nullable
	private final Thread taskMainThread;

	/** The mailbox that manages the submitted runnable objects. */
	@Nonnull
	private final Mailbox mailbox;

	public TaskMailboxExecutorServiceImpl(@Nonnull Mailbox mailbox) {
		this(mailbox, null);
	}

	private TaskMailboxExecutorServiceImpl(@Nonnull Mailbox mailbox, @Nullable Thread taskMainThread) {
		this.mailbox = mailbox;
		this.taskMainThread = taskMainThread;
	}

	@Override
	public void execute(@Nonnull Runnable command) {
		assertIsNotMailboxThread();
		try {
			mailbox.putMail(command);
		} catch (InterruptedException irex) {
			Thread.currentThread().interrupt();
			throw new RejectedExecutionException("Sender thread was interrupted while blocking on mailbox.", irex);
		} catch (MailboxStateException mbex) {
			throw new RejectedExecutionException(mbex);
		}
	}

	@Override
	public boolean tryExecute(Runnable command) {
		try {
			return mailbox.tryPutMail(command);
		} catch (MailboxStateException e) {
			throw new RejectedExecutionException(e);
		}
	}

	@Override
	public void waitUntilCanExecute() throws InterruptedException {
		assertIsNotMailboxThread();
		mailbox.waitUntilHasCapacity();
	}

	@Override
	public void yield() throws InterruptedException, IllegalStateException {
		assertIsMailboxThread();
		try {
			Runnable runnable = mailbox.takeMail();
			runnable.run();
		} catch (MailboxStateException e) {
			throw new IllegalStateException("Mailbox can no longer supply runnables for yielding.", e);
		}
	}

	@Override
	public boolean tryYield() throws IllegalStateException {
		assertIsMailboxThread();
		try {
			Optional<Runnable> runnableOptional = mailbox.tryTakeMail();
			if (runnableOptional.isPresent()) {
				runnableOptional.get().run();
				return true;
			} else {
				return false;
			}
		} catch (MailboxStateException e) {
			throw new IllegalStateException("Mailbox can no longer supply runnables for yielding.", e);
		}
	}

	@Override
	public void shutdown() {
		mailbox.quiesce();
	}

	@Nonnull
	@Override
	public List<Runnable> shutdownNow() {
		return mailbox.close();
	}

	@Override
	public boolean isShutdown() {
		return mailbox.getState() != Mailbox.State.OPEN;
	}

	@Override
	public boolean isTerminated() {
		return mailbox.getState() == Mailbox.State.CLOSED;
	}

	@Override
	public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
		return isTerminated();
	}

	/**
	 * Returns the mailbox that manages the execution order.
	 *
	 * @return the mailbox.
	 */
	@Nonnull
	public Mailbox getMailbox() {
		return mailbox;
	}

	private void assertIsMailboxThread() {
		assert (taskMainThread == null || taskMainThread == Thread.currentThread());
	}

	private void assertIsNotMailboxThread() {
		assert (taskMainThread == null || taskMainThread != Thread.currentThread());
	}
}
