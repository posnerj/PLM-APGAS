package apgas.impl;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncFuture<R> implements Future, Serializable {

  private static final AtomicInteger counter = new AtomicInteger(0);
  final int myID;
  private final transient CountDownLatch latch = new CountDownLatch(1);
  private transient R value;

  public AsyncFuture() {
    this.myID = AsyncFuture.counter.getAndIncrement();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return this.latch.getCount() == 0;
  }

  @Override
  public R get() throws InterruptedException {
    System.err.println("AsyncFuture: starts myStartWorker(), experimental !!!!!");
    GlobalRuntimeImpl.getRuntime().getPool().myStartWorker();
    this.latch.await();
    return this.value;
  }

  @Override
  public R get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (this.latch.await(timeout, unit)) {
      return this.value;
    } else {
      throw new TimeoutException();
    }
  }

  void put(R result) {
    if (this.value != null) {
      return;
    }
    this.value = result;
    GlobalRuntimeImpl.getRuntime().getAsyncFutures().remove(this.myID);
    this.latch.countDown();
  }
}
