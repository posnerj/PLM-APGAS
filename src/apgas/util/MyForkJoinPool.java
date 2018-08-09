package apgas.util;

import static apgas.Constructs.here;

import apgas.impl.GlobalRuntimeImpl;
import apgas.impl.Task;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;

public class MyForkJoinPool extends ForkJoinPool {

  private Method pollMethod;
  private Object[] workQueues;

  public MyForkJoinPool(
      int parallelism,
      int maxParallelism,
      ForkJoinWorkerThreadFactory factory,
      Thread.UncaughtExceptionHandler handler,
      boolean asyncMode) {
    super(
        parallelism,
        factory,
        handler,
        asyncMode,
        parallelism,
        maxParallelism,
        1,
        null,
        60_000L,
        TimeUnit.MILLISECONDS);
  }

  public ForkJoinTask<?> myPollSubmission() {
    return super.pollSubmission();
  }

  public int myDrainTasksTo(Collection<? super ForkJoinTask<?>> c) {
    return super.drainTasksTo(c);
  }

  public void myStartWorker() {

    try {
      MyForkJoinPool pool = GlobalRuntimeImpl.getRuntime().getPool();

      if (pool.getActiveThreadCount() < pool.getParallelism()) {
        return;
      }

      final Field ctl = MyForkJoinPool.class.getSuperclass().getDeclaredField("ctl");
      ctl.setAccessible(true);
      Long c = ctl.getLong(pool);

      Method signalWorkMethod =
          this.getClass().getSuperclass().getDeclaredMethod("tryAddWorker", long.class);
      signalWorkMethod.setAccessible(true);
      signalWorkMethod.invoke(pool, c);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchFieldException e) {
      e.printStackTrace();
    }
  }

  public int drainHalfTasksTo(Collection<Task> c, int thief) throws Exception {
    int count = 0;
    Task t;

    long toSteal = (super.getQueuedSubmissionCount() + super.getQueuedTaskCount()) / 2;
    if (toSteal <= 0) {
      return 0;
    }

    if (this.workQueues == null) {
      Field declaredField = this.getClass().getSuperclass().getDeclaredField("workQueues");
      declaredField.setAccessible(true);
      Object o = declaredField.get(this);
      this.workQueues = (Object[]) o;
    }

    if (this.workQueues == null) {
      return 0;
    }

    for (Object obj : this.workQueues) {
      if (obj == null) {
        continue;
      }

      if (pollMethod == null) {
        pollMethod = obj.getClass().getDeclaredMethod("poll");
        pollMethod.setAccessible(true);
      }

      while ((t = (Task) pollMethod.invoke(obj)) != null) {
        if (true == t.isCancelable()) {
          GlobalRuntimeImpl.getRuntime().localCancelableTasks.remove(t.getId());
        }
        c.add(t);
        ++count;

        if (count >= toSteal) {
          ConsolePrinter.getInstance()
              .println(
                  here()
                      + " MyForkJoinPool1: toSteal: "
                      + toSteal
                      + ", count: "
                      + count
                      + ", "
                      + "thief: "
                      + thief);
          return count;
        }
      }
    }
    ConsolePrinter.getInstance()
        .println(
            here()
                + " MyForkJoinPool2: toSteal: "
                + toSteal
                + ", count: "
                + count
                + ", "
                + "thief: "
                + thief);
    return count;
  }
}
