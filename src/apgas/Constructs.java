/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2016.
 */

package apgas;

import apgas.impl.AsyncFuture;
import apgas.impl.DoubleBinaryOperatorSerializable;
import apgas.impl.LongBinaryOperatorSerializable;
import apgas.impl.ResultAsyncAny;
import apgas.impl.SerializableRunnable;
import apgas.impl.Task;
import apgas.impl.Worker;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

/** The {@link Constructs} class defines the APGAS constructs by means of static methods. */
public final class Constructs {

  public static final LongBinaryOperatorSerializable PLUSLONG = (x, y) -> x + y;
  public static final LongBinaryOperatorSerializable MINUSLONG = (x, y) -> x - y;
  public static final LongBinaryOperatorSerializable MULTIPLYLONG = (x, y) -> x * y;
  public static final LongBinaryOperatorSerializable DIVIDELONG = (x, y) -> x / y;

  public static final DoubleBinaryOperatorSerializable PLUSDOUBLE = (x, y) -> x + y;
  public static final DoubleBinaryOperatorSerializable MINUSDOUBLE = (x, y) -> x - y;
  public static final DoubleBinaryOperatorSerializable MULTIPLYDOUBLE = (x, y) -> x * y;
  public static final DoubleBinaryOperatorSerializable DIVIDEDOUBLE = (x, y) -> x / y;

  /** Prevents instantiation. */
  private Constructs() {}

  /**
   * Runs {@code f} then waits for all tasks transitively spawned by {@code f} to complete.
   *
   * <p>If {@code f} or the tasks transitively spawned by {@code f} have uncaught exceptions then
   * {@code finish(f)} then throws a {@link MultipleException} that collects these uncaught
   * exceptions.
   *
   * @param f the function to run
   * @throws MultipleException if there are uncaught exceptions
   */
  public static void finish(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().finish(f);
  }

  /**
   * Evaluates {@code f}, waits for all the tasks transitively spawned by {@code f}, and returns the
   * result.
   *
   * <p>If {@code f} or the tasks transitively spawned by {@code f} have uncaught exceptions then
   * {@code finish(F)} then throws a {@link MultipleException} that collects these uncaught
   * exceptions.
   *
   * @param <T> the type of the result
   * @param f the function to run
   * @return the result of the evaluation
   */
  public static <T> T finish(Callable<T> f) {
    return GlobalRuntime.getRuntimeImpl().finish(f);
  }

  /**
   * Submits a new local task to the global runtime with body {@code f} and returns immediately.
   *
   * @param f the function to run
   */
  public static void async(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().async(f);
  }

  /**
   * Submits a new task to the global runtime to be run at {@link Place} {@code p} with body {@code
   * f} and returns immediately.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void asyncAt(Place p, SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().asyncAt(p, f);
  }

  /**
   * Submits an uncounted task to the global runtime to be run at {@link Place} {@code p} with body
   * {@code f} and returns immediately. The termination of this task is not tracked by the enclosing
   * finish. Exceptions thrown by the task are ignored.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void uncountedAsyncAt(Place p, SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().uncountedAsyncAt(p, f);
  }

  /**
   * Evaluates {@code f} at {@link Place} {@code p}, waits for all the tasks transitively spawned by
   * {@code f}, and returns the result.
   *
   * @param <T> the type of the result (must implement java.io.Serializable)
   * @param p the place of execution
   * @param f the function to run
   * @return the result of the evaluation
   */
  public static <T extends Serializable> T at(Place p, SerializableCallable<T> f) {
    return GlobalRuntime.getRuntimeImpl().at(p, f);
  }

  /**
   * Runs {@code f} at {@link Place} {@code p} and waits for all the tasks transitively spawned by
   * {@code f}.
   *
   * <p>Equivalent to {@code finish(() -> asyncAt(p, f))}
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void at(Place p, SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().at(p, f);
  }

  /**
   * Returns the current {@link Place}.
   *
   * @return the current place
   */
  public static Place here() {
    return GlobalRuntime.getRuntimeImpl().here();
  }

  /**
   * Returns the place with the given ID.
   *
   * @param id the requested ID
   * @return the place with the given ID
   */
  public static Place place(int id) {
    return GlobalRuntime.getRuntimeImpl().place(id);
  }

  /**
   * Returns the current list of places in the global runtime.
   *
   * @return the current list of places in the global runtime
   */
  public static List<? extends Place> places() {
    return GlobalRuntime.getRuntimeImpl().places();
  }

  /**
   * Runs {@code f} at {@link Place} {@code p} immediately
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public static void immediateAsyncAt(Place p, SerializableRunnable f) {
    GlobalRuntime.getRuntimeImpl().immediateAsyncAt(p, f);
  }

  /**
   * Runs {@code f} as a local flexible task, which will be executed by any work-free place.
   *
   * @param f the function to run
   */
  public static void asyncAny(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().asyncAny(f);
  }

  /**
   * Reduces all asyncAny results and returns the result.
   *
   * @return the final reduced result
   */
  public static synchronized ResultAsyncAny reduceAsyncAny() {
    return GlobalRuntime.getRuntimeImpl().reduceAsyncAny();
  }

  /**
   * Reduces all long results by using @param op and returns a singe long value.
   *
   * @param op used for reducing the results
   * @return the final reduced result
   */
  public static synchronized Long reduceAsyncAnyLong(LongBinaryOperatorSerializable op) {
    return GlobalRuntime.getRuntimeImpl().reduceAsyncAnyLong(op);
  }

  /**
   * Reduces all long results by using @param op and returns a singe long value.
   *
   * @param op used for reducing the results
   * @return the final reduced result
   */
  public static synchronized Double reduceAsyncAnyDouble(DoubleBinaryOperatorSerializable op) {
    return GlobalRuntime.getRuntimeImpl().reduceAsyncAnyDouble(op);
  }

  /**
   * Reduces the place internal result over all worker results.
   *
   * @return the place internal result
   */
  public static ResultAsyncAny reduceAsyncAnyLocal() {
    return GlobalRuntime.getRuntimeImpl().reduceAsyncAnyLocal();
  }

  /**
   * Reduces the place internal long result over all worker results.
   *
   * @param op used for reducing the results
   * @return the place internal long result
   */
  public static Long reduceAsyncAnyLocalLong(LongBinaryOperatorSerializable op) {
    return GlobalRuntime.getRuntimeImpl().reduceAsyncAnyLocalLong(op);
  }

  /**
   * Reduces the place internal double result over all worker results.
   *
   * @param op used for reducing the results
   * @return the place internal long result
   */
  public static Double reduceAsyncAnyLocalDouble(DoubleBinaryOperatorSerializable op) {
    return GlobalRuntime.getRuntimeImpl().reduceAsyncAnyLocalDouble(op);
  }

  /**
   * Merges {@code result} to the matching thread partial result
   *
   * @param result the partial result to merge
   */
  public static void mergeAsyncAny(ResultAsyncAny result) {
    GlobalRuntime.getRuntimeImpl().mergeAsyncAny(result);
  }

  /**
   * Merges {@code result} to the matching thread partial result
   *
   * @param value the partial result to merge
   * @param op the merge operator
   */
  public static void mergeAsyncAny(long value, LongBinaryOperatorSerializable op) {
    GlobalRuntime.getRuntimeImpl().mergeAsyncAny(value, op);
  }

  /**
   * Merges {@code result} to the matching thread partial result
   *
   * @param value the partial result to merge
   * @param op the merge operator
   */
  public static void mergeAsyncAny(double value, DoubleBinaryOperatorSerializable op) {
    GlobalRuntime.getRuntimeImpl().mergeAsyncAny(value, op);
  }

  /**
   * Sends a list of tasks to a remote place
   *
   * @param thief place that gets the list of tasks
   * @param list the list of tasks
   * @param victim the sending place
   * @param lifeline indicates whether it is a lifeline send
   */
  public static void asyncAtTaskList(Place thief, List<Task> list, int victim, boolean lifeline) {
    GlobalRuntime.getRuntimeImpl().asyncAtTaskList(thief, list, victim, lifeline);
  }

  /**
   * Starts the management workers to enable local flexible tasks and their automatically
   * distribution over all threads and places. Evaluates {@code f}, waits for all the tasks
   * transitively spawned by {@code f}.
   *
   * <p>If {@code f} or the tasks transitively spawned by {@code f} have uncaught exceptions then
   * {@code finishAsyncAny(F)} then throws a {@link MultipleException} that collects these uncaught
   * exceptions.
   *
   * @param f the function to run
   */
  public static void finishAsyncAny(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().finishAsyncAny(f);
  }

  /**
   * Starts the management workers to enable local flexible tasks and their automatically
   * distribution over all threads and places. A partial result is cyclically {@code cyclical}
   * reduced and out printed. Evaluates {@code f}, waits for all the tasks transitively spawned by
   * {@code f}.
   *
   * <p>If {@code f} or the tasks transitively spawned by {@code f} have uncaught exceptions then
   * {@code finishAsyncAny(F)} then throws a {@link MultipleException} that collects these uncaught
   * exceptions.
   *
   * @param f the function to run
   * @param cyclical how often is partial result reduced, -1 for never
   */
  public static void finishAsyncAny(SerializableJob f, long cyclical) {
    GlobalRuntime.getRuntimeImpl().finishAsyncAny(f, cyclical);
  }

  /**
   * Executes {@code f} on each place to initialize data.
   *
   * @param f the function to run
   */
  public static void staticInit(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().staticInit(f);
  }

  /**
   * Enables static initial task distribution.
   *
   * <p>Must be called before an {@link #finishAsyncAny(SerializableJob)} or {@link
   * #finishAsyncAny(SerializableJob, long)}.
   */
  public static void enableStaticDistribution() {
    GlobalRuntime.getRuntimeImpl().enableStaticDistribution();
  }

  /**
   * Encapsulates {@code f} as a new APGAS task and returns it.
   *
   * @param f the function for the new Task
   * @return new created APGAS task
   */
  public static Task createAsyncAnyTask(SerializableJob f) {
    return GlobalRuntime.getRuntimeImpl().createAsyncAnyTask(f);
  }

  /**
   * Returns the matching thread local result.
   *
   * @param <T> Result Type
   * @return thread local result
   */
  public static <T extends ResultAsyncAny> T getThreadLocalResult() {
    return GlobalRuntime.getRuntimeImpl().getThreadLocalResult();
  }

  /**
   * Sets the matching thread local result
   *
   * @param result the new result
   */
  public static void setThreadLocalResult(ResultAsyncAny result) {
    GlobalRuntime.getRuntimeImpl().setThreadLocalResult(result);
  }

  /**
   * Starts {@code count} tasks with {@code f} and distributes them evenly over all places.
   *
   * @param f the function to run
   * @param count count of tasks to be create
   */
  public static void staticAsyncAny(SerializableJob f, int count) {
    GlobalRuntime.getRuntimeImpl().staticAsyncAny(f, count);
  }

  /**
   * Starts on {@code remote} place a list of tasks {@code jobList}.
   *
   * @param remote remote place
   * @param jobList list of jobs for tasks
   */
  public static void staticAsyncAny(Place remote, List<SerializableJob> jobList) {
    GlobalRuntime.getRuntimeImpl().staticAsyncAny(remote, jobList);
  }

  /**
   * Submits a new task to the global runtime to be run at {@link Place} {@code p} with body {@code
   * f} and returns {@link AsyncFuture} immediately.
   *
   * @param p the place of execution
   * @param f the {@link SerializableCallable} to run
   */
  public static <T extends Serializable> AsyncFuture<T> asyncAtWithFuture(
      Place p, SerializableCallable<T> f) {
    return GlobalRuntime.getRuntimeImpl().asyncAtWithFuture(p, f);
  }

  /**
   * Submits a new local task to the global runtime with body {@code f} and returns a {@link
   * AsyncFuture} immediately.
   *
   * @param f the {@link SerializableCallable} to run
   */
  public static <T extends Serializable> AsyncFuture<T> asyncWithFuture(SerializableCallable<T> f) {
    return GlobalRuntime.getRuntimeImpl().asyncWithFuture(f);
  }

  /**
   * Runs {@code f} as a local flexible task, which will be executed by any work-free place. Returns
   * a {@link AsyncFuture} immediately.
   *
   * @param f the function to run
   */
  public static <T extends Serializable> AsyncFuture<T> asyncAnyWithFuture(
      SerializableCallable<T> f) {
    return GlobalRuntime.getRuntimeImpl().asyncAnyWithFuture(f);
  }

  /**
   * Submits a new local task to the global runtime with body {@code f}. Returns the generated
   * {@link Task} immediately.
   *
   * @param f the function to run
   */
  public static void cancelableAsyncAny(SerializableJob f) {
    GlobalRuntime.getRuntimeImpl().cancelableAsyncAny(f);
  }

  /** Cancels all cancelable system-wide tasks and blocks the creation of new tasks */
  public static void cancelAllCancelableAsyncAny() {
    GlobalRuntime.getRuntimeImpl().cancelAllCancelableAsyncAny();
  }

  /**
   * Returns the liveness of a place
   *
   * @return isDead
   */
  public static boolean isDead(Place place) {
    return GlobalRuntime.getRuntimeImpl().isDead(place);
  }

  /**
   * Returns the next place
   *
   * @return the next place
   */
  public static Place nextPlace(Place place) {
    return GlobalRuntime.getRuntimeImpl().nextPlace(place);
  }

  /**
   * Returns the previous place
   *
   * @return the previuos place
   */
  public static Place prevPlace(Place place) {
    return GlobalRuntime.getRuntimeImpl().prevPlace(place);
  }

  /**
   * Returns the current worker
   *
   * @return the worker place
   */
  public static Worker getCurrentWorker() {
    return GlobalRuntime.getRuntimeImpl().getCurrentWorker();
  }

  /**
   * Returns the number of local workers (same on every place)
   *
   * @return the number of local workers
   */
  public static int numLocalWorkers() {
    return GlobalRuntime.getRuntimeImpl().numLocalWorkers();
  }
}
