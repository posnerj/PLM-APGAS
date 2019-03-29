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

import apgas.impl.GlobalRuntimeImpl;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The {@link GlobalRuntime} class provides mechanisms to initialize and shut down the APGAS global
 * runtime for the application.
 *
 * <p>The global runtime is implicitly initialized when first used. The current runtime can be
 * obtained from the {@link #getRuntime()} method, which forces initialization.
 */
public abstract class GlobalRuntime {

  /** The command line arguments if the main method of this class is invoked. */
  private static String[] args;

  /** Indicates if all system wide instances are ready, only used on place 0 */
  public static AtomicInteger readyCounter;

  /** Constructs a new {@link GlobalRuntime} instance. */
  protected GlobalRuntime() {}

  /**
   * Returns the {@link GlobalRuntime} instance for this place.
   *
   * @return the GlobalRuntime instance
   */
  public static GlobalRuntime getRuntime() {
    return getRuntimeImpl();
  }

  /**
   * Returns the {@link GlobalRuntimeImpl} instance for this place.
   *
   * @return the GlobalRuntimeImpl instance
   */
  static GlobalRuntimeImpl getRuntimeImpl() {
    GlobalRuntimeImpl runtime = GlobalRuntimeWrapper.runtime;
    while (runtime.ready != true) { // Wait for constructor
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
    return runtime;
  }

  /**
   * Initializes the global runtime.
   *
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    GlobalRuntime.args = args;
    getRuntime();
  }

  /** Shuts down the {@link GlobalRuntime} instance. */
  public abstract void shutdown();

  /**
   * Registers a place failure handler.
   *
   * <p>The handler is invoked for each failed place.
   *
   * @param handler the handler to register or null to unregister the current handler
   */
  public abstract void setPlaceFailureHandler(Consumer<Place> handler);

  /**
   * Registers a runtime failure handler.
   *
   * <p>The handler is invoked on runtime shutdown. Irregular runtime shutdown due to losing
   * Hazelcast partitions may also invoke this handler.
   *
   * @param handler the handler to register or null to unregister the current handler
   */
  public abstract void setRuntimeShutdownHandler(Runnable handler);

  /**
   * Returns the executor service for the place.
   *
   * @return the executor service
   */
  public abstract ExecutorService getExecutorService();

  /**
   * Returns the time of the last place failure as reported by {@link System#nanoTime} if any or
   * null otherwise.
   *
   * @return the time of the last place failure or null
   */
  public abstract Long lastfailureTime();

  /** A wrapper class for implementing the singleton pattern. */
  private static class GlobalRuntimeWrapper {

    /** The {@link GlobalRuntime} instance for this place. */
    private static final GlobalRuntimeImpl runtime = new GlobalRuntimeImpl(args);
  }
}
