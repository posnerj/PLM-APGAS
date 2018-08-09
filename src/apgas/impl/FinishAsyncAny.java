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

package apgas.impl;

import static apgas.Constructs.here;
import static apgas.Constructs.immediateAsyncAt;
import static apgas.Constructs.place;

import apgas.util.ConsolePrinter;
import apgas.util.MyForkJoinPool;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class FinishAsyncAny implements Serializable, Finish {

  /** Only one {@link FinishAsyncAny} is allowed */
  public static final FinishAsyncAny SINGLETON = new FinishAsyncAny();

  private static final long serialVersionUID = 3789869778188598267L;
  /** Array for counting transferred tasks */
  private final int[] counts;
  /** Detects whether a calling is the first start */
  public boolean firstStart = true;
  /** Uncaught exceptions collected by this finish construct. */
  private transient List<Throwable> exceptions;

  private FinishAsyncAny() {
    this.counts = new int[GlobalRuntimeImpl.getRuntime().maxPlace()];
  }

  @Override
  public void submit(int p) {}

  @Override
  public void spawn(int p) {}

  @Override
  public void unspawn(int p) {}

  @Override
  public void tell() {}

  /**
   * Increments the entry in counts of {@code p}
   *
   * @param p place id
   */
  public synchronized void incrementCounts(int p) {
    this.counts[p]++;
  }

  /** Decrements my entry in counts */
  public synchronized void decrementMyCounts() {
    this.counts[here().id]--;
  }

  /**
   * Updates the counts on place 0
   *
   * @param counts count from remote place
   */
  private synchronized void update(int[] counts) {
    ConsolePrinter.getInstance().println(here() + " update: " + Arrays.toString(this.counts));
    boolean notify = true;
    for (int i = 0; i < this.counts.length; i++) {
      this.counts[i] += counts[i];
      if (this.counts[i] != 0) {
        notify = false;
      }
    }

    if (true == notify) {
      synchronized (SINGLETON) {
        SINGLETON.notifyAll();
      }
    }
  }

  /**
   * Is called by any place to send its counts to place 0
   *
   * @return {@code true} whether it was successful
   */
  public synchronized boolean updateRemoteCounts() {
    if (here().id == 0) {
      return false;
    }

    MyForkJoinPool pool = GlobalRuntimeImpl.getRuntime().getPool();

    if (pool.getActiveThreadCount() > 0) {
      return false;
    }

    if (false == pool.isQuiescent()) {
      return false;
    }

    final int[] counts = FinishAsyncAny.SINGLETON.getCounts();
    boolean send = false;
    for (int i : counts) {
      if (i != 0) {
        send = true;
        break;
      }
    }
    if (false == send) {
      return false;
    }

    ConsolePrinter.getInstance().println(here() + " updateCounts: " + Arrays.toString(counts));
    immediateAsyncAt(
        place(0),
        () -> {
          FinishAsyncAny.SINGLETON.update(counts);
        });
    FinishAsyncAny.SINGLETON.clearCounts();
    return true;
  }

  @Override
  public void addSuppressed(Throwable exception) {
    final int here = GlobalRuntimeImpl.getRuntime().here;
    if (here == 0) {
      // root finish
      if (exceptions == null) {
        exceptions = new ArrayList<>();
      }
      exceptions.add(exception);
    } else {
      // remote finish: spawn remote task to transfer exception to root finish
      final SerializableThrowable t = new SerializableThrowable(exception);
      immediateAsyncAt(
          place(0),
          () -> {
            FinishAsyncAny.SINGLETON.addSuppressed(t.t);
          });
    }
  }

  @Override
  public boolean isReleasable() {

    MyForkJoinPool pool = GlobalRuntimeImpl.getRuntime().getPool();

    if (pool.getActiveThreadCount() > 0) {
      return false;
    }

    if (false == pool.isQuiescent()) {
      return false;
    }

    for (int i = 0; i < this.counts.length; i++) {
      if (this.counts[i] != 0) {
        return false;
      }
    }

    ConsolePrinter.getInstance()
        .println(here() + " isReleasable true " + pool + " " + Arrays.toString(this.counts));

    return true;
  }

  @Override
  public List<Throwable> exceptions() {
    return exceptions;
  }

  @Override
  public boolean block() {
    while (false == isReleasable()) {
      try {
        synchronized (SINGLETON) {
          SINGLETON.wait(10);
        }
      } catch (final InterruptedException e) {
        e.printStackTrace();
        System.out.println("InterruptedException");
      }
    }
    return isReleasable();
  }

  /** Clears the count array */
  private synchronized void clearCounts() {
    Arrays.fill(this.counts, 0);
  }

  /**
   * Returns the count array
   *
   * @return the count array
   */
  int[] getCounts() {
    return counts;
  }
}
