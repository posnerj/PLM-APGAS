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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

/** The {@link Worker} class implements a worker thread. */
public final class Worker extends ForkJoinWorkerThread {

  private final int myID;
  /** The current task. */
  Task task;

  /**
   * Instantiates a Worker operating in the given pool.
   *
   * @param pool the pool this worker works in
   */
  protected Worker(ForkJoinPool pool, int myID) {
    super(pool);
    this.myID = myID;
  }

  public int getMyID() {
    return myID;
  }
}
