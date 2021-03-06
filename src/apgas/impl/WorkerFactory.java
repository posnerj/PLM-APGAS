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

/** The {@link WorkerFactory} class implements a thread factory for the thread pool. */
final class WorkerFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

  private int counter = 0;

  @Override
  public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
    return new Worker(pool, counter++);
  }
}
