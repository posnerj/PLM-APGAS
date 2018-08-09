package apgas.impl;

import static apgas.Constructs.asyncAtTaskList;
import static apgas.Constructs.at;
import static apgas.Constructs.here;
import static apgas.Constructs.immediateAsyncAt;
import static apgas.Constructs.place;
import static apgas.Constructs.places;

import apgas.Place;
import apgas.util.ConsolePrinter;
import apgas.util.MyForkJoinPool;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagementWorker {

  /** lifeline are closed at program start if {@code true} */
  static boolean staticDistribution = false;

  /**
   * {@code true} whether this management worker is waiting for an answer, otherwise {@code false}
   */
  final AtomicBoolean waiting = new AtomicBoolean(true);
  /** {@code true} whether work was received otherwise {@code false} */
  final AtomicBoolean receivedWork = new AtomicBoolean(false);
  /**
   * The data structure to keep a key invariant: At any time, at most one message has been sent on
   * an outgoing lifeline (and hence at most one message has been received on an incoming lifeline).
   */
  final boolean[] lifelinesActivated;
  /** Helper for partial live reducing */
  private final AtomicInteger reduceCount = new AtomicInteger();
  /** Number of places */
  private final int P;
  /** Link to the APGAS runtime */
  private final GlobalRuntimeImpl runtime;
  /** number of threads */
  private final int threads;
  /** Read as I am the "lifeline buddy" of my "lifelineThieves" */
  private final ConcurrentLinkedQueue<Integer> lifelineThieves;
  /** Lifeline buddies */
  private final int[] lifelines;
  /** Number of random victims to probe before sending requests to lifeline buddy */
  private final int w;
  /** Maximum number of random victims */
  private final int m;
  /**
   * Random number, used when picking a non-lifeline victim/buddy. Important to seed with place id,
   * otherwise BG/Q, the random sequence will be exactly same at different places
   */
  private final Random random;
  /**
   * Random buddies, a runner first probeWs its random buddy, only when none of the random buddies
   * responds it starts to probe its lifeline buddies
   */
  private final int[] victims;
  /** Logger to record the work-stealing status */
  LoggerAsyncAny logger;
  /** while {@code true} the management worker runs */
  private volatile boolean run;
  /** Thread for Management Worker */
  private final Thread t = new Thread(this::run);
  /** Place internal result */
  private ResultAsyncAny partialResult;
  /** Thread for local live reducing */
  private Thread reduceT;

  /**
   * Constructor
   *
   * @param w Number of random victim
   * @param l Dimension for lifeline graph
   * @param threads number of threads
   */
  public ManagementWorker(int w, int l, int threads, boolean logger) {
    this.w = w;
    this.m = 1024;
    this.P = places().size();
    this.threads = threads;
    this.run = false;
    this.runtime = GlobalRuntimeImpl.getRuntime();
    this.random = new Random(here().id);

    int z0 = 1;
    int zz = l;
    while (zz < P) {
      z0++;
      zz *= l;
    }
    int z = z0;

    this.lifelines = new int[z];
    Arrays.fill(this.lifelines, -1);

    int h = here().id;

    victims = new int[m];
    if (P > 1) {
      for (int i = 0; i < m; i++) {
        while ((victims[i] = random.nextInt(P)) == h) {}
      }
    }

    // lifelines
    int x = 1;
    int y = 0;
    for (int j = 0; j < z; j++) {
      int v = h;
      for (int k = 1; k < l; k++) {
        v = v - v % (x * l) + (v + x * l - x) % (x * l);
        if (v < P) {
          lifelines[y++] = v;
          break;
        }
      }
      x *= l;
    }

    lifelineThieves = new ConcurrentLinkedQueue<>();

    lifelinesActivated = new boolean[P];
    Arrays.fill(lifelinesActivated, false);

    if (false == staticDistribution) {
      int[] calculateLifelineThieves = calculateLifelineThieves(l, z, h);
      for (int i : calculateLifelineThieves) {
        if (i != -1) {
          lifelineThieves.add(i);
        }
      }
      for (int i : lifelines) {
        if (i != -1) {
          lifelinesActivated[i] = true;
        }
      }
    }
    // implicit
    //    else {
    //      for (int i : lifelines) {
    //        if (i != -1) {
    //          lifelinesActivated[i] = false;
    //        }
    //      }
    //    }
    this.logger = new LoggerAsyncAny(logger);
  }

  /**
   * Returns the lifeline graph
   *
   * @param nodecountPerEdge (l) Dimension
   * @param z Dimension
   * @param id place id
   * @return the lifeline graph
   */
  private int[] calculateLifelineThieves(int nodecountPerEdge, int z, int id) {
    int[] predecessors = new int[z];
    int mathPower_nodecoutPerEdge_I = 1;
    for (int i = 0; i < z; i++) {
      int vectorLength = (id / mathPower_nodecoutPerEdge_I) % nodecountPerEdge;

      if (vectorLength + 1 == nodecountPerEdge
          || (predecessors[i] = id + mathPower_nodecoutPerEdge_I) >= P) {
        predecessors[i] = id - (vectorLength * mathPower_nodecoutPerEdge_I);

        if (predecessors[i] == id) {
          predecessors[i] = -1;
        }
      }
      mathPower_nodecoutPerEdge_I *= nodecountPerEdge;
    }
    return predecessors;
  }

  /** Starts the management worker */
  private void start() {
    this.logger.startLive();
    this.run = true;
    this.t.start();
  }

  /**
   * Starts the management worker
   *
   * @param sleep time between live reduces
   */
  public void start(long sleep) {
    ConsolePrinter.getInstance().println(here() + " manWorker is starting...");
    this.run = true;
    this.start();
    if (here().id == 0 && sleep >= 0) {
      this.reduceT = new Thread(() -> this.showPartialResult(sleep));
      this.reduceT.start();
    }
  }

  /** Stops the management worker */
  public void stop() {
    ConsolePrinter.getInstance().println(here() + " manWorker is stopping...");
    //    this.logger.stopLive();
    MyForkJoinPool pool = GlobalRuntimeImpl.getRuntime().getPool();
    this.logger.nodesCount =
        pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount() + pool.getRunningThreadCount();
    this.run = false;
    synchronized (this.waiting) {
      this.waiting.set(false);
      this.waiting.notifyAll();
    }
    this.t.interrupt();
    if (here().id == 0 && this.reduceT != null) {
      this.reduceT.interrupt();
    }
    ConsolePrinter.getInstance().println(here() + " manWorker has stopped!");
  }

  /** Main loop for management worker */
  private void run() {
    while (true == this.run) {
      try {
        ConsolePrinter.getInstance()
            .println(
                here()
                    + " "
                    + GlobalRuntimeImpl.getRuntime().getPool()
                    + " "
                    + Arrays.toString(FinishAsyncAny.SINGLETON.getCounts()));

        Thread.sleep(100);

        if (true == areAllLifelineActivated()) { // && poolIsEmpty() == true) {
          FinishAsyncAny.SINGLETON.updateRemoteCounts();

          ConsolePrinter.getInstance().println(here() + " all lifelines are active, sleep");
          synchronized (this.waiting) {
            while (true == waiting.get()) {
              try {
                waiting.wait();
              } catch (InterruptedException e) {
                //                                e.printStackTrace();
              }
            }
          }
        }

        sendTasksToLifeLineSteal();

        if (false == randomSteal()) {
          lifelineSteal();
        }

      } catch (InterruptedException e) {
        //                e.printStackTrace();
      }
    }
  }

  /**
   * Returns {@code true} whether the pool is empty, otherwise {@code false}
   *
   * @return {@code true} whether the pool is empty, otherwise {@code false}
   */
  private boolean isPoolEmpty() {
    MyForkJoinPool pool = GlobalRuntimeImpl.getRuntime().getPool();

    if (pool.getActiveThreadCount() > 0) {
      return false;
    }

    return false != pool.isQuiescent();
  }

  /**
   * Returns {@code true} whether all lifeline are activated, otherwise {@code false}
   *
   * @return {@code true} whether all lifeline are activated, otherwise {@code false}
   */
  protected boolean areAllLifelineActivated() {
    for (int i : this.lifelines) {
      if (i < 0) {
        continue;
      }
      if (false == this.lifelinesActivated[i]) {
        return false;
      }
    }
    return true;
  }

  /** Answers lifeline requests */
  private void sendTasksToLifeLineSteal() {
    if (true == lifelineThieves.isEmpty()) {
      return;
    }

    Integer lifelineThief;
    while ((lifelineThief = lifelineThieves.poll()) != null) {
      ConsolePrinter.getInstance().println(here() + " sendTasksToLifeLineSteal: " + lifelineThief);
      ArrayList<Task> list = new ArrayList<>();
      try {
        int num = GlobalRuntimeImpl.getRuntime().getPool().drainHalfTasksTo(list, lifelineThief);

        // reject
        if (num == 0) {
          ConsolePrinter.getInstance()
              .println(here() + " sendTasksToLifeLineSteal REJECT: " + lifelineThief);
          this.lifelineThieves.add(lifelineThief);
          return;
        }

        FinishAsyncAny.SINGLETON.incrementCounts(lifelineThief);
        logger.nodesGiven += num;
        logger.lifelineStealsSuffered++;
        asyncAtTaskList(place(lifelineThief), list, here().id, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /** Sends steal requests to lifeline buddies */
  private void lifelineSteal() {
    if (this.P == 1) {
      return;
    }

    final int lifelineThief = here().id;

    int i = 0;
    while (((this.runtime.pool.getQueuedSubmissionCount() + this.runtime.pool.getQueuedTaskCount())
            < this.threads)
        && i < this.lifelines.length
        && 0 <= this.lifelines[i]) {
      int lifeline = this.lifelines[i];
      if (false == this.lifelinesActivated[lifeline]) {
        this.logger.stopLive();
        MyForkJoinPool pool = GlobalRuntimeImpl.getRuntime().getPool();
        this.logger.nodesCount =
            pool.getQueuedSubmissionCount()
                + pool.getQueuedTaskCount()
                + pool.getRunningThreadCount();

        logger.lifelineStealsAttempted++;
        this.lifelinesActivated[lifeline] = true;
        Place lifelineVictim = place(lifeline);
        this.waiting.set(true);
        ConsolePrinter.getInstance()
            .println(here() + " tries to lifelineSteal from " + lifelineVictim);
        immediateAsyncAt(
            lifelineVictim,
            () -> {
              GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().lifelineStealsAttempted++;
              ConsolePrinter.getInstance()
                  .println(here() + " received a lifelineSteal request from " + lifelineThief);
              ArrayList<Task> list = new ArrayList<>();
              try {
                int num =
                    GlobalRuntimeImpl.getRuntime().getPool().drainHalfTasksTo(list, lifelineThief);

                // reject
                if (num == 0) {
                  GlobalRuntimeImpl.getRuntime().getManWorker().lifelineThieves.add(lifelineThief);
                  immediateAsyncAt(
                      place(lifelineThief),
                      () -> {
                        synchronized (GlobalRuntimeImpl.getRuntime().getManWorker().waiting) {
                          GlobalRuntimeImpl.getRuntime().getManWorker().waiting.set(false);
                          GlobalRuntimeImpl.getRuntime().getManWorker().waiting.notifyAll();
                        }
                      });
                  return;
                }

                FinishAsyncAny.SINGLETON.incrementCounts(lifelineThief);
                GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().nodesGiven += num;
                GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().lifelineStealsSuffered++;
                asyncAtTaskList(place(lifelineThief), list, here().id, true);
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

        synchronized (this.waiting) {
          while (waiting.get()) {
            try {
              waiting.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
      i++;
      logger.startLive();
    }
  }

  /**
   * Sends steal requests to random victims
   *
   * @return {@code true} whether stealing was successful
   */
  private boolean randomSteal() {
    if (this.P == 1) {
      return false;
    }

    int i = 0;
    MyForkJoinPool pool = this.runtime.pool;
    while ((pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount()) < this.threads) {

      if (++i > w) {
        break;
      }

      int victim = victims[random.nextInt(this.m)];
      final int thief = here().id;

      if (here().id == victim) {
        break;
      }

      ConsolePrinter.getInstance().println(here() + " tries to randomSteal from " + victim);
      logger.stealsAttempted++;
      logger.stopLive();
      this.logger.nodesCount =
          pool.getQueuedSubmissionCount()
              + pool.getQueuedTaskCount()
              + pool.getRunningThreadCount();

      this.waiting.set(true);
      immediateAsyncAt(
          place(victim),
          () -> {
            GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().stealsAttempted++;
            ConsolePrinter.getInstance()
                .println(here() + " received a randomSteal request from " + thief);
            ArrayList<Task> list = new ArrayList<>();
            try {
              int num = GlobalRuntimeImpl.getRuntime().getPool().drainHalfTasksTo(list, thief);

              // reject
              if (num == 0) {
                ConsolePrinter.getInstance()
                    .println(here() + " reject a randomSteal request from " + thief);

                immediateAsyncAt(
                    place(thief),
                    () -> {
                      synchronized (GlobalRuntimeImpl.getRuntime().getManWorker().waiting) {
                        GlobalRuntimeImpl.getRuntime().getManWorker().waiting.set(false);
                        GlobalRuntimeImpl.getRuntime().getManWorker().waiting.notifyAll();
                      }
                    });
                return;
              }

              FinishAsyncAny.SINGLETON.incrementCounts(thief);
              GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().nodesGiven += num;
              GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().stealsSuffered++;
              asyncAtTaskList(place(thief), list, here().id, false);
            } catch (Exception e) {
              e.printStackTrace();
            }
          });

      synchronized (this.waiting) {
        while (waiting.get()) {
          try {
            waiting.wait();
          } catch (InterruptedException e) {
            //            e.printStackTrace();
          }
        }
      }
      logger.startLive();
    }

    return this.receivedWork.getAndSet(false);
  }

  /**
   * Only executed by place 0. Merges a result to its own result. Used for the live result reducing
   *
   * @param r result
   */
  private void mergePartialResult(ResultAsyncAny r) {
    synchronized (this.reduceCount) {
      this.partialResult.mergeResult(r);
      this.reduceCount.incrementAndGet();
      this.reduceCount.notifyAll();
    }
  }

  /**
   * Only executed by place 0. Reduces live the partial result and prints it out.
   *
   * @param sleep time between the reduces
   */
  private void showPartialResult(long sleep) {
    if (sleep == 0) {
      sleep = 3000;
    } else if (sleep < 0) {
      return;
    }

    while (true == this.run) {
      try {

        Thread.sleep(sleep);

        this.partialResult = GlobalRuntimeImpl.getRuntime().reduceAsyncAnyLocal();

        if (this.partialResult == null) {
          continue;
        }

        this.reduceCount.set(1);

        for (Place p : places()) {
          if (p.id != 0) {
            immediateAsyncAt(
                p,
                () -> {
                  final ResultAsyncAny rr = GlobalRuntimeImpl.getRuntime().reduceAsyncAnyLocal();
                  if (rr == null) {
                    immediateAsyncAt(
                        place(0),
                        () -> {
                          synchronized (
                              GlobalRuntimeImpl.getRuntime().getManWorker().partialResult) {
                            GlobalRuntimeImpl.getRuntime()
                                .getManWorker()
                                .reduceCount
                                .incrementAndGet();
                            GlobalRuntimeImpl.getRuntime().getManWorker().reduceCount.notifyAll();
                          }
                        });
                  } else {
                    immediateAsyncAt(
                        place(0),
                        () -> {
                          GlobalRuntimeImpl.getRuntime().getManWorker().mergePartialResult(rr);
                        });
                  }
                });
          }
        }

        synchronized (this.reduceCount) {
          while (this.reduceCount.get() < this.P) {
            try {
              this.reduceCount.wait(sleep);
            } catch (InterruptedException e) {
              // e.printStackTrace();
            }
          }
        }

        ConsolePrinter.getInstance().println(here() + " partial Result: " + this.partialResult);

      } catch (InterruptedException e) {
        // e.printStackTrace();
      }
    }
  }

  public boolean isActive() {
    return run;
  }

  public LoggerAsyncAny getLogger() {
    return logger;
  }

  protected void collectLifelineStatus() {
    if (false == logger.isActive()) {
      return;
    }

    LoggerAsyncAny[] logs = new LoggerAsyncAny[places().size()];
    for (int i = 0; i < places().size(); i++) {
      logs[i] =
          at(
              place(i),
              () -> {
                return GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().get(true);
              });
    }

    LoggerAsyncAny log = new LoggerAsyncAny(true);
    log.collect(logs);
    log.stats();
  }
}
