package apgas.impl;

import static apgas.Constructs.here;
import static apgas.Constructs.place;

import apgas.Place;
import apgas.util.ConsolePrinter;

public class TaskList implements SerializableRunnable {

  /** Array with the tasks */
  private final Task[] tasks;
  /** Victim */
  private final int victim;
  /** Lifeline */
  private final boolean lifeline;
  /** Number of current tasks */
  private int count = 0;

  public TaskList(int initialCapacity, int victim, boolean lifeline) {
    this.tasks = new Task[initialCapacity];
    this.victim = victim;
    this.lifeline = lifeline;
  }

  /**
   * Adds a task
   *
   * @param t task
   */
  public void add(Task t) {
    this.tasks[this.count++] = t;
  }

  /** Is executed on the remote place and adds the tasks to the remote task pool */
  @Override
  public void run() {
    if (lifeline) {
      GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().lifelineStealsPerpetrated++;
      GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().lifelineNodesReceived += count;
    } else {
      GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().stealsPerpetrated++;
      GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().nodesReceived += count;
    }
    if (true == GlobalRuntimeImpl.getRuntime().getManWorker().areAllLifelineActivated()) {
      GlobalRuntimeImpl.getRuntime().getManWorker().getLogger().startLive();
    }
    ConsolePrinter.getInstance()
        .println(
            here()
                + " received "
                + this.tasks.length
                + " tasks from "
                + place(this.victim)
                + " as "
                + (lifeline ? "lifeline" : "random"));

    FinishAsyncAny.SINGLETON.decrementMyCounts();

    for (int i = 0; i < count; i++) {

      if (true == this.tasks[i].isCancelable()) {
        GlobalRuntimeImpl.getRuntime()
            .localCancelableTasks
            .put(this.tasks[i].getId(), this.tasks[i]);
      }

      this.tasks[i].run();
    }

    synchronized (GlobalRuntimeImpl.getRuntime().getManWorker().waiting) {
      if (true == this.lifeline) {
        GlobalRuntimeImpl.getRuntime().getManWorker().lifelinesActivated[this.victim] = false;
      } else {
        GlobalRuntimeImpl.getRuntime().getManWorker().receivedWork.set(true);
      }

      GlobalRuntimeImpl.getRuntime().getManWorker().waiting.set(false);
      GlobalRuntimeImpl.getRuntime().getManWorker().waiting.notifyAll();
    }
  }

  public void unspawn(Place p) {
    for (int i = 0; i < this.count; i++) {
      this.tasks[i].finish.unspawn(p.id);
    }
  }
}
