package apgas.impl;

import static apgas.Constructs.here;

import apgas.util.ConsolePrinter;
import java.io.Serializable;

public class LoggerAsyncAny implements Serializable {

  private static final long serialVersionUID = 1L;

  public long nodesCount = 0;
  public long nodesGiven = 0;
  public long lifelineNodesReceived = 0;
  /* (random)stealing requests stat*/
  public long stealsAttempted = 0;
  public long stealsPerpetrated = 0;
  public long stealsReceived = 0;
  public long stealsSuffered = 0;
  public long nodesReceived = 0;
  /* (lifeline)stealing requests stat*/
  public long lifelineStealsAttempted = 0;
  public long lifelineStealsPerpetrated = 0;
  public long lifelineStealsReceived = 0;
  public long lifelineStealsSuffered = 0;
  /* timing stat */
  public long lastStartStopLiveTimeStamp = -1;
  public long timeAlive = 0;
  public long timeDead = 0;
  public long startTime = 0;
  public long timeReference;

  ConsolePrinter consolePrinter;
  private boolean active;

  public LoggerAsyncAny(boolean active) {
    this.active = active;
    this.timeReference = System.nanoTime();
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  static String sub(String str, int start, int end) {
    return str.substring(start, Math.min(end, str.length()));
  }

  public synchronized void startLive() {
    if (false == active) {
      return;
    }
    long time = System.nanoTime();
    if (startTime == 0) {
      startTime = time;
    }

    if (lastStartStopLiveTimeStamp >= 0) {
      timeDead += time - lastStartStopLiveTimeStamp;
    }
    lastStartStopLiveTimeStamp = time;
  }

  public synchronized void stopLive() {
    if (false == active) {
      return;
    }
    long time = System.nanoTime();
    timeAlive += time - lastStartStopLiveTimeStamp;
    lastStartStopLiveTimeStamp = time;
  }

  public synchronized void collect(LoggerAsyncAny logs[]) {
    if (false == active) {
      return;
    }
    for (LoggerAsyncAny l : logs) {
      add(l);
    }
  }

  public synchronized void stats() {
    if (false == active) {
      return;
    }
    System.out.println(
        nodesGiven
            + " Task items stolen = "
            + nodesReceived
            + " (direct) + "
            + lifelineNodesReceived
            + " (lifeline).");
    System.out.println(stealsPerpetrated + " successful direct steals.");
    System.out.println(lifelineStealsPerpetrated + " successful lifeline steals.");
  }

  public synchronized void add(LoggerAsyncAny other) {
    if (false == active) {
      return;
    }
    nodesCount += other.nodesCount;
    nodesGiven += other.nodesGiven;
    nodesReceived += other.nodesReceived;
    stealsPerpetrated += other.stealsPerpetrated;
    lifelineNodesReceived += other.lifelineNodesReceived;
    lifelineStealsPerpetrated += other.lifelineStealsPerpetrated;
  }

  public synchronized LoggerAsyncAny get(boolean verbose) {
    if (false == active) {
      return null;
    }
    if (verbose) {
      System.out.println(
          ""
              + here().id
              + " -> "
              + sub("" + (timeAlive / 1E9), 0, 6)
              + " : "
              + sub("" + (timeDead / 1E9), 0, 6)
              + " : "
              + sub("" + ((timeAlive + timeDead) / 1E9), 0, 6)
              + " : "
              + sub("" + (100.0 * timeAlive / (timeAlive + timeDead)), 0, 6)
              + "%"
              + " :: "
              + sub("" + ((startTime - timeReference) / 1E9), 0, 6)
              + " : "
              + sub("" + ((lastStartStopLiveTimeStamp - timeReference) / 1E9), 0, 6)
              + " :: "
              + nodesCount
              + " :: "
              + nodesGiven
              + " : "
              + nodesReceived
              + " : "
              + lifelineNodesReceived
              + " :: "
              + stealsReceived
              + " : "
              + lifelineStealsReceived
              + " :: "
              + stealsSuffered
              + " : "
              + lifelineStealsSuffered
              + " :: "
              + stealsAttempted
              + " : "
              + (stealsAttempted - stealsPerpetrated)
              + " :: "
              + lifelineStealsAttempted
              + " : "
              + (lifelineStealsAttempted - lifelineStealsPerpetrated)
              + " : "
              + timeReference);
    }
    return this;
  }

  public boolean isActive() {
    return active;
  }
}
