package apgas.util;

import static apgas.Constructs.here;
import static apgas.Constructs.place;

import apgas.impl.Config;
import java.io.Serializable;

public class ConsolePrinter implements Serializable {

  private static final boolean PRINT =
      Boolean.parseBoolean(System.getProperty(Config.APGAS_CONSOLEPRINTER, "false"));
  private static final ConsolePrinter instance = new ConsolePrinter();

  public static synchronized ConsolePrinter getInstance() {
    return ConsolePrinter.instance;
  }

  public synchronized void println(String output) {
    if (true == PRINT) {
      System.out.println(output);
    }
  }

  public synchronized void printlnErr(String output) {
    if (true == PRINT) {
      System.err.println(output);
    }
  }

  public synchronized void print(String output) {
    if (true == PRINT) {
      System.out.print(output);
    }
  }

  public synchronized void printErr(String output) {
    if (true == PRINT) {
      System.err.print(output);
    }
  }

  public synchronized void remotePrintln(int source, String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      System.out.println(
          place(source) + " (in " + callerName + " at " + here().id + "): " + output);
    }
  }

  public synchronized void remotePrintlnErr(int source, String output) {
    if (PRINT == true) {
      String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
      System.err.println(
          place(source) + " (in " + callerName + " at " + here().id + "): " + output);
    }
  }

  public synchronized boolean getStatus() {
    return PRINT;
  }
}
