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

/**
 * The {@link Config} class defines the names of the system properties used to configure the global
 * runtime.
 */
public final class Config {

  /**
   * Name of the network transport class implementation to instantiate (String property).
   *
   * <p>Currently "{@code apgas.impl.Transport}" and " {@code apgas.sockets.SocketTransport}" are
   * supported. Defaults to " {@code apgas.impl.Transport}".
   */
  public static final String APGAS_TRANSPORT = "apgas.transport";
  /**
   * Enables or disables compression on the network links when using transport " {@code
   * apgas.sockets.SocketTransport}".
   *
   * <p>Set to "none" or "snappy", which is the default.
   */
  public static final String APGAS_TRANSPORT_COMPRESSION = "apgas.transport.compression";
  /**
   * Upper bound on the number of persistent threads in the thread pool (Integer property).
   *
   * <p>Defaults to 256.
   */
  public static final String APGAS_MAX_THREADS = "apgas.max.threads";
  /** Reduces the number of threads used by Hazelcast if set (Boolean property). */
  public static final String APGAS_COMPACT = "apgas.compact";
  /**
   * Name of the serialization framework to instantiate (String property).
   *
   * <p>Currently "{@code java}" and "{@code kryo}" are supported. Defaults to "{@code kryo}".
   */
  public static final String APGAS_SERIALIZATION = "apgas.serialization";
  /**
   * Specifies the java command to run for spawning places (String property).
   *
   * <p>Defaults to "{@code java}".
   */
  public static final String APGAS_JAVA = "apgas.java";
  /**
   * Name of the finish implementation class to instantiate (String property).
   *
   * <p>Defaults to "{@code apgas.impl.DefaultFinish}" or " {@code apgas.impl.ResilientFinish}".
   */
  public static final String APGAS_FINISH = "apgas.finish";
  /**
   * Name of the launcher implementation class to instantiate (String property).
   *
   * <p>Defaults to "{@code apgas.impl.SShLauncher}".
   */
  public static final String APGAS_LAUNCHER = "apgas.launcher";
  /**
   * Enables the {@code apgas.util.ConsolePrinter}
   *
   * <p>Defaults to false
   */
  public static final String APGAS_CONSOLEPRINTER = "apgas.consoleprinter";
  /**
   * Count of backups used by the internal distributed data structures. (Integer property)
   *
   * <p>Set to value between {@code 0} (inclusive) and {@code 6} (inclusive). Defaults to "{@code
   * 1}".
   */
  public static final String APGAS_BACKUPCOUNT = "apgas.backupcount";
  /**
   * Enables the {@code apgas.LoggerAsyncAny}
   *
   * <p>Defaults to false
   */
  public static final String APGAS_LOGGERASYNCANY = "apgas.loggerasyncany";

  /** Prevents instantiation. */
  private Config() {}
}
