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

import apgas.Configuration;
import apgas.Constructs;
import apgas.DeadPlaceException;
import apgas.GlobalRuntime;
import apgas.MultipleException;
import apgas.Place;
import apgas.SerializableCallable;
import apgas.SerializableJob;
import apgas.impl.Finish.Factory;
import apgas.util.ConsolePrinter;
import apgas.util.GlobalID;
import apgas.util.GlobalRef;
import apgas.util.MyForkJoinPool;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** The {@link GlobalRuntimeImpl} class implements the {@link apgas.GlobalRuntime} class. */
public final class GlobalRuntimeImpl extends GlobalRuntime {

  private static GlobalRuntimeImpl runtime;

  /** Holds cancelable Tasks in order to be able to find them again in the FJPool */
  public final Map<Long, Task> localCancelableTasks = new ConcurrentHashMap<>();
  /** Indicates if the instance is ready */
  public final boolean ready;
  /** The transport for this global runtime instance. */
  protected final Transport transport;
  /** The value of the APGAS_VERBOSE_SERIALIZATION system property. */
  final boolean verboseSerialization;
  /** This place's ID. */
  final int here;
  /** The pool for this global runtime instance. */
  final MyForkJoinPool pool;
  /** The resilient map from finish IDs to finish states. */
  final IMap<GlobalID, ResilientFinishState> resilientFinishMap;
  /** Num of local workers * */
  final int numLocalWorkers;
  /** The value of the APGAS_RESILIENT system property. */
  private final boolean resilient;
  /** The finish factory. */
  private final Factory factory;
  /** This place. */
  private final Place home;
  /** The mutable set of places in this global runtime instance. */
  private final SortedSet<Place> placeSet = new TreeSet<>();
  /** The launcher used to spawn additional places. */
  private final Launcher launcher;
  /** Helper for partial live reducing */
  private final AtomicInteger reduceCount = new AtomicInteger();
  /** Holds tasks which are started with future option */
  private final HashMap<Integer, AsyncFuture> asyncFutures = new HashMap<>();
  /** The registered runtime failure handler. */
  Runnable shutdownHandler;
  /** The time of the last place failure. */
  Long failureTime;
  /** An immutable ordered list of the current places. */
  private List<Place> places;
  /** The registered place failure handler. */
  private Consumer<Place> handler;
  /** Performs inter-place work stealing for asyncAny */
  private ManagementWorker manWorker;
  /** Holds the Thread Result when using asyncAny */
  private ResultAsyncAny[] result;
  /** When set to false, no more asyncAnyTasks can be submitted */
  private boolean allowMoreAsyncAnyTask = true;
  /** True if shutdown is in progress. */
  private boolean dying;
  /** Place internal result */
  private ResultAsyncAny partialResult;
  /** Status of loggerAsyncAny */
  private boolean loggerAsyncAny;
  /** timeout for apgas runtime starting in seconds */
  int timeoutStarting = 1200;

  /**
   * Constructs a new {@link GlobalRuntimeImpl} instance.
   *
   * @param args the command line arguments
   */
  public GlobalRuntimeImpl(String[] args) {
    try {
      final long begin = System.nanoTime();
      GlobalRuntimeImpl.runtime = this;

      // parse configuration
      final int p = Integer.getInteger(Configuration.APGAS_PLACES, 1);
      GlobalRuntime.readyCounter = new AtomicInteger(p);
      System.setProperty(Configuration.APGAS_PLACES, Integer.toString(p));
      numLocalWorkers =
          Integer.getInteger(
              Configuration.APGAS_THREADS, Runtime.getRuntime().availableProcessors());
      System.setProperty(Configuration.APGAS_THREADS, Integer.toString(numLocalWorkers));
      final String master = System.getProperty(Configuration.APGAS_MASTER);
      final String hostfile = System.getProperty(Configuration.APGAS_HOSTFILE);
      verboseSerialization = Boolean.getBoolean(Configuration.APGAS_VERBOSE_SERIALIZATION);
      final boolean verboseLauncher = Boolean.getBoolean(Configuration.APGAS_VERBOSE_LAUNCHER);
      resilient = Boolean.getBoolean(Configuration.APGAS_RESILIENT);

      final boolean compact = Boolean.getBoolean(Config.APGAS_COMPACT);
      final int maxThreads = Integer.getInteger(Config.APGAS_MAX_THREADS, 256);
      final String serialization = System.getProperty(Config.APGAS_SERIALIZATION, "java");
      final String finishName = System.getProperty(Config.APGAS_FINISH);
      final String java = System.getProperty(Config.APGAS_JAVA, "java");
      final String transportName = System.getProperty(Config.APGAS_TRANSPORT);
      final String launcherName = System.getProperty(Config.APGAS_LAUNCHER);
      final int backupCount = Integer.getInteger(Config.APGAS_BACKUPCOUNT, 1);
      loggerAsyncAny = Boolean.getBoolean(Config.APGAS_LOGGERASYNCANY);

      final String localhost = InetAddress.getLoopbackAddress().getHostAddress();

      // parse hostfile
      List<String> hosts = null;
      if (master == null && hostfile != null) {
        try {
          hosts = Files.readAllLines(FileSystems.getDefault().getPath(hostfile));
          if (hosts.isEmpty()) {
            System.err.println("[APGAS] Empty hostfile: " + hostfile + ". Using localhost.");
          }
        } catch (final IOException e) {
          System.err.println("[APGAS] Unable to read hostfile: " + hostfile + ". Using localhost.");
        }
      }

      // initialize launcher
      Launcher localLauncher = null;
      if (master == null && p > 1) {
        if (launcherName != null) {
          try {
            // Java 9
            localLauncher =
                (Launcher) Class.forName(launcherName).getDeclaredConstructor().newInstance();
          } catch (InstantiationException
              | IllegalAccessException
              | ExceptionInInitializerError
              | ClassNotFoundException
              | NoClassDefFoundError
              | ClassCastException e) {
            System.err.println(
                "[APGAS] Unable to instantiate launcher: "
                    + launcherName
                    + ". Using default launcher (ssh).");
          }
        }
        if (localLauncher == null) {
          localLauncher = new SshLauncher();
        }
      }
      this.launcher = localLauncher;

      if (master == null && args != null && args.length > 0) {
        // invoked as a launcher
        final ArrayList<String> command = new ArrayList<>();
        command.add(java);
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        List<String> inputArguments = runtimeMXBean.getInputArguments();
        for (String a : inputArguments) {
          if (a.contains("-X") == true) {
            command.add(a);
          }
        }
        command.add("-Duser.dir=" + System.getProperty("user.dir"));
        // Java 9
        //        command.add("-Xbootclasspath:" +
        // ManagementFactory.getRuntimeMXBean().getBootClassPath());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        for (final String property : System.getProperties().stringPropertyNames()) {
          if (property.startsWith("apgas.")) {
            command.add("-D" + property + "=" + System.getProperty(property));
          }
        }
        command.addAll(Arrays.asList(args));
        final String host = hosts == null || hosts.isEmpty() ? localhost : hosts.get(0);
        final Process process = localLauncher.launch(command, host, verboseLauncher);
        while (true) {
          try {
            System.exit(process.waitFor());
          } catch (final InterruptedException e) {
          }
        }
      }

      // initialize finish
      Factory localFactory = null;
      if (finishName != null) {
        final String finishFactoryName = finishName + "$Factory";
        try {
          // Java 9
          localLauncher =
              (Launcher) Class.forName(launcherName).getDeclaredConstructor().newInstance();
        } catch (InstantiationException
            | IllegalAccessException
            | ExceptionInInitializerError
            | ClassNotFoundException
            | NoClassDefFoundError
            | ClassCastException e) {
          System.err.println(
              "[APGAS] Unable to instantiate finish factory: "
                  + finishFactoryName
                  + ". Using default factory.");
        }
      }
      if (localFactory == null) {
        this.factory = resilient ? new ResilientFinishOpt.Factory() : new DefaultFinish.Factory();
      } else {
        this.factory = localFactory;
      }

      // initialize scheduler
      pool = new MyForkJoinPool(numLocalWorkers, maxThreads, new WorkerFactory(), null, false);
      // instead of this original ctl hack, we use the novel constructor call in MyForkJoinPool
      // since Java 10
      //      final Field ctl = ForkJoinPool.class.getDeclaredField("ctl");
      //      ctl.setAccessible(true);
      //      ctl.setLong(pool, ctl.getLong(pool) + (((long) maxThreads - threads) << 48));

      // serialization
      final Boolean kryo = !"java".equals(serialization);
      if (kryo && !"kryo".equals(serialization)) {
        System.err.println(
            "[APGAS] Unable to instantiate serialization framework: "
                + serialization
                + ". Using default serialization.");
      }

      // attempt to select a good ip for this host
      String ip = null;
      String host = master;
      if (host == null && hosts != null) {
        for (final String h : hosts) {
          try {
            if (!InetAddress.getByName(h).isLoopbackAddress()) {
              host = h;
              break;
            }
          } catch (final UnknownHostException e) {
          }
        }
      }
      if (host == null) {
        host = localhost;
      }

      try {
        final Enumeration<NetworkInterface> networkInterfaces =
            NetworkInterface.getNetworkInterfaces();

        while (networkInterfaces.hasMoreElements()) {
          final NetworkInterface ni = networkInterfaces.nextElement();

          // TODO: dirty hack
          if ("apgas.impl.MPILeibnizLauncher".equals(launcherName)
              && false == ni.toString().contains("ib")) {
            continue;
          } else {
            // TODO: old apgas
            try {
              if (!InetAddress.getByName(host.split(":")[0]).isReachable(ni, 0, 100)) {
                if (true == verboseLauncher) {
                  System.err.println(
                      "[APGAS] host " + host + " is not reachable with networkinterface " + ni);
                }
                continue;
              }

            } catch (Throwable t) {
              if (true == verboseLauncher) {
                System.out.println("[APGAS]: unexpected error in finding host");
              }
              t.printStackTrace();
            }
          }

          System.err.println("[APGAS] host " + host + " is reachable with network interface " + ni);
          final Enumeration<InetAddress> e = ni.getInetAddresses();
          while (e.hasMoreElements()) {
            final InetAddress inetAddress = e.nextElement();
            if (inetAddress.isLoopbackAddress() || inetAddress instanceof Inet6Address) {
              continue;
            }

            // infiniband for kassel cluster
            if ("apgas.impl.SrunKasselLauncher".equals(launcherName)
                && false == inetAddress.getHostAddress().contains(SrunKasselLauncher.IPRANGE)) {
              continue;
            }

            ip = inetAddress.getHostAddress();
          }
        }
      } catch (final IOException e) {
        e.printStackTrace();
      }

      // check first entry of hostfile
      if (hosts != null && !hosts.isEmpty()) {
        try {
          final InetAddress inet = InetAddress.getByName(hosts.get(0));
          if (!inet.isLoopbackAddress()) {
            if (NetworkInterface.getByInetAddress(inet) == null) {
              System.err.println(
                  "[APGAS] First hostfile entry does not correspond to localhost. Ignoring and using localhost instead.");
            }
          }
        } catch (final IOException e) {
          System.err.println(
              "[APGAS] Unable to resolve first hostfile entry. Ignoring and using localhost instead.");
        }
      }

      if (true == verboseLauncher) {
        System.err.println("[APGAS] found ip: " + (null == ip ? "null" : ip));
      }

      // initialize transport
      Transport localTransport = null;
      if (transportName != null) {
        try {
          localTransport =
              (Transport)
                  Class.forName(transportName)
                      .getDeclaredConstructor(
                          GlobalRuntimeImpl.class,
                          String.class,
                          String.class,
                          String.class,
                          boolean.class,
                          boolean.class,
                          int.class)
                      .newInstance(this, master, ip, launcherName, compact, kryo, backupCount);
        } catch (InstantiationException
            | IllegalAccessException
            | ExceptionInInitializerError
            | ClassNotFoundException
            | NoClassDefFoundError
            | ClassCastException e) {
          System.err.println(
              "[APGAS] Unable to instantiate transport: "
                  + transportName
                  + ". Using default transport.");
        }
      }

      String _ip = ip;
      if (localTransport == null) {

        Transport tmpTransport =
            new Transport(this, master, _ip, launcherName, compact, kryo, backupCount);

        ExecutorService executor = Executors.newFixedThreadPool(1);

        Future<Boolean> future =
            executor.submit(
                () -> {
                  return tmpTransport.startHazelcast();
                });

        TimeUnit.SECONDS.sleep(10);

        launchPlaces(master, p, java, ip, localLauncher, hosts, verboseLauncher);

        final long beforeFuture = System.nanoTime();
        while (false == future.isDone()) {
          final long now = System.nanoTime();
          if ((now - beforeFuture) / 1E9 > timeoutStarting) {
            System.err.println(
                "[APGAS]: "
                    + ManagementFactory.getRuntimeMXBean().getName()
                    + " ran into timeout, exit JVM");
            System.exit(40);
          }

          TimeUnit.SECONDS.sleep(10);
          System.err.println(
              ManagementFactory.getRuntimeMXBean().getName() + " future is not done...waiting");
        }

        future.get();
        if (true == verboseLauncher) {
          System.err.println(ManagementFactory.getRuntimeMXBean().getName() + " future is done");
        }
        localTransport = tmpTransport;
        executor.shutdown();
      }

      this.transport = localTransport;

      // initialize here
      here = localTransport.here();
      home = new Place(here);
      resilientFinishMap = resilient ? localTransport.getResilientFinishMap() : null;

      if (true == verboseLauncher) {
        System.err.println("[APGAS] New place starting at " + localTransport.getAddress() + ".");
      }

      // install hook on thread 1
      if (master == null) {
        final Thread[] thread = new Thread[Thread.activeCount()];
        Thread.enumerate(thread);
        for (final Thread t : thread) {
          if (t != null && t.getId() == 1) {
            new Thread(
                    () -> {
                      while (t.isAlive()) {
                        try {
                          t.join();
                        } catch (final InterruptedException e) {
                        }
                      }
                      shutdown();
                    })
                .start();
            break;
          }
        }
      }

      // start monitoring cluster
      localTransport.start();

      // wait for enough places to join the global runtime
      final long beforeMaxPlaces = System.nanoTime();
      while (maxPlace() < p) {
        try {
          TimeUnit.SECONDS.sleep(3);
          if (true == verboseLauncher) {
            System.err.println(
                "[APGAS]: "
                    + ManagementFactory.getRuntimeMXBean().getName()
                    + " not all places are started, maxPlaces: "
                    + maxPlace());
          }

          final long now = System.nanoTime();
          if ((now - beforeMaxPlaces) / 1E9 > timeoutStarting) {
            System.err.println(
                "[APGAS]: "
                    + ManagementFactory.getRuntimeMXBean().getName()
                    + " ran into timeout, exit JVM");
            System.exit(40);
          }
        } catch (final InterruptedException e) {
        }
        if (localLauncher != null && !localLauncher.healthy()) {
          throw new Exception("A process exited prematurely");
        }
      }

      if (true == verboseLauncher) {
        System.err.println(
            "[APGAS]: "
                + ManagementFactory.getRuntimeMXBean().getName()
                + " = "
                + home
                + " has been started");
      }

      ready = true;

      if (here != 0) {
        if (true == verboseLauncher) {
          System.err.println("[APGAS] " + here + " sends ready to place 0");
        }
        final int _h = here;
        try {
          transport.send(
              0,
              new UncountedTask(
                  () -> {
                    int value = GlobalRuntime.readyCounter.decrementAndGet();
                    if (true == verboseLauncher) {
                      System.err.println(
                          "[APGAS}] "
                              + _h
                              + " on place 0 decremented ready counter, is now "
                              + value);
                    }
                  }));
        } catch (final Throwable e) {
        }
      } else {
        GlobalRuntime.readyCounter.decrementAndGet();

        while (GlobalRuntime.readyCounter.get() > 0) {
          if (true == verboseLauncher) {
            System.err.println(
                "[APGAS]: "
                    + here
                    + " not all constructors are ready, waiting...."
                    + GlobalRuntime.readyCounter.get());
          }
          TimeUnit.SECONDS.sleep(1);
        }
      }

      if (here == 0) {
        System.out.println(
            "[APGAS] Starting Places time: " + ((System.nanoTime() - begin) / 1E9) + " sec");
      }

    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void launchPlaces(
      String master,
      int p,
      String java,
      Transport localTransport,
      Launcher localLauncher,
      List<String> hosts,
      boolean verboseLauncher) {
    // launch additional places
    if (master == null && p > 1) {
      new Thread(
              () -> {
                try {
                  final ArrayList<String> command = new ArrayList<>();
                  command.add(java);
                  RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
                  List<String> inputArguments = runtimeMXBean.getInputArguments();
                  for (String a : inputArguments) {
                    if (a.contains("-X") == true) {
                      command.add(a);
                    }
                  }
                  command.add("-Duser.dir=" + System.getProperty("user.dir"));
                  // Java 9
                  //                    command.add("-Xbootclasspath:"
                  //                            +
                  // ManagementFactory.getRuntimeMXBean().getBootClassPath());
                  command.add("-cp");
                  command.add(System.getProperty("java.class.path"));
                  for (final String property : System.getProperties().stringPropertyNames()) {
                    if (property.startsWith("apgas.")) {
                      command.add("-D" + property + "=" + System.getProperty(property));
                    }
                  }
                  command.add(
                      "-D" + Configuration.APGAS_MASTER + "=" + localTransport.getAddress());
                  command.add(getClass().getSuperclass().getCanonicalName());

                  localLauncher.launch(p - 1, command, hosts, verboseLauncher);
                } catch (final Exception t) {
                  // initiate shutdown
                  shutdown();
                  // TODO
                  //        throw t;
                }
              })
          .start();
    }
  }

  private void launchPlaces(
      String master,
      int p,
      String java,
      String ip,
      Launcher localLauncher,
      List<String> hosts,
      boolean verboseLauncher) {
    // launch additional places
    if (master == null && p > 1) {
      new Thread(
              () -> {
                try {
                  final ArrayList<String> command = new ArrayList<>();
                  command.add(java);
                  RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
                  List<String> inputArguments = runtimeMXBean.getInputArguments();
                  for (String a : inputArguments) {
                    if (a.contains("-X") == true) {
                      command.add(a);
                    }
                  }
                  command.add("-Duser.dir=" + System.getProperty("user.dir"));
                  // Java 9
                  //                    command.add("-Xbootclasspath:"
                  //                            +
                  // ManagementFactory.getRuntimeMXBean().getBootClassPath());
                  command.add("-cp");
                  command.add(System.getProperty("java.class.path"));
                  for (final String property : System.getProperties().stringPropertyNames()) {
                    if (property.startsWith("apgas.")) {
                      command.add("-D" + property + "=" + System.getProperty(property));
                    }
                  }
                  command.add("-D" + Configuration.APGAS_MASTER + "=" + ip + ":5701");
                  command.add(getClass().getSuperclass().getCanonicalName());

                  localLauncher.launch(p - 1, command, hosts, verboseLauncher);
                } catch (final Exception t) {
                  // initiate shutdown
                  shutdown();
                  // TODO
                  //        throw t;
                }
              })
          .start();
    }
  }

  private static Worker currentWorker() {
    final Thread t = Thread.currentThread();
    return t instanceof Worker ? (Worker) t : null;
  }

  public static GlobalRuntimeImpl getRuntime() {
    while (runtime.ready != true) { // Wait for constructor
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
    return runtime;
  }

  /**
   * Updates the place collections.
   *
   * @param added added places
   * @param removed removed places
   */
  public void updatePlaces(List<Integer> added, List<Integer> removed) {
    synchronized (placeSet) {
      for (final int id : added) {
        placeSet.add(new Place(id));
      }
      for (final int id : removed) {
        placeSet.remove(new Place(id));
      }
      places = Collections.unmodifiableList(new ArrayList<>(placeSet));
    }
    if (removed.isEmpty()) {
      return;
    }
    if (!resilient) {
      shutdown();
      return;
    }
    final Consumer<Place> localHandler = this.handler;
    execute(
        new RecursiveAction() {
          private static final long serialVersionUID = 1052937749744648347L;

          @Override
          public void compute() {
            final Worker worker = (Worker) Thread.currentThread();
            worker.task = null; // a handler is not a task (yet)
            for (final int id : removed) {
              ResilientFinishState.purge(id);
            }
            if (localHandler != null) {
              for (final int id : removed) {
                localHandler.accept(new Place(id));
              }
            }
          }
        });
  }

  @Override
  public void setPlaceFailureHandler(Consumer<Place> handler) {
    this.handler = handler;
  }

  @Override
  public void setRuntimeShutdownHandler(Runnable handler) {
    this.shutdownHandler = handler;
  }

  @Override
  public void shutdown() {
    if (shutdownHandler != null) {
      shutdownHandler.run();
    }

    synchronized (this) {
      if (dying) {
        return;
      }
      dying = true;
    }
    if (launcher != null) {
      launcher.shutdown();
    }
    pool.shutdown();
    transport.shutdown();
  }

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
  public void finish(SerializableJob f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println("[APGAS] does not support a finish() within an finishAsyncAny!");
      return;
    }

    final Worker worker = currentWorker();
    final Finish finish =
        factory.make(
            worker == null || worker.task == null ? NullFinish.SINGLETON : worker.task.finish);
    new Task(finish, f, here).finish(worker);
    final List<Throwable> exceptions = finish.exceptions();
    if (exceptions != null) {
      throw MultipleException.make(exceptions);
    }
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
  public <T> T finish(Callable<T> f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println("[APGAS] does not support a finish() within an finishAsyncAny!");
      return null;
    }
    final Cell<T> cell = new Cell<>();
    finish(() -> cell.set(f.call()));
    return cell.get();
  }

  /**
   * Submits a new local task to the global runtime with body {@code f} and returns immediately.
   *
   * @param f the function to run
   */
  public void async(SerializableJob f) {
    final Worker worker = currentWorker();
    final Finish finish =
        worker == null || worker.task == null ? NullFinish.SINGLETON : worker.task.finish;
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println("[APGAS] does not support async calls within an finishAsyncAny!");
      return;
    }

    finish.spawn(here);
    Task task = new Task(finish, f, here);
    task.async(worker);
  }

  /**
   * Submits a new task to the global runtime to be run at {@link Place} {@code p} with body {@code
   * f} and returns immediately.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public void asyncAt(Place p, SerializableJob f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println("[APGAS] does not support asyncAt calls within an finishAsyncAny!");
      System.out.println(Arrays.deepToString(Thread.currentThread().getStackTrace()));
      return;
    }
    final Worker worker = currentWorker();
    final Finish finish =
        worker == null || worker.task == null ? NullFinish.SINGLETON : worker.task.finish;
    finish.spawn(p.id);

    new Task(finish, f, here).asyncAt(p.id);
  }

  /**
   * Submits an uncounted task to the global runtime to be run at {@link Place} {@code p} with body
   * {@code f} and returns immediately. The termination of this task is not tracked by the enclosing
   * finish. Exceptions thrown by the task are ignored.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public void uncountedAsyncAt(Place p, SerializableJob f) {
    new UncountedTask(f).uncountedAsyncAt(p.id);
  }

  /**
   * /** Submits an immediate task to the global runtime to be run at {@link Place} {@code p} with
   * body {@code f}.
   *
   * @param p the place of execution
   * @param f the function to run
   */
  public void immediateAsyncAt(Place p, SerializableRunnable f) {
    transport.sendImmediate(p.id, f);
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
  public void at(Place p, SerializableJob f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println("[APGAS] does not support an at() within an finishAsyncAny!");
      return;
    }
    Constructs.finish(() -> Constructs.asyncAt(p, f));
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
  @SuppressWarnings("unchecked")
  public <T extends Serializable> T at(Place p, SerializableCallable<T> f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println("[APGAS] does not support an at() within an finishAsyncAny!");
      return null;
    }
    final GlobalID id = new GlobalID();
    final Place _home = here();
    Constructs.finish(
        () ->
            Constructs.asyncAt(
                p,
                () -> {
                  final T _result = f.call();
                  Constructs.asyncAt(_home, () -> id.putHere(_result));
                }));
    return (T) id.removeHere();
  }

  /**
   * Returns the current {@link Place}.
   *
   * @return the current place
   */
  public Place here() {
    return home;
  }

  /**
   * Returns the current list of places in the global runtime.
   *
   * @return the current list of places in the global runtime
   */
  public List<? extends Place> places() {
    return places;
  }

  /**
   * Returns the place with the given ID.
   *
   * @param id the requested ID
   * @return the place with the given ID
   */
  public Place place(int id) {
    return new Place(id);
  }

  /**
   * Returns the first unused place ID.
   *
   * @return the first unused place ID
   */
  public int maxPlace() {
    return transport.maxPlace();
  }

  @Override
  public ExecutorService getExecutorService() {
    return pool;
  }

  /**
   * Submits a task to the pool making sure that a thread will be available to run it. run.
   *
   * @param task the task
   */
  void execute(ForkJoinTask<?> task) {
    pool.execute(task);
  }

  @Override
  public Long lastfailureTime() {
    return failureTime;
  }

  /**
   * Starts the management worker for enable local flexible tasks and their automatically
   * distribution over all threads and places.
   *
   * @param cyclical how often is partial result reduced, -1 for never
   */
  private void startAsyncAny(final long cyclical) {
    final int threads =
        Integer.getInteger(Configuration.APGAS_THREADS, Runtime.getRuntime().availableProcessors());
    final int maxThreads = Integer.getInteger(Config.APGAS_MAX_THREADS, 256);
    final int numPlaces = GlobalRuntimeImpl.getRuntime().places.size();
    int tmpL = 1;
    while ((tmpL * tmpL) < numPlaces) {
      tmpL++;
    }
    final int paraW = numPlaces > 6 ? 6 : numPlaces - 1;
    final int paraL = tmpL;
    final boolean paraLogger = loggerAsyncAny;
    GlobalRef<AtomicInteger> count = new GlobalRef<>(new AtomicInteger(places.size()));
    for (Place p : places()) {
      immediateAsyncAt(
          p,
          () -> {
            GlobalRuntimeImpl.getRuntime().result = new ResultAsyncAny[maxThreads];
            GlobalRuntimeImpl.getRuntime().manWorker =
                new ManagementWorker(paraW, paraL, threads, paraLogger);

            GlobalRuntimeImpl.getRuntime().manWorker.start(cyclical);
            GlobalRuntimeImpl.getRuntime().allowMoreAsyncAnyTask = true;

            Constructs.immediateAsyncAt(
                count.home(),
                () -> {
                  count.get().decrementAndGet();
                });
          });
    }

    while (count.get().get() > 0) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Reduces all asyncAny results and returns the result.
   *
   * @return the partial reduced result
   */
  public synchronized ResultAsyncAny reduceAsyncAny() {
    if (this.reduceCount.get() != 0) {
      return null;
    }

    final int localID = here;

    this.partialResult = reduceAsyncAnyLocal();

    if (this.partialResult == null) {
      return null;
    }

    this.reduceCount.set(1);

    for (Place p : places()) {
      if (p.id == localID) {
        continue;
      }
      Constructs.immediateAsyncAt(
          p,
          () -> {
            final ResultAsyncAny rr = GlobalRuntimeImpl.getRuntime().reduceAsyncAnyLocal();
            if (rr == null) {
              Constructs.immediateAsyncAt(
                  Constructs.place(localID),
                  () -> {
                    synchronized (GlobalRuntimeImpl.getRuntime().reduceCount) {
                      GlobalRuntimeImpl.getRuntime().reduceCount.incrementAndGet();
                      GlobalRuntimeImpl.getRuntime().reduceCount.notifyAll();
                    }
                  });
            } else {
              Constructs.immediateAsyncAt(
                  Constructs.place(localID),
                  () -> {
                    synchronized (GlobalRuntimeImpl.getRuntime().reduceCount) {
                      if (false
                          == GlobalRuntimeImpl.getRuntime()
                              .partialResult
                              .getClass()
                              .equals(rr.getClass())) {
                        System.err.println(
                            "[APGAS]: reduceAsyncAny() could not merge the result: two different result types");
                      } else {
                        GlobalRuntimeImpl.getRuntime().partialResult.mergeResult(rr);
                      }
                      GlobalRuntimeImpl.getRuntime().reduceCount.incrementAndGet();
                      GlobalRuntimeImpl.getRuntime().reduceCount.notifyAll();
                    }
                  });
            }
          });
    }

    synchronized (this.reduceCount) {
      while (this.reduceCount.get() < places().size()) {
        try {
          this.reduceCount.wait();
        } catch (InterruptedException e) {
          //                            e.printStackTrace();
        }
      }
    }
    this.reduceCount.set(0);
    return partialResult;
  }

  /**
   * Reduces all long results by using @param op and returns a singe long value.
   *
   * @param op used for reducing the results
   * @return the final reduced result
   */
  public synchronized Long reduceAsyncAnyLong(final LongBinaryOperatorSerializable op) {
    if (this.reduceCount.get() != 0) {
      return null;
    }

    final int localID = here;

    this.partialResult = new ResultAsyncAnyLong(reduceAsyncAnyLocalLong(op));

    if (this.partialResult == null) {
      return null;
    }

    this.reduceCount.set(1);

    for (Place p : places()) {
      if (p.id == localID) {
        continue;
      }
      Constructs.immediateAsyncAt(
          p,
          () -> {
            final long rr = GlobalRuntimeImpl.getRuntime().reduceAsyncAnyLocalLong(op);
            Constructs.immediateAsyncAt(
                Constructs.place(localID),
                () -> {
                  synchronized (GlobalRuntimeImpl.getRuntime().reduceCount) {
                    ((ResultAsyncAnyLong) GlobalRuntimeImpl.getRuntime().partialResult)
                        .mergeValue(rr, op);
                    GlobalRuntimeImpl.getRuntime().reduceCount.incrementAndGet();
                    GlobalRuntimeImpl.getRuntime().reduceCount.notifyAll();
                  }
                });
          });
    }

    synchronized (this.reduceCount) {
      while (this.reduceCount.get() < places().size()) {
        try {
          this.reduceCount.wait();
        } catch (InterruptedException e) {
          //                            e.printStackTrace();
        }
      }
    }
    this.reduceCount.set(0);
    return ((ResultAsyncAnyLong) partialResult).getInternalResult();
  }

  /**
   * Reduces all double results by using @param op and returns a singe long value.
   *
   * @param op used for reducing the results
   * @return the final reduced result
   */
  public synchronized Double reduceAsyncAnyDouble(final DoubleBinaryOperatorSerializable op) {
    if (this.reduceCount.get() != 0) {
      return null;
    }

    final int localID = here;

    this.partialResult = new ResultAsyncAnyDouble(reduceAsyncAnyLocalDouble(op));

    if (this.partialResult == null) {
      return null;
    }

    this.reduceCount.set(1);

    for (Place p : places()) {
      if (p.id == localID) {
        continue;
      }
      Constructs.immediateAsyncAt(
          p,
          () -> {
            final double rr = GlobalRuntimeImpl.getRuntime().reduceAsyncAnyLocalDouble(op);
            Constructs.immediateAsyncAt(
                Constructs.place(localID),
                () -> {
                  synchronized (GlobalRuntimeImpl.getRuntime().reduceCount) {
                    ((ResultAsyncAnyDouble) GlobalRuntimeImpl.getRuntime().partialResult)
                        .mergeValue(rr, op);
                    GlobalRuntimeImpl.getRuntime().reduceCount.incrementAndGet();
                    GlobalRuntimeImpl.getRuntime().reduceCount.notifyAll();
                  }
                });
          });
    }

    synchronized (this.reduceCount) {
      while (this.reduceCount.get() < places().size()) {
        try {
          this.reduceCount.wait();
        } catch (InterruptedException e) {
          //                            e.printStackTrace();
        }
      }
    }
    this.reduceCount.set(0);
    return ((ResultAsyncAnyDouble) partialResult).getInternalResult();
  }

  /**
   * Reduces the place internal result over all thread results.
   *
   * @return the place internal result
   */
  public ResultAsyncAny reduceAsyncAnyLocal() {
    ResultAsyncAny local = null;

    if (GlobalRuntimeImpl.getRuntime().result == null) {
      return null;
    }

    for (ResultAsyncAny entry : GlobalRuntimeImpl.getRuntime().result) {
      if (entry == null) {
        continue;
      }

      if (local == null) {
        local = entry.clone();
      } else {
        if (false == local.getClass().equals(entry.getClass())) {
          System.err.println(
              "[APGAS]: reduceAsyncAny() could not merge the result: two different result types");
        } else {
          local.mergeResult(entry);
        }
      }
    }
    return local;
  }

  /**
   * Reduces the place internal long result over all thread results.
   *
   * @param op used for reducing the results
   * @return the place internal long result
   */
  public Long reduceAsyncAnyLocalLong(LongBinaryOperatorSerializable op) {
    long local = 0L;

    if (GlobalRuntimeImpl.getRuntime().result == null) {
      return null;
    }

    for (ResultAsyncAny entry : GlobalRuntimeImpl.getRuntime().result) {
      if (entry == null) {
        continue;
      }

      if (false == entry.getClass().equals(ResultAsyncAnyLong.class)) {
        System.err.println(
            "[APGAS]: reduceAsyncAny() could not merge the result: two different result types");
      } else {
        local = op.applyAsLong(((ResultAsyncAnyLong) entry).getInternalResult(), local);
      }
    }
    return local;
  }

  /**
   * Reduces the place internal long result over all thread results.
   *
   * @param op used for reducing the results
   * @return the place internal long result
   */
  public Double reduceAsyncAnyLocalDouble(DoubleBinaryOperatorSerializable op) {
    double local = 0;

    if (GlobalRuntimeImpl.getRuntime().result == null) {
      return null;
    }

    for (ResultAsyncAny entry : GlobalRuntimeImpl.getRuntime().result) {
      if (entry == null) {
        continue;
      }

      if (false == entry.getClass().equals(ResultAsyncAnyDouble.class)) {
        System.err.println(
            "[APGAS]: reduceAsyncAny() could not merge the result: two different result types");
      } else {
        local = op.applyAsDouble(((ResultAsyncAnyDouble) entry).getInternalResult(), local);
      }
    }
    return local;
  }

  /** Stops the management workers */
  private void stopAsyncAny() {
    final GlobalRef<AtomicInteger> count = new GlobalRef<>(new AtomicInteger(places.size()));
    for (Place p : places) {
      immediateAsyncAt(
          p,
          () -> {
            if (getRuntime().manWorker != null) {
              getRuntime().manWorker.stop();
              Constructs.immediateAsyncAt(
                  count.home(),
                  () -> {
                    count.get().decrementAndGet();
                  });
            }
          });
    }

    while (count.get().get() > 0) {
      try {
        //        System.out.println("sleeping");
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Sends a list of tasks {@code list} from {@code victim} to {@code thief}. {@code victim} will
   * execute them.
   *
   * @param thief gets tasks
   * @param list list of tasks
   * @param victim the sender
   * @param lifeline true whether the tasks are sent over a lifeline
   */
  public void asyncAtTaskList(Place thief, List<Task> list, int victim, boolean lifeline) {
    if (null == manWorker || false == manWorker.isActive()) {
      System.err.println("[APGAS]: asyncAtTaskList() can only be called within finishAsyncAny");
      return;
    }

    final TaskList tasks = new TaskList(list.size(), victim, lifeline);
    for (Task t : list) {
      tasks.add(t);
    }

    try {
      GlobalRuntimeImpl.getRuntime().transport.send(thief.id, tasks);
    } catch (final Throwable e) {
      e.printStackTrace();
      tasks.unspawn(thief);
      if (GlobalRuntimeImpl.getRuntime().verboseSerialization
          && !(e instanceof DeadPlaceException)) {
        System.err.println(
            "[APGAS] Failed to spawn a task list at place " + thief + " due to: " + e);
      }
      throw e;
    }
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
  public void finishAsyncAny(SerializableJob f) {
    this.finishAsyncAny(f, -1);
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
  public void finishAsyncAny(SerializableJob f, long cyclical) {
    final GlobalRef<AtomicInteger> placesReady = new GlobalRef<>(new AtomicInteger(places.size()));

    for (final Place p : places) {
      Constructs.immediateAsyncAt(
          p,
          () -> {
            boolean activeTasks = true;
            int k = 0;
            MyForkJoinPool localPool = GlobalRuntimeImpl.getRuntime().getPool();
            while (true == activeTasks) {
              activeTasks = false;
              k++;
              if (localPool.getActiveThreadCount() > 0) {
                activeTasks = true;
              }
              if (false == localPool.isQuiescent()) {
                activeTasks = true;
              }
              if (true == activeTasks) {
                try {
                  TimeUnit.MILLISECONDS.sleep(100);
                  if (k >= 20) {
                    k = 0;
                    ConsolePrinter.getInstance()
                        .println(
                            Constructs.here()
                                + " [finishAsyncAny] found task in my local pool: WAITING");
                  }
                } catch (InterruptedException e) {
                }
              }
            }

            Constructs.immediateAsyncAt(
                placesReady.home(),
                () -> {
                  synchronized (placesReady.get()) {
                    placesReady.get().decrementAndGet();
                    placesReady.get().notifyAll();
                  }
                });
          });
    }

    synchronized (placesReady.get()) {
      while (placesReady.get().get() > 0) {
        try {
          placesReady.get().wait();
          ConsolePrinter.getInstance()
              .println(
                  Constructs.here()
                      + " [finishAsyncAny] some places have unfinished tasks: WAITING");
        } catch (InterruptedException e) {
          //          e.printStackTrace();
        }
      }
    }
    // at this point it is guaranteed that all pools are empty

    if (FinishAsyncAny.SINGLETON.firstStart == true) {
      finish(
          () -> {
            for (Place p : places()) {
              asyncAt(
                  p,
                  () -> {
                    final int threads =
                        Integer.getInteger(
                            Configuration.APGAS_THREADS,
                            Runtime.getRuntime().availableProcessors());
                    for (int i = 0; i < threads; i++) {
                      GlobalRuntimeImpl.getRuntime()
                          .getPool()
                          .submit(
                              () -> {
                                try {
                                  TimeUnit.MILLISECONDS.sleep(10);
                                } catch (InterruptedException e) {
                                  e.printStackTrace();
                                }
                              });
                    }
                    FinishAsyncAny.SINGLETON.firstStart = false;
                  });
            }
          });
    }

    startAsyncAny(cyclical);

    final Worker worker = currentWorker();
    final Finish finish = FinishAsyncAny.SINGLETON;
    new Task(finish, f, here).finish(worker);
    final List<Throwable> exceptions = finish.exceptions();
    if (exceptions != null) {
      throw MultipleException.make(exceptions);
    }
    stopAsyncAny();
    getRuntime().getManWorker().collectLifelineStatus();
  }

  /**
   * Runs {@code f} as a local flexible task, which will be executed by any work-free place.
   *
   * @param f the function to run
   */
  public void asyncAny(SerializableJob f) {
    if (null == manWorker || false == manWorker.isActive()) {
      System.err.println("[APGAS]: asyncAny() can only be called within finishAsyncAny");
      return;
    }

    final Worker worker = currentWorker();
    final Finish finish = FinishAsyncAny.SINGLETON;
    new Task(finish, f, here).async(worker);

    synchronized (this.manWorker.waiting) {
      this.manWorker.waiting.set(false);
      this.manWorker.waiting.notifyAll();
    }
  }

  /**
   * Enables static initial task distribution. Executes {@code f} on each place to initialize data.
   *
   * <p>Must be called before an {@link #finishAsyncAny(SerializableJob)} or {@link
   * #finishAsyncAny(SerializableJob, long)}.
   *
   * @param f the function to run
   */
  public void staticInit(SerializableJob f) {
    for (Place p : places) {
      finish(
          () -> {
            asyncAt(
                p,
                () -> {
                  f.run();
                });
          });
    }
  }

  /**
   * Enables static initial task distribution.
   *
   * <p>Must be called before an {@link #finishAsyncAny(SerializableJob)} or {@link
   * #finishAsyncAny(SerializableJob, long)}.
   */
  public void enableStaticDistribution() {
    if (null != manWorker && false == manWorker.isActive()) {
      System.err.println(
          "[APGAS]: enableStaticDistribution() can only be called before finishAsyncAny");
    }

    for (Place p : places) {
      finish(
          () -> {
            asyncAt(
                p,
                () -> {
                  ManagementWorker.staticDistribution = true;
                });
          });
    }
  }

  /**
   * Encapsulates {@code f} as a new APGAS task and returns it.
   *
   * @param f the function for the new Task
   * @return new created APGAS task
   */
  public Task createAsyncAnyTask(SerializableJob f) {
    final Finish finish = FinishAsyncAny.SINGLETON;
    return new Task(finish, f, here);
  }

  /**
   * Returns the internal pool
   *
   * @return the internal p
   */
  public MyForkJoinPool getPool() {
    return this.pool;
  }

  /**
   * Returns the matching thread local result.
   *
   * @param <T> Result Type
   * @return thread local result
   */
  public <T extends ResultAsyncAny> T getThreadLocalResult() {
    Worker worker = (Worker) Thread.currentThread();
    final int pos = worker.getMyID();
    if (null == result) {
      return null;
    }
    return (T) this.result[pos];
  }

  /**
   * Sets the matching thread local result
   *
   * @param result the new result
   */
  public void setThreadLocalResult(ResultAsyncAny result) {
    Worker worker = (Worker) Thread.currentThread();
    final int pos = worker.getMyID();
    if (null == result) {
      System.err.println("[APGAS]: setThreadLocalResult() could not set a result");
      return;
    }
    this.result[pos] = result;
  }

  /**
   * Starts {@code count} tasks with {@code f} and distributes them evenly over all places.
   *
   * @param f the function to run
   * @param count count of tasks to be create
   */
  public void staticAsyncAny(SerializableJob f, int count) {
    if (null == manWorker || false == manWorker.isActive()) {
      System.err.println("[APGAS]: staticAsyncAny() can only be called within finishAsyncAny");
      return;
    }

    final int stepSize = (int) Math.ceil((double) count / (double) places().size());

    for (int currentID = places.size() - 1; currentID >= 0; currentID--) {
      int from = currentID * stepSize;
      int to = Math.min(count, (currentID + 1) * stepSize);

      ArrayList<Task> list = new ArrayList<>();
      for (int i = from; i < to; i++) {
        Task newTask = createAsyncAnyTask(f);
        list.add(newTask);
      }

      FinishAsyncAny.SINGLETON.incrementCounts(currentID);
      asyncAtTaskList(place(currentID), list, 0, false);
    }

    for (Place p : places()) {
      immediateAsyncAt(
          p,
          () -> {
            ManagementWorker.staticDistribution = true;
          });
    }
  }

  /**
   * Starts on {@code remote} place a list of tasks {@code jobList}.
   *
   * @param remote remote place
   * @param jobList list of jobs for tasks
   */
  public void staticAsyncAny(Place remote, List<SerializableJob> jobList) {
    if (null == manWorker || false == manWorker.isActive()) {
      System.err.println("[APGAS]: staticAsyncAny() can only be called within finishAsyncAny");
      return;
    }

    ArrayList<Task> list = new ArrayList<>();
    for (SerializableJob job : jobList) {
      Task newTask = createAsyncAnyTask(job);
      list.add(newTask);
    }

    FinishAsyncAny.SINGLETON.incrementCounts(remote.id);

    asyncAtTaskList(remote, list, 0, false);

    immediateAsyncAt(
        remote,
        () -> {
          ManagementWorker.staticDistribution = true;
        });
  }

  /**
   * TODO: test me! Starts on {@code remote} place a list of tasks {@code jobList}.
   *
   * @param remote remote place
   * @param jobList list of jobs for tasks
   */
  public void asyncStaticAsyncAny(Place remote, List<SerializableJob> jobList) {
    if (null == manWorker || false == manWorker.isActive()) {
      System.err.println("[APGAS]: staticAsyncAny() can only be called within finishAsyncAny");
      return;
    }

    FinishAsyncAny.SINGLETON.incrementCounts(remote.id);

    immediateAsyncAt(
        remote,
        () -> {
          ManagementWorker.staticDistribution = true;
          FinishAsyncAny.SINGLETON.decrementMyCounts();

          for (SerializableJob job : jobList) {
            getRuntime().createAsyncAnyTask(job).run();
          }

          synchronized (GlobalRuntimeImpl.getRuntime().getManWorker().waiting) {
            getRuntime().getManWorker().waiting.set(false);
            getRuntime().getManWorker().waiting.notifyAll();
          }
        });
  }

  /**
   * Submits a new task to the global runtime to be run at {@link Place} {@code p} with body {@code
   * f} and returns {@link AsyncFuture} immediately.
   *
   * @param p the place of execution
   * @param f the {@link SerializableCallable} to run
   */
  public <T extends Serializable> AsyncFuture<T> asyncAtWithFuture(
      Place p, SerializableCallable<T> f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println(
          "[APGAS] does not support asyncAtWithFuture calls within an finishAsyncAny!");
      return null;
    }
    final Place _home = here();
    AsyncFuture<T> future = new AsyncFuture<>();
    this.asyncFutures.put(future.myID, future);
    final int _id = future.myID;

    Constructs.asyncAt(
        p,
        () -> {
          final T call = f.call();
          Constructs.immediateAsyncAt(
              _home,
              () -> {
                GlobalRuntimeImpl.getRuntime().asyncFutures.get(_id).put(call);
              });
        });
    return future;
  }

  /**
   * Submits a new local task to the global runtime with body {@code f} and returns a {@link
   * AsyncFuture} immediately.
   *
   * @param f the {@link SerializableCallable} to run
   */
  public <T extends Serializable> AsyncFuture<T> asyncWithFuture(SerializableCallable<T> f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println(
          "[APGAS] does not support asyncWithFuture calls within an finishAsyncAny!");
      return null;
    }

    AsyncFuture<T> future = new AsyncFuture<>();
    this.asyncFutures.put(future.myID, future);
    final int _id = future.myID;

    Constructs.async(
        () -> {
          T call = f.call();
          GlobalRuntimeImpl.getRuntime().asyncFutures.get(_id).put(call);
        });

    return future;
  }

  /**
   * Runs {@code f} as a local flexible task, which will be executed by any work-free place. Returns
   * a {@link AsyncFuture} immediately.
   *
   * @param f the function to run
   */
  public <T extends Serializable> AsyncFuture<T> asyncAnyWithFuture(SerializableCallable<T> f) {
    if (null != manWorker && true == manWorker.isActive()) {
      System.err.println(
          "[APGAS] does not support asyncAnyWithFuture calls within an finishAsyncAny!");
      return null;
    }
    final Place _home = here();
    AsyncFuture<T> future = new AsyncFuture<>();
    this.asyncFutures.put(future.myID, future);
    final int _id = future.myID;

    Constructs.asyncAny(
        () -> {
          final T call = f.call();
          Constructs.immediateAsyncAt(
              _home,
              () -> {
                GlobalRuntimeImpl.getRuntime().asyncFutures.get(_id).put(call);
              });
        });
    return future;
  }

  /**
   * Submits a new local task to the global runtime with body {@code f}. Returns the generated
   * {@link Task} immediately.
   *
   * @param f the function to run
   */
  public void cancelableAsyncAny(SerializableJob f) {
    if (null == manWorker || false == manWorker.isActive()) {
      System.err.println("[APGAS]: cancelableAsyncAny() can only be called within finishAsyncAny");
      return;
    }

    if (false == allowMoreAsyncAnyTask) {
      return;
    }

    final Worker worker = currentWorker();
    final Finish finish = FinishAsyncAny.SINGLETON;
    Task task = new Task(finish, f, here, true);

    this.localCancelableTasks.put(task.getId(), task);

    task.async(worker);

    synchronized (this.manWorker.waiting) {
      this.manWorker.waiting.set(false);
      this.manWorker.waiting.notifyAll();
    }
  }

  /** Cancels all system wide cancelable tasks */
  public void cancelAllCancelableAsyncAny() {
    if (false == GlobalRuntimeImpl.getRuntime().allowMoreAsyncAnyTask) {
      return;
    }

    for (Place p : places()) {
      immediateAsyncAt(
          p,
          () -> {
            if (false == GlobalRuntimeImpl.getRuntime().allowMoreAsyncAnyTask) {
              return;
            }

            GlobalRuntimeImpl.getRuntime().allowMoreAsyncAnyTask = false;
            while (false == GlobalRuntimeImpl.getRuntime().localCancelableTasks.isEmpty()) {
              for (Long id : GlobalRuntimeImpl.getRuntime().localCancelableTasks.keySet()) {
                Task remove = GlobalRuntimeImpl.getRuntime().localCancelableTasks.remove(id);
                if (remove != null) {
                  remove.cancel(true);
                }
              }
            }
          });
    }
  }

  public ResultAsyncAny[] getResult() {
    return result;
  }

  public ManagementWorker getManWorker() {
    return manWorker;
  }

  public Map<Integer, AsyncFuture> getAsyncFutures() {
    return asyncFutures;
  }

  public boolean areMoreAsyncAnyTasksAllowed() {
    return allowMoreAsyncAnyTask;
  }

  /**
   * Returns the liveness of a place
   *
   * @return isDead
   */
  public boolean isDead(Place place) {
    return !this.places.contains(place);
  }

  /**
   * Returns the next place
   *
   * @return the next place
   */
  public Place nextPlace(Place place) {
    List<? extends Place> tmpPlaces = new ArrayList<>(places());
    for (Place p : tmpPlaces) {
      if (p.id > place.id) {
        return p;
      }
    }
    return tmpPlaces.get(0);
  }

  /**
   * Returns the previous place
   *
   * @return the previuos place
   */
  public Place prevPlace(Place place) {
    List<? extends Place> tmpPlaces = new ArrayList<>(places());
    Place prev = tmpPlaces.get(tmpPlaces.size() - 1);
    for (Place p : tmpPlaces) {
      if (p.id < place.id) {
        prev = p;
      } else {
        return prev;
      }
    }
    return prev;
  }

  public Map<Integer, Member> getMembers() {
    return transport.getMembers();
  }

  /**
   * Merges {@code result} to the matching thread partial result
   *
   * @param r the partial result to merge
   */
  public void mergeAsyncAny(ResultAsyncAny r) {
    final int pos = getCurrentWorker().getMyID();

    if (null == result) {
      System.err.println("[APGAS]: mergeAsyncAny() could not merge the result");
    }

    if (null != result[pos]) {
      if (false == r.getClass().equals(result[pos].getClass())) {
        System.err.println(
            "[APGAS]: mergeAsyncAny() could not merge the result: two different result types");
        return;
      }

      result[pos].mergeResult(r);
    } else {
      result[pos] = r;
    }
  }

  /**
   * Merges {@code result} to the matching thread partial result
   *
   * @param value the partial result to merge
   * @param op the merge operator
   */
  public void mergeAsyncAny(long value, LongBinaryOperatorSerializable op) {
    final int pos = getCurrentWorker().getMyID();

    if (null == result) {
      System.err.println("[APGAS]: mergeAsyncAny() could not merge the result");
    }

    if (null != result[pos]) {
      if (false == ResultAsyncAnyLong.class.equals(result[pos].getClass())) {
        System.err.println(
            "[APGAS]: mergeAsyncAny() could not merge the result: two different result types");
        return;
      }
      ((ResultAsyncAnyLong) result[pos]).mergeValue(value, op);
    } else {
      result[pos] = new ResultAsyncAnyLong(value);
    }
  }

  /**
   * Merges {@code result} to the matching thread partial result
   *
   * @param value the partial result to merge
   * @param op the merge operator
   */
  public void mergeAsyncAny(double value, DoubleBinaryOperatorSerializable op) {
    final int pos = getCurrentWorker().getMyID();

    if (null == result) {
      System.err.println("[APGAS]: mergeAsyncAny() could not merge the result");
    }

    if (null != result[pos]) {
      if (false == ResultAsyncAnyDouble.class.equals(result[pos].getClass())) {
        System.err.println(
            "[APGAS]: mergeAsyncAny() could not merge the result: two different result types");
        return;
      }
      ((ResultAsyncAnyDouble) result[pos]).mergeValue(value, op);
    } else {
      result[pos] = new ResultAsyncAnyDouble(value);
    }
  }

  /**
   * Returns the current worker
   *
   * @return the worker place
   */
  public Worker getCurrentWorker() {
    return (Worker) Thread.currentThread();
  }

  /**
   * Returns the number of local workers (same on every place)
   *
   * @return the number of local workers
   */
  public int numLocalWorkers() {
    return numLocalWorkers;
  }
}
