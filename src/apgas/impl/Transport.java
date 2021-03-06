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
import apgas.DeadPlaceException;
import apgas.Place;
import apgas.util.ConsolePrinter;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.spi.ExecutionService;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** The {@link Transport} class manages the Hazelcast cluster and implements active messages. */
public class Transport
    implements com.hazelcast.core.ItemListener<Member>, InitialMembershipListener {

  private static final String APGAS = "apgas";
  private static final String APGAS_PLACES = "apgas:places";
  private static final String APGAS_EXECUTOR = "apgas:executor";
  private static final String APGAS_FINISH = "apgas:finish";

  /** The Hazelcast instance for this JVM. */
  private HazelcastInstance hazelcast;

  /** The place ID for this JVM. */
  private int here;

  /** The current members indexed by place ID. */
  private final Map<Integer, Member> map = new ConcurrentHashMap<>();

  /** Past and present members indexed by place ID. */
  private IList<Member> allMembers;

  /** The local member. */
  private Member me;

  /** Executor service for sending active messages. */
  private IExecutorService executor;

  /** The global runtime instance to notify of new and dead places. */
  private final GlobalRuntimeImpl runtime;

  /** The first unused place ID. */
  private int maxPlace;

  /** Current members. */
  private Set<Member> currentMembers;

  /** Registration ID. */
  private String regMembershipListener;

  /** Registration ID. */
  private String regItemListener;

  /** Hazelcast config */
  private final Config config;

  /**
   * Initializes the {@link HazelcastInstance} for this global runtime instance.
   *
   * @param runtime the global runtime instance
   * @param master member to connect to or null
   * @param localhost the preferred ip address of this host or null
   * @param compact reduce thread creation if set
   * @param kryo use kryo serialization if set
   * @param backupCount number of backups to use for distributed data structures
   */
  protected Transport(
      GlobalRuntimeImpl runtime,
      String master,
      String localhost,
      String launcherName,
      boolean compact,
      boolean kryo,
      int backupCount) {
    this.runtime = runtime;
    // config
    config = new Config();
    config.setProperty("hazelcast.logging.type", "none");
    config.setProperty("hazelcast.wait.seconds.before.join", "0");
    //    TODO: experimental
    //    config.setProperty("hazelcast.operation.call.timeout.millis", "600000");
    config.setProperty("hazelcast.socket.connect.timeout.seconds", "1");

    if (System.getProperty(Configuration.APGAS_PLACES) != null) {
      // experimental
      config.setProperty(
          "hazelcast.partition.count", System.getProperty(Configuration.APGAS_PLACES));

      config.setProperty(
          "hazelcast.initial.min.cluster.size",
          "" + System.getProperty(Configuration.APGAS_PLACES));
    }

    NetworkConfig networkConfig = config.getNetworkConfig();

    if ("apgas.impl.SrunKasselLauncher".equals(launcherName)) {
      try {
        final String hostName = InetAddress.getLocalHost().getHostName();
        if (null != hostName
            && hostName.contains(".its.uni-kassel.de")
            && false == (hostName.contains("its-cs1.") || hostName.contains("its-cs10."))
            && null != localhost
            && localhost.contains(SrunKasselLauncher.IPRANGE)) {
          System.err.println(
              "[APGAS] sets network config to kassel cluster (infiniband "
                  + SrunKasselLauncher.IPRANGE
                  + ")");
          networkConfig
              .getInterfaces()
              .setEnabled(true)
              .addInterface(SrunKasselLauncher.IPRANGE + ".*");
        } else {
          System.err.println(
              "[APGAS] WARNING: using apgas.impl.SrunKasselLauncher but NOT infiniband");
        }
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }

    if (compact) {
      config.setProperty("hazelcast.operation.thread.count", "2");
      config.setProperty("hazelcast.operation.generic.thread.count", "2");
      config.setProperty("hazelcast.io.thread.count", "2");
      config.setProperty("hazelcast.event.thread.count", "2");
      config.addExecutorConfig(new ExecutorConfig(ExecutionService.ASYNC_EXECUTOR, 2));
      config.addExecutorConfig(new ExecutorConfig(ExecutionService.SYSTEM_EXECUTOR, 2));
      config.addExecutorConfig(new ExecutorConfig(ExecutionService.SCHEDULED_EXECUTOR, 2));
    }

    // kryo
    if (kryo) {
      config
          .getSerializationConfig()
          .addSerializerConfig(
              new SerializerConfig()
                  .setTypeClass(SerializableRunnable.class)
                  .setImplementation(new KryoSerializer()));
    }

    config.addMapConfig(
        new MapConfig(APGAS_FINISH)
            .setInMemoryFormat(InMemoryFormat.OBJECT)
            .setBackupCount(backupCount));

    // join config
    final JoinConfig join = config.getNetworkConfig().getJoin();
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(true);
    if (localhost != null) {
      System.setProperty("hazelcast.local.localAddress", localhost);
    }
    if (master != null) {
      join.getTcpIpConfig().addMember(master);
    }
    config.setInstanceName(APGAS);
    config.addListConfig(new ListConfig(APGAS_PLACES).setBackupCount(backupCount));
  }

  public boolean startHazelcast() {
    try {
      hazelcast = Hazelcast.newHazelcastInstance(config);
      me = hazelcast.getCluster().getLocalMember();

      allMembers = hazelcast.getList(APGAS_PLACES);
      allMembers.add(me);
      int id = 0;
      for (final Member member : allMembers) {
        if (member.getUuid().equals(me.getUuid())) {
          break;
        }
        ++id;
      }
      here = id;

      executor = hazelcast.getExecutorService(APGAS_EXECUTOR);
    } catch (Throwable t) {
      System.err.println(
          "[APGAS] startHazelcast: "
              + ManagementFactory.getRuntimeMXBean().getName()
              + " throws Exception");
      t.printStackTrace();
    }
    return true;
  }

  /** Starts monitoring cluster membership events. */
  protected synchronized void start() {
    regItemListener = allMembers.addItemListener(this, false);
    regMembershipListener = hazelcast.getCluster().addMembershipListener(this);
  }

  /**
   * Returns the distributed map instance with the given name.
   *
   * @param <K> key type
   * @param <V> value type
   * @param name map name
   * @return the map
   */
  <K, V> IMap<K, V> getMap(String name) {
    return hazelcast.getMap(name);
  }

  /**
   * Returns the distributed map instance implementing resilient finish.
   *
   * @param <K> key type
   * @param <V> value type
   * @return the map
   */
  <K, V> IMap<K, V> getResilientFinishMap() {
    return hazelcast.getMap(APGAS_FINISH);
  }

  /**
   * Returns the socket address of this Hazelcast instance.
   *
   * @return an address in the form "ip:port"
   */
  protected String getAddress() {
    final InetSocketAddress address = me.getSocketAddress();
    return address.getAddress().getHostAddress() + ":" + address.getPort();
  }

  /** Shuts down this Hazelcast instance. */
  protected synchronized void shutdown() {
    hazelcast.getCluster().removeMembershipListener(regMembershipListener);
    allMembers.removeItemListener(regItemListener);
    hazelcast.shutdown();
  }

  /**
   * Returns the first unused place ID.
   *
   * @return a place ID.
   */
  protected int maxPlace() {
    return maxPlace;
  }

  /**
   * Returns the current place ID.
   *
   * @return the place ID of this Hazelcast instance
   */
  protected int here() {
    return here;
  }

  /**
   * Executes a function at the given place.
   *
   * @param place the requested place of execution
   * @param f the function to execute
   * @throws DeadPlaceException if the cluster does not contain this place
   */
  protected void send(int place, SerializableRunnable f) {
    if (place == here) {
      f.run();
    } else {
      final Member member = map.get(place);
      if (member == null) {
        throw new DeadPlaceException(new Place(place));
      }
      executor.executeOnMember(f, member);
    }
  }

  protected void sendImmediate(int place, SerializableRunnable f) {
    if (place == here) {
      new Thread(f).start();
      ConsolePrinter.getInstance()
          .println("[APGAS]: " + here + " Transport[sendImmediate]: new Thread started");
    } else {
      send(place, f);
    }
  }

  private boolean live(String uuid) {
    for (final Member member : currentMembers) {
      if (uuid.equals(member.getUuid())) {
        return true;
      }
    }
    return false;
  }

  private synchronized void updatePlaces() {
    if (currentMembers == null) {
      return;
    }
    final Iterator<Member> it = allMembers.iterator();
    final ArrayList<Integer> added = new ArrayList<>();
    final ArrayList<Integer> removed = new ArrayList<>();
    int place = 0;
    while (it.hasNext()) {
      final Member member = it.next();
      if (live(member.getUuid())) {
        if (!map.containsKey(place)) {
          added.add(place);
          map.put(place, member);
        }
      } else {
        if (map.containsKey(place)) {
          removed.add(place);
          map.remove(place);
        }
      }
      ++place;
    }
    if (place > maxPlace) {
      maxPlace = place;
    }
    if (!added.isEmpty() || !removed.isEmpty()) {
      runtime.updatePlaces(added, removed);
    }
  }

  @Override
  public synchronized void init(InitialMembershipEvent event) {
    currentMembers = event.getMembers();
    updatePlaces();
  }

  @Override
  public synchronized void memberAdded(MembershipEvent membershipEvent) {
    currentMembers = membershipEvent.getMembers();
    updatePlaces();
  }

  @Override
  public synchronized void memberRemoved(MembershipEvent membershipEvent) {
    runtime.failureTime = System.nanoTime();
    currentMembers = membershipEvent.getMembers();
    updatePlaces();
  }

  @Override
  public synchronized void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
    // unused
  }

  @Override
  public synchronized void itemAdded(ItemEvent<Member> item) {
    updatePlaces();
  }

  @Override
  public synchronized void itemRemoved(ItemEvent<Member> item) {
    // unused
  }

  public Map<Integer, Member> getMembers() {
    return map;
  }
}
