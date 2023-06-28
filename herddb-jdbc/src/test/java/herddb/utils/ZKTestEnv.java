/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.utils;

import herddb.network.netty.NetworkUtils;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZKTestEnv implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ZKTestEnv.class.getName());

    TestingServer zkServer;
    EmbeddedServer embeddedServer;
    Path path;

    public ZKTestEnv(Path path) throws Exception {
        zkServer = new TestingServer(NetworkUtils.assignFirstFreePort(), path.toFile(), true);
        // waiting for ZK to be reachable
        CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(zkServer.getConnectString(),
                herddb.server.ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT, (WatchedEvent event) -> {
                    LOG.log(Level.INFO, "ZK EVENT {0}", event);
                    if (event.getState() == KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                });
        try {
            if (!latch.await(herddb.server.ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT, TimeUnit.MILLISECONDS)) {
                LOG.log(Level.INFO, "ZK client did not connect withing {0} seconds, maybe the server did not start up",
                        herddb.server.ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT);
            }
        } finally {
            zk.close(1000);
        }
        this.path = path;
    }

    public void startBookie() throws Exception {
        startBookie(true);
    }

    public void startBookie(boolean format) throws Exception {
        if (embeddedServer != null) {
            throw new Exception("bookie already started");
        }
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(0);
        conf.setUseHostNameAsBookieID(true);

        Path targetDir = path.resolve("bookie_data");
        conf.setMetadataServiceUri("zk+null://" + zkServer.getConnectString() + herddb.server.ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
        conf.setLedgerDirNames(new String[]{targetDir.toAbsolutePath().toString()});
        conf.setJournalDirName(targetDir.toAbsolutePath().toString());
        conf.setFlushInterval(10000);
        conf.setGcWaitTime(5);
        conf.setJournalFlushWhenQueueEmpty(true);
//        conf.setJournalBufferedEntriesThreshold(1);
        conf.setAutoRecoveryDaemonEnabled(false);
        conf.setEnableLocalTransport(true);
        conf.setJournalSyncData(false);

        conf.setAllowLoopback(true);
        conf.setProperty("journalMaxGroupWaitMSec", 10); // default 200ms

        try (ZooKeeperClient zkc = ZooKeeperClient
                .newBuilder()
                .connectString(zkServer.getConnectString())
                .sessionTimeoutMs(10000)
                .build()) {

            boolean rootExists = zkc.exists(getPath(), false) != null;

            if (!rootExists) {
                zkc.create(getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        }

        if (format) {
            BookKeeperAdmin.initNewCluster(conf);
            BookKeeperAdmin.format(conf, false, true);
        }

        BookieConfiguration bkConf = new BookieConfiguration(conf);
        embeddedServer = EmbeddedServer.builder(bkConf).build();

        embeddedServer.getLifecycleComponentStack().start();
        waitForBookieServiceState(Lifecycle.State.STARTED);
    }

    public String getAddress() {
        return zkServer.getConnectString();
    }

    public int getTimeout() {
        return 40000;
    }

    public String getPath() {
        return "/herdtest";
    }

    @Override
    public void close() throws Exception {
        try {
            if (embeddedServer != null) {
                embeddedServer.getLifecycleComponentStack().close();
                waitForBookieServiceState(Lifecycle.State.CLOSED);
            }
        } catch (Throwable t) {
        }
        try {
            if (zkServer != null) {
                zkServer.close();
            }
        } catch (Throwable t) {
        }
    }

    private boolean waitForBookieServiceState(Lifecycle.State expectedState) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Lifecycle.State currentState = embeddedServer.getBookieService().lifecycleState();
            if (currentState == expectedState) {
                System.out.println("bookie is in state: " + expectedState.name());
                return true;
            }
            Thread.sleep(500);
        }
        System.out.println("timeout waiting for bookie state: " + expectedState.name());
        return false;
    }
}
