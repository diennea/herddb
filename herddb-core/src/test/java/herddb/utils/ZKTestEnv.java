/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class ZKTestEnv implements AutoCloseable {

    static {
        System.setProperty("zookeeper.admin.enableServer", "false");
        System.setProperty("zookeeper.forceSync", "no");
    }

    TestingServer zkServer;
    List<BookieServer> bookies = new ArrayList<>();
    Path path;

    public ZKTestEnv(Path path) throws Exception {
        this.path = path;
        restartZkServer();

        try (ZooKeeperClient zkc = ZooKeeperClient
                .newBuilder()
                .connectString("localhost:1282")
                .sessionTimeoutMs(10000)
                .build()) {

            boolean rootExists = zkc.exists(getPath(), false) != null;

            if (!rootExists) {
                zkc.create(getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    public void stopZkServer() throws IOException {
        if (zkServer != null) {
            zkServer.close();
            zkServer = null;
        }
    }

    public final void restartZkServer() throws Exception {
        stopZkServer();
        zkServer = new TestingServer(1282, path.toFile(), true);
    }

    public void startBookies() throws Exception {
        startBookies(true);
    }

    public void startNewBookie() throws Exception {
        startBookie(true, true);
    }
     
    public void startBookies(boolean format) throws Exception {
        startBookie(format, false);
    }
    private void startBookie(boolean format, boolean newBookie) throws Exception {
        if (!newBookie && !bookies.isEmpty()) {
            throw new Exception("bookie already started");
        }
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(PortManager.nextFreePort());
        conf.setUseHostNameAsBookieID(true);

        Path targetDir = path.resolve("bookie_data_"+bookies.size());
        conf.setZkServers("localhost:1282");
        conf.setZkLedgersRootPath(herddb.server.ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
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

        if (format) {
            BookKeeperAdmin.initNewCluster(conf);
            BookKeeperAdmin.format(conf, false, true);
        }
  
        BookieServer bookie = new BookieServer(conf);
        bookies.add(bookie);
        bookie.start();
    }

    public void stopBookies() throws Exception {
        for (BookieServer bookie : bookies) {
            bookie.shutdown();
            bookie.join();
        }
    }

    public String getAddress() {
        return "localhost:1282";
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
            stopBookies();
        } catch (Throwable t) {
        }
        stopZkServer();
    }

}
