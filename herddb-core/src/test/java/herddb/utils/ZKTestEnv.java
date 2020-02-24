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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
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
    private int nextBookiePort = 5621;


    public ZKTestEnv(Path path) throws Exception {
        zkServer = new TestingServer(1282, path.toFile(), true);
        this.path = path;

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
    }

    public void startBookieAndInitCluster() throws Exception {
        startBookie(true);
    }
    
    public void startNewBookie() throws Exception {
        startBookie(false);
    }
    
    private void startBookie(boolean format) throws Exception {
        if (format && !bookies.isEmpty()) {
            throw new Exception("cannot format, you aleady have bookies");
        }
        ServerConfiguration conf = createBookieConf(nextBookiePort++);

        if (format) {
            BookKeeperAdmin.initNewCluster(conf);
            BookKeeperAdmin.format(conf, false, true);
        }

        BookieServer bookie = new BookieServer(conf);
        bookies.add(bookie);
        bookie.start();
    }

    private ServerConfiguration createBookieConf(int port) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(port++);
        System.out.println("STARTING BOOKIE at port " + port);
        conf.setUseHostNameAsBookieID(true);
        // no need to preallocate journal and entrylog in tests
        conf.setEntryLogFilePreAllocationEnabled(false);
        conf.setProperty("journalPreAllocSizeMB", 1);
        Path targetDir = path.resolve("bookie_data_"+conf.getBookiePort());
        conf.setMetadataServiceUri("zk+null://" + zkServer.getConnectString() + herddb.server.ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
        conf.setLedgerDirNames(new String[]{targetDir.toAbsolutePath().toString()});
        conf.setJournalDirName(targetDir.toAbsolutePath().toString());
        conf.setFlushInterval(10000);
        conf.setGcWaitTime(5);
        conf.setJournalFlushWhenQueueEmpty(true);
        //        conf.setJournalBufferedEntriesThreshold(1);
        conf.setAutoRecoveryDaemonEnabled(false);
        // no need for real network in tests
        conf.setEnableLocalTransport(true);
        conf.setDisableServerSocketBind(true);
        // no need to fsync in tests
        conf.setJournalSyncData(false);
        conf.setAllowLoopback(true);
        conf.setProperty("journalMaxGroupWaitMSec", 10); // default 200ms
        return conf;
    }

    public void startStoppedBookie(String addr) throws Exception {
        int index = 0;
        for (BookieServer bookie : bookies) {
            if (bookie.getLocalAddress().getSocketAddress().toString().equals(addr)) {
                if (bookie.isRunning()) {
                    throw new Exception("you did not stop bookie "+addr);
                }
                ServerConfiguration conf = createBookieConf(bookie.getLocalAddress().getPort());
                BookieServer newBookie = new BookieServer(conf);
                bookies.set(index, newBookie);
                newBookie.start();
                return;
            }
            index++;
        }
        throw new Exception("Cannot find bookie "+addr);
    }

    public void pauseBookie() throws Exception {
        bookies.get(0).suspendProcessing();
    }

    public void pauseBookie(String addr) throws Exception {
        for (BookieServer bookie : bookies) {
            if (bookie.getLocalAddress().getSocketAddress().toString().equals(addr)) {
                bookie.suspendProcessing();
                return;
            }
        }
        throw new Exception("Cannot find bookie "+addr);
    }

    public void resumeBookie() throws Exception {
        bookies.get(0).resumeProcessing();
    }

    public void resumeBookie(String addr) throws Exception {
        for (BookieServer bookie : bookies) {
            if (bookie.getLocalAddress().getSocketAddress().toString().equals(addr)) {
                bookie.resumeProcessing();
                return;
            }
        }
        throw new Exception("Cannot find bookie "+addr);
    }

    public String stopBookie() throws Exception {
        String addr = bookies.get(0).getLocalAddress().getSocketAddress().toString();
        stopBookie(addr);
        return addr;
    }
    
    public void stopBookie(String addr) throws Exception {
        for (BookieServer bookie : bookies) {
            if (bookie.getLocalAddress().getSocketAddress().toString().equals(addr)) {
                bookie.shutdown();
                bookie.join();
                return;
            }
        }
        throw new Exception("Cannot find bookie "+addr);
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
        for (BookieServer bookie : bookies) {
            try {
                bookie.shutdown();
            } catch (Throwable t) {
            }
        }
        bookies.clear();

        try {
            if (zkServer != null) {
                zkServer.close();
                zkServer = null;
            }
        } catch (Throwable t) {
        }
    }

}
