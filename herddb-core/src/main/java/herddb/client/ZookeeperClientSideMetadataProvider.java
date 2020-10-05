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

package herddb.client;

import herddb.client.impl.UnreachableServerException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.network.ServerHostData;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Clients side lookup of metadata using ZooKeeper
 *
 * @author enrico.olivelli
 */
public final class ZookeeperClientSideMetadataProvider implements ClientSideMetadataProvider {

    private static final Logger LOG = Logger.getLogger(ZookeeperClientSideMetadataProvider.class.getName());

    private final String basePath;
    private final ZookKeeperHolder zookeeperSupplier;
    private static final int MAX_TRIALS = 20;

    private static class ZookKeeperHolder {

        private final AtomicReference<ZooKeeper> zk = new AtomicReference<>();
        private final String zkAddress;
        private final int zkSessionTimeout;
        private final ReentrantLock makeLock = new ReentrantLock();

        private ZooKeeper makeZooKeeper() {
             try {
                CountDownLatch waitForConnection = new CountDownLatch(1);
                final ZooKeeper z =  new ZooKeeper(zkAddress, zkSessionTimeout, new WatcherImpl(waitForConnection, null), true);
                boolean waitResult = waitForConnection.await(zkSessionTimeout * 2L, TimeUnit.MILLISECONDS);
                if (!waitResult) {
                    LOG.log(Level.SEVERE, "ZK session to ZK " + zkAddress + " did not establish within "
                            + (zkSessionTimeout * 2L) + " ms");
                }
                // re-register the watcher, we want the handle to be recreated in case of "session expired"
                z.register(new WatcherImpl(waitForConnection, z));
                return z;
            } catch (IOException err) {
                LOG.log(Level.SEVERE, "zk client error " + err, err);
            } catch (InterruptedException err) {
                LOG.log(Level.SEVERE, "zk client error " + err, err);
                Thread.currentThread().interrupt();
            }
             return null;

        }
        private ZookKeeperHolder(String zkAddress, int zkSessionTimeout) {
            this.zkAddress = zkAddress;
            this.zkSessionTimeout = zkSessionTimeout;
            makeZooKeeper();
        }

        private ZooKeeper get() throws InterruptedException {
            ZooKeeper current = zk.get();
            if (current != null
                    && current.getState() != ZooKeeper.States.CLOSED) {
                return current;
            }

            makeLock.lockInterruptibly(); // we don't want to race creating ZK handles
            try {
                ZooKeeper newHandle = makeZooKeeper();
                boolean ok = zk.compareAndSet(current, newHandle);
                if (ok) {
                    return newHandle;
                } else {
                    // we failed setting the reference ? this should not be possible
                    newHandle.close();
                    return current;
                }
            } finally {
                makeLock.unlock();
            }
        }

        private void close() {
            try {
                ZooKeeper current = zk.get();
                zk.compareAndSet(current, null);
                if (current != null) {
                    current.close(1000);
                }
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            }
        }

        class WatcherImpl implements Watcher {

            private final CountDownLatch waitForConnection;
            private final ZooKeeper handle;

            public WatcherImpl(CountDownLatch waitForConnection, ZooKeeper handle) {
                this.waitForConnection = waitForConnection;
                this.handle = handle;
            }

            @Override
            public void process(WatchedEvent event) {
                switch (event.getState()) {
                    case SyncConnected:
                    case SaslAuthenticated:
                    case ConnectedReadOnly:
                        LOG.log(Level.FINE, "zk client event {0}", event);
                        waitForConnection.countDown();
                        break;
                    case Expired:
                        LOG.log(Level.INFO, "zk client event {0}", event);
                        ZookKeeperHolder.this.zk.compareAndSet(handle, null);
                        break;
                    default:
                        LOG.log(Level.INFO, "zk client event {0}", event);
                        break;
                }
            }
        }
    }

    public ZookeeperClientSideMetadataProvider(String zkAddress, int zkSessionTimeout, String basePath) {
        this.zookeeperSupplier = new ZookKeeperHolder(zkAddress, zkSessionTimeout);
        this.basePath = basePath;
    }

    private final Map<String, String> tableSpaceLeaders = new ConcurrentHashMap<>();
    private final Map<String, ServerHostData> servers = new ConcurrentHashMap<>();

    @Override
    public void requestMetadataRefresh(Exception error) {
        if (error instanceof UnreachableServerException) {
            UnreachableServerException u = (UnreachableServerException) error;
            servers.remove(u.getNodeId());
            List<String> tablespaces =
                    tableSpaceLeaders
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().equalsIgnoreCase(u.getNodeId()))
                            .map(entry -> entry.getKey())
                            .collect(Collectors.toList());
            tablespaces.forEach(tableSpaceLeaders::remove);
        } else {
            tableSpaceLeaders.clear();
            servers.clear();
        }
    }

    @Override
    public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
        tableSpace = tableSpace.toLowerCase();
        String cached = tableSpaceLeaders.get(tableSpace);
        if (cached != null) {
            return cached;
        }

        for (int i = 0; i < MAX_TRIALS; i++) {
            ZooKeeper zooKeeper = getZooKeeper();
            try {
                try {
                    return readAsTableSpace(zooKeeper, tableSpace);
                } catch (KeeperException.NoNodeException ex) {
                    try {
                        // use the nodeid as tablespace
                        return readAsNode(zooKeeper, tableSpace);
                    } catch (KeeperException.NoNodeException ex2) {
                        return null;
                    }
                }
            } catch (KeeperException.ConnectionLossException ex) {
                LOG.log(Level.SEVERE, "tmp error getTableSpaceLeader for " + tableSpace + ": " + ex);
                try {
                    Thread.sleep(i * 500 + 1000);
                } catch (InterruptedException exit) {
                    throw new ClientSideMetadataProviderException(exit);
                }
            } catch (KeeperException | InterruptedException | IOException ex) {
                throw new ClientSideMetadataProviderException(ex);
            }
        }

        throw new ClientSideMetadataProviderException(
                "Could not find a leader for tablespace " + tableSpace + " in time");
    }

    private String readAsTableSpace(ZooKeeper zooKeeper, String tableSpace) throws IOException, InterruptedException, KeeperException {
        tableSpace = tableSpace.toLowerCase();
        Stat stat = new Stat();
        byte[] result = zooKeeper.getData(basePath + "/tableSpaces/" + tableSpace, false, stat);
        String leader = TableSpace.deserialize(result, stat.getVersion(), stat.getCtime()).leaderId;
        tableSpaceLeaders.put(tableSpace, leader);
        return leader;
    }

    private String readAsNode(ZooKeeper zooKeeper, String tableSpace) throws IOException, InterruptedException, KeeperException {
        tableSpace = tableSpace.toLowerCase();
        Stat stat = new Stat();
        byte[] result = zooKeeper.getData(basePath + "/nodes/" + tableSpace, false, stat);
        NodeMetadata md = NodeMetadata.deserialize(result, stat.getVersion());
        String leader = md.nodeId;
        tableSpaceLeaders.put(tableSpace, leader);
        return leader;
    }

    @Override
    public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
        ServerHostData cached = servers.get(nodeId);
        if (cached != null) {
            return cached;
        }
        for (int i = 0; i < MAX_TRIALS; i++) {
            ZooKeeper zooKeeper = getZooKeeper();

            try {
                Stat stat = new Stat();
                byte[] node = zooKeeper.getData(basePath + "/nodes/" + nodeId, null, stat);
                NodeMetadata nodeMetadata = NodeMetadata.deserialize(node, stat.getVersion());
                ServerHostData result = new ServerHostData(nodeMetadata.host, nodeMetadata.port, "?", nodeMetadata.ssl, new HashMap<>());
                servers.put(nodeId, result);
                return result;
            } catch (KeeperException.NoNodeException ex) {
                return null;
            } catch (KeeperException.ConnectionLossException ex) {
                LOG.log(Level.SEVERE, "tmp error getServerHostData for " + nodeId + ": " + ex);
                try {
                    Thread.sleep(i * 500 + 1000);
                } catch (InterruptedException exit) {
                    throw new ClientSideMetadataProviderException(exit);
                }
            } catch (KeeperException | InterruptedException | IOException ex) {
                throw new ClientSideMetadataProviderException(ex);
            }

        }
        throw new ClientSideMetadataProviderException("Could not find a server info for node " + nodeId + " in time");
    }

    // visible for testing
    protected ZooKeeper getZooKeeper() throws ClientSideMetadataProviderException {
        try {
            ZooKeeper zooKeeper = zookeeperSupplier.get();
            if (zooKeeper == null) {
                throw new ClientSideMetadataProviderException(new Exception("ZooKeeper client is not available"));
            }
            return zooKeeper;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ClientSideMetadataProviderException(ex);
        }
    }

    @Override
    public void close() {
        zookeeperSupplier.close();
    }


}
