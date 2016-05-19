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

import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.network.ServerHostData;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
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
public class ZookeeperClientSideMetadataProvider implements ClientSideMetadataProvider {

    private final String basePath;
    private final Supplier<ZooKeeper> zookeeperSupplier;
    private boolean ownZooKeeper;

    public ZookeeperClientSideMetadataProvider(String basePath, Supplier<ZooKeeper> zookeeper) {
        this.basePath = basePath;
        this.zookeeperSupplier = zookeeper;
        this.ownZooKeeper = false;
    }

    public ZookeeperClientSideMetadataProvider(String zkAddress, int zkSessionTimeout, String basePath) {
        this(basePath, () -> {
            try {
                return new ZooKeeper(zkAddress, zkSessionTimeout, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                    }
                });
            } catch (Exception err) {
                return null;
            }
        });
        this.ownZooKeeper = true;
    }

    private final Map<String, String> tableSpaceLeaders = new ConcurrentHashMap<>();
    private final Map<String, ServerHostData> servers = new ConcurrentHashMap<>();

    @Override
    public void requestMetadataRefresh() {
        tableSpaceLeaders.clear();
        servers.clear();
    }

    @Override
    public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
        String cached = tableSpaceLeaders.get(tableSpace);
        if (cached != null) {
            return cached;
        }
        ZooKeeper zooKeeper = getZooKeeper();
        try {
            Stat stat = new Stat();
            byte[] result = zooKeeper.getData(basePath + "/tableSpaces/" + tableSpace, false, stat);
            String leader = TableSpace.deserialize(result, stat.getVersion()).leaderId;
            tableSpaceLeaders.put(tableSpace, leader);
            return leader;
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new ClientSideMetadataProviderException(ex);
        } finally {
            if (ownZooKeeper) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException ex) {
                    throw new ClientSideMetadataProviderException(ex);
                }
            }
        }
    }

    @Override
    public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
        ServerHostData cached = servers.get(nodeId);
        if (cached != null) {
            return cached;
        }
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
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new ClientSideMetadataProviderException(ex);
        } finally {
            if (ownZooKeeper) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException ex) {
                    throw new ClientSideMetadataProviderException(ex);
                }
            }
        }
    }

    private ZooKeeper getZooKeeper() throws ClientSideMetadataProviderException {
        ZooKeeper zooKeeper = zookeeperSupplier.get();
        if (zooKeeper == null) {
            throw new ClientSideMetadataProviderException(new Exception("ZooKeeper client is not available"));
        }
        return zooKeeper;
    }

}
