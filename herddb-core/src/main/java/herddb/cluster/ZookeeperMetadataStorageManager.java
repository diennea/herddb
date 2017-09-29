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
package herddb.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.log.LogNotAvailableException;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Metadata storage manager over Zookeeper
 *
 * @author enrico.olivelli
 */
public class ZookeeperMetadataStorageManager extends MetadataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperMetadataStorageManager.class.getName());

    private ZooKeeper zooKeeper;
    private final String zkAddress;
    private final int zkSessionTimeout;
    private final String basePath;
    private final String ledgersPath;
    private final String tableSpacesPath;
    private final String tableSpacesReplicasPath;
    private final String nodesPath;
    private volatile boolean started;

    private final Watcher mainWatcher = (WatchedEvent event) -> {
        switch (event.getState()) {
            case SyncConnected:
            case SaslAuthenticated:
                notifyMetadataChanged();
                break;
            default:
                // ignore
                break;

        }
    };

    public String getZkAddress() {
        return zkAddress;
    }

    public int getZkSessionTimeout() {
        return zkSessionTimeout;
    }

    private synchronized void restartZooKeeper() throws IOException, InterruptedException {
        ZooKeeper old = zooKeeper;
        if (old != null) {
            old.close();
        }
        CountDownLatch firstConnectionLatch = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(zkAddress, zkSessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getState()) {
                    case SyncConnected:
                    case SaslAuthenticated:
                        firstConnectionLatch.countDown();
                        notifyMetadataChanged();
                        break;
                    default:
                        // ignore
                        break;
                }
            }
        });
        if (!firstConnectionLatch.await(zkSessionTimeout, TimeUnit.SECONDS)) {
            zk.close();
            throw new IOException("Could not connect to zookeeper at " + zkAddress + " within " + zkSessionTimeout + " ms");
        }
        this.zooKeeper = zk;
        LOGGER.info("Connected to ZK " + zk);
    }

    private void handleSessionExpiredError(Throwable error) {
        if (!(error instanceof KeeperException.SessionExpiredException)) {
            return;
        }
        try {
            restartZooKeeper();
        } catch (IOException | InterruptedException err) {
            LOGGER.log(Level.SEVERE, "Error handling session expired", err);
        }
    }

    public ZookeeperMetadataStorageManager(String zkAddress, int zkSessionTimeout, String basePath) {
        this.zkAddress = zkAddress;
        this.zkSessionTimeout = zkSessionTimeout;
        this.basePath = basePath;
        this.ledgersPath = basePath + "/ledgers"; // ledgers/TABLESPACEUUID
        this.tableSpacesPath = basePath + "/tableSpaces";  // tableSpaces/TABLESPACENAME
        this.tableSpacesReplicasPath = basePath + "/replicas";  // replicas/TABLESPACEUUID
        this.nodesPath = basePath + "/nodes";
    }

    @Override
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
    public synchronized void start() throws MetadataStorageManagerException {
        if (started) {
            return;
        }
        LOGGER.log(Level.SEVERE, "start, zkAddress " + zkAddress + ", zkSessionTimeout:" + zkSessionTimeout + ", basePath:" + basePath);
        try {
            restartZooKeeper();
            ensureRoot();
            started = true;
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    private void ensureRoot() throws KeeperException, InterruptedException, IOException {
        try {
            zooKeeper.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(tableSpacesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(tableSpacesReplicasPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(ledgersPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(nodesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }

    }

    public synchronized ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    private synchronized ZooKeeper ensureZooKeeper() throws KeeperException, InterruptedException, IOException {
        if (!started) {
            throw new IOException("MetadataStorageManager not yet started");
        }
        if (zooKeeper == null) {
            restartZooKeeper();
        }
        return this.zooKeeper;
    }

    /**
     * Let (embedded) brokers read actual list of ledgers used. in order to perform extrernal clean ups
     *
     * @param zooKeeper
     * @param ledgersPath
     * @param tableSpaceUUID
     * @return
     * @throws LogNotAvailableException
     */
    public static LedgersInfo readActualLedgersListFromZookeeper(ZooKeeper zooKeeper, String ledgersPath, String tableSpaceUUID) throws LogNotAvailableException {
        while (zooKeeper.getState() != ZooKeeper.States.CLOSED) {
            try {
                Stat stat = new Stat();
                byte[] actualLedgers = zooKeeper.getData(ledgersPath + "/" + tableSpaceUUID, false, stat);
                return LedgersInfo.deserialize(actualLedgers, stat.getVersion());
            } catch (KeeperException.NoNodeException firstboot) {
                LOGGER.log(Level.SEVERE, "node " + ledgersPath + "/" + tableSpaceUUID + " not found");
                return LedgersInfo.deserialize(null, -1); // -1 is a special ZK version
            } catch (KeeperException.ConnectionLossException error) {
                LOGGER.log(Level.SEVERE, "error while loading actual ledgers list at " + ledgersPath + "/" + tableSpaceUUID, error);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException err) {
                    // maybe stopping
                    throw new LogNotAvailableException(err);
                }
            } catch (Exception error) {
                LOGGER.log(Level.SEVERE, "error while loading actual ledgers list at " + ledgersPath + "/" + tableSpaceUUID, error);
                throw new LogNotAvailableException(error);
            }
        }
        throw new LogNotAvailableException(new Exception("zk client closed"));
    }

    public LedgersInfo getActualLedgersList(String tableSpaceUUID) throws LogNotAvailableException {
        try {
            return readActualLedgersListFromZookeeper(ensureZooKeeper(), ledgersPath, tableSpaceUUID);
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public void saveActualLedgersList(String tableSpaceUUID, LedgersInfo info) throws LogNotAvailableException {
        byte[] actualLedgers = info.serialize();
        try {
            while (true) {
                try {
                    try {
                        Stat newStat = ensureZooKeeper().setData(ledgersPath + "/" + tableSpaceUUID, actualLedgers, info.getZkVersion());
                        info.setZkVersion(newStat.getVersion());
                        LOGGER.log(Level.SEVERE, "save new ledgers list " + info + " to " + ledgersPath + "/" + tableSpaceUUID);
                        return;
                    } catch (KeeperException.NoNodeException firstboot) {
                        ensureZooKeeper().create(ledgersPath + "/" + tableSpaceUUID, actualLedgers, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.BadVersionException fenced) {
                        throw new LogNotAvailableException(new Exception("ledgers actual list was fenced, expecting version " + info.getZkVersion() + " " + fenced, fenced).fillInStackTrace());
                    }
                } catch (KeeperException.ConnectionLossException anyError) {
                    LOGGER.log(Level.SEVERE, "temporary error", anyError);
                    Thread.sleep(10000);
                } catch (Exception anyError) {
                    handleSessionExpiredError(anyError);
                    throw new LogNotAvailableException(anyError);
                }
            }
        } catch (InterruptedException stop) {
            LOGGER.log(Level.SEVERE, "fatal error", stop);
            throw new LogNotAvailableException(stop);

        }
    }

    private static class TableSpaceList {

        private final int version;
        private final List<String> tableSpaces;

        public TableSpaceList(int version, List<String> tableSpaces) {
            this.version = version;
            this.tableSpaces = tableSpaces;
        }

    }

    private TableSpaceList listTablesSpaces() throws KeeperException, InterruptedException, IOException {
        Stat stat = new Stat();
        List<String> children = ensureZooKeeper().getChildren(tableSpacesPath, mainWatcher, stat);
        return new TableSpaceList(stat.getVersion(), children);
    }

    private void createTableSpaceNode(TableSpace tableSpace) throws KeeperException, InterruptedException, IOException, TableSpaceAlreadyExistsException {
        try {
            ensureZooKeeper().create(tableSpacesPath + "/" + tableSpace.name, tableSpace.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException err) {
            throw new TableSpaceAlreadyExistsException(tableSpace.uuid);
        }
    }

    private boolean updateTableSpaceNode(TableSpace tableSpace, int metadataStorageVersion) throws KeeperException, InterruptedException, IOException, TableSpaceDoesNotExistException {
        try {
            ensureZooKeeper().setData(tableSpacesPath + "/" + tableSpace.name, tableSpace.serialize(), metadataStorageVersion);
            notifyMetadataChanged();
            return true;
        } catch (KeeperException.BadVersionException changed) {
            return false;
        } catch (KeeperException.NoNodeException changed) {
            throw new TableSpaceDoesNotExistException(tableSpace.uuid);
        }
    }

    private boolean deleteTableSpaceNode(String tableSpaceName, int metadataStorageVersion) throws KeeperException, InterruptedException, IOException, TableSpaceDoesNotExistException {
        try {
            ensureZooKeeper().delete(tableSpacesPath + "/" + tableSpaceName, metadataStorageVersion);
            notifyMetadataChanged();
            return true;
        } catch (KeeperException.BadVersionException changed) {
            return false;
        } catch (KeeperException.NoNodeException changed) {
            throw new TableSpaceDoesNotExistException(tableSpaceName);
        }
    }

    @Override
    public void ensureDefaultTableSpace(String localNodeId) throws MetadataStorageManagerException {
        try {
            TableSpaceList list = listTablesSpaces();
            if (!list.tableSpaces.contains(TableSpace.DEFAULT)) {
                TableSpace tableSpace = TableSpace.builder()
                    .leader(localNodeId)
                    .replica(localNodeId)
                    .expectedReplicaCount(1)
                    .maxLeaderInactivityTime(0)
                    .name(TableSpace.DEFAULT)
                    .build();
                createTableSpaceNode(tableSpace);
            }
        } catch (TableSpaceAlreadyExistsException err) {
            // not a problem
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws MetadataStorageManagerException {
        try {
            synchronized (this) {
                if (zooKeeper == null) {
                    return;
                }
            }
            ZooKeeper zk = ensureZooKeeper();
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException err) {
                }
            }
        } catch (IOException | InterruptedException | KeeperException err) {

        }
    }

    @Override
    public Collection<String> listTableSpaces() throws MetadataStorageManagerException {
        try {
            return listTablesSpaces().tableSpaces;
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public TableSpace describeTableSpace(String name) throws MetadataStorageManagerException {
        try {
            Stat stat = new Stat();
            byte[] result = ensureZooKeeper().getData(tableSpacesPath + "/" + name, mainWatcher, stat);
            return TableSpace.deserialize(result, stat.getVersion());
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {

        try {
            createTableSpaceNode(tableSpace);
            notifyMetadataChanged();
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous == null || previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            boolean result = updateTableSpaceNode(tableSpace, (Integer) previous.metadataStorageVersion);
            if (result) {
                notifyMetadataChanged();
            }
            return result;
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous == null || previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            boolean result = deleteTableSpaceNode(name, (Integer) previous.metadataStorageVersion);
            if (result) {
                notifyMetadataChanged();
            }
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public List<NodeMetadata> listNodes() throws MetadataStorageManagerException {
        try {
            List<String> children = ensureZooKeeper().getChildren(nodesPath, mainWatcher, null);
            LOGGER.severe("listNodes: for " + nodesPath + ": " + children);
            List<NodeMetadata> result = new ArrayList<>();
            for (String child : children) {
                NodeMetadata nodeMetadata = getNode(child);
                result.add(nodeMetadata);
            }
            return result;
        } catch (IOException | InterruptedException | KeeperException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }

    }

    @Override
    public void clear() throws MetadataStorageManagerException {
        try {
            List<String> children = ensureZooKeeper().getChildren(nodesPath, false);
            for (String child : children) {
                ensureZooKeeper().delete(nodesPath + "/" + child, -1);
            }
            children = ensureZooKeeper().getChildren(tableSpacesPath, false);
            for (String child : children) {
                ensureZooKeeper().delete(tableSpacesPath + "/" + child, -1);
            }
            children = ensureZooKeeper().getChildren(ledgersPath, false);
            for (String child : children) {
                ensureZooKeeper().delete(ledgersPath + "/" + child, -1);
            }
        } catch (InterruptedException | KeeperException | IOException error) {
            LOGGER.log(Level.SEVERE, "Cannot clear metadata", error);
            throw new MetadataStorageManagerException(error);
        }

    }

    private NodeMetadata getNode(String nodeId) throws KeeperException, IOException, InterruptedException {
        Stat stat = new Stat();
        byte[] node = ensureZooKeeper().getData(nodesPath + "/" + nodeId, mainWatcher, stat);
        NodeMetadata nodeMetadata = NodeMetadata.deserialize(node, stat.getVersion());
        return nodeMetadata;
    }

    @Override
    public void registerNode(NodeMetadata nodeMetadata) throws MetadataStorageManagerException {
        try {
            String path = nodesPath + "/" + nodeMetadata.nodeId;
            LOGGER.severe("registerNode at " + path + " -> " + nodeMetadata);
            byte[] data = nodeMetadata.serialize();
            try {
                ensureZooKeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ok) {
                LOGGER.severe("registerNode at " + path + " " + ok);
                ensureZooKeeper().setData(path, data, -1);
            }
            notifyMetadataChanged();
        } catch (IOException | InterruptedException | KeeperException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }

    }

    @Override
    public List<TableSpaceReplicaState> getTableSpaceReplicaState(String tableSpaceUuid) throws MetadataStorageManagerException {
        try {
            List<String> children;
            try {
                children = ensureZooKeeper().getChildren(tableSpacesReplicasPath + "/" + tableSpaceUuid, false);
            } catch (KeeperException.NoNodeException err) {
                return Collections.emptyList();
            }
            List<TableSpaceReplicaState> result = new ArrayList<>();
            for (String child : children) {
                String path = tableSpacesReplicasPath + "/" + tableSpaceUuid + "/" + child;
                try {
                    byte[] data = ensureZooKeeper().getData(path, false, null);
                    TableSpaceReplicaState nodeMetadata = TableSpaceReplicaState.deserialize(data);
                    result.add(nodeMetadata);
                } catch (IOException deserializeError) {
                    LOGGER.log(Level.SEVERE, "error reading " + path, deserializeError);
                }
            }
            return result;
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void updateTableSpaceReplicaState(TableSpaceReplicaState state) throws MetadataStorageManagerException {
        try {
            String tableSpacePath = tableSpacesReplicasPath + "/" + state.uuid;
            byte[] data = state.serialize();
            try {
                ensureZooKeeper().setData(tableSpacePath + "/" + state.nodeId, data, -1);
            } catch (KeeperException.NoNodeException notExists) {
                try {
                    ensureZooKeeper().create(tableSpacePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException existsRoot) {
                }
                ensureZooKeeper().create(tableSpacePath + "/" + state.nodeId, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

}
