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

import herddb.log.LogNotAvailableException;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
    private final String nodesPath;
    private final CountDownLatch firstConnectionLatch = new CountDownLatch(1);
    private final Watcher mainWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            switch (event.getState()) {
                case SyncConnected:
                    firstConnectionLatch.countDown();
                    break;
            }
            notifyMetadataChanged();
        }
    };

    public ZookeeperMetadataStorageManager(String zkAddress, int zkSessionTimeout, String basePath) {
        this.zkAddress = zkAddress;
        this.zkSessionTimeout = zkSessionTimeout;
        this.basePath = basePath;
        this.ledgersPath = basePath + "/ledgers"; // ledgers/TABLESPACEUUID
        this.tableSpacesPath = basePath + "/tableSpaces";  // tableSpaces/TABLESPACENAME
        this.nodesPath = basePath + "/nodes";
    }

    @Override
    public void start() throws MetadataStorageManagerException {
        LOGGER.log(Level.SEVERE, "start, zkAddress " + zkAddress + ", zkSessionTimeout:" + zkSessionTimeout + ", basePath:" + basePath);
        try {
            this.zooKeeper = new ZooKeeper(zkAddress, zkSessionTimeout, mainWatcher);
            firstConnectionLatch.await(zkSessionTimeout, TimeUnit.SECONDS); // TODO: use another timeout?
            ensureRoot();
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    private void ensureRoot() throws KeeperException, InterruptedException {
        try {
            this.zooKeeper.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {            
        }
        try {
            this.zooKeeper.create(tableSpacesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {            
        }
        try {
            this.zooKeeper.create(ledgersPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {            
        }
        try {
            this.zooKeeper.create(nodesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {            
        }

    }

    public ZooKeeper getZooKeeper() {
        return this.zooKeeper;
    }

    /**
     * Let (embedded) brokers read actual list of ledgers used. in order to
     * perform extrernal clean ups
     *
     * @param zk
     * @param ledgersPath
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
        return readActualLedgersListFromZookeeper(zooKeeper, ledgersPath, tableSpaceUUID);
    }

    public void saveActualLedgersList(String tableSpaceUUID, LedgersInfo info) throws LogNotAvailableException {
        byte[] actualLedgers = info.serialize();
        try {
            while (true) {
                try {
                    try {                        
                        Stat newStat = zooKeeper.setData(ledgersPath + "/" + tableSpaceUUID, actualLedgers, info.getZkVersion());
                        info.setZkVersion(newStat.getVersion());
                        LOGGER.log(Level.SEVERE, "save new ledgers list " + info+" to "+ledgersPath + "/" + tableSpaceUUID);
                        return;
                    } catch (KeeperException.NoNodeException firstboot) {
                        zooKeeper.create(ledgersPath + "/" + tableSpaceUUID, actualLedgers, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.BadVersionException fenced) {
                        throw new LogNotAvailableException(new Exception("ledgers actual list was fenced, expecting version " + info.getZkVersion() + " " + fenced, fenced).fillInStackTrace());
                    }
                } catch (KeeperException.ConnectionLossException anyError) {
                    LOGGER.log(Level.SEVERE, "temporary error", anyError);
                    Thread.sleep(10000);
                } catch (Exception anyError) {
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

    private TableSpaceList listTablesSpaces() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        List<String> children = zooKeeper.getChildren(tableSpacesPath, mainWatcher, stat);
        return new TableSpaceList(stat.getVersion(), children);
    }

    private void createTableSpaceNode(TableSpace tableSpace) throws KeeperException, InterruptedException, IOException, TableSpaceAlreadyExistsException {
        try {
            zooKeeper.create(tableSpacesPath +"/"+ tableSpace.name, tableSpace.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException err) {
            throw new TableSpaceAlreadyExistsException(tableSpace.uuid);
        }
    }

    private boolean updateTableSpaceNode(TableSpace tableSpace, int metadataStorageVersion) throws KeeperException, InterruptedException, IOException, TableSpaceDoesNotExistException {
        try {
            zooKeeper.setData(tableSpacesPath+"/" + tableSpace.name, tableSpace.serialize(), metadataStorageVersion);
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
            zooKeeper.delete(tableSpacesPath+"/" + tableSpaceName, metadataStorageVersion);
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
                TableSpace tableSpace = TableSpace.builder().leader(localNodeId).replica(localNodeId).name(TableSpace.DEFAULT).build();
                createTableSpaceNode(tableSpace);
            }
        } catch (TableSpaceAlreadyExistsException err) {
            // not a problem
        } catch (InterruptedException | KeeperException | IOException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws MetadataStorageManagerException {
        if (this.zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException err) {
            }
        }
    }

    @Override
    public Collection<String> listTableSpaces() throws MetadataStorageManagerException {
        try {
            return listTablesSpaces().tableSpaces;
        } catch (KeeperException | InterruptedException ex) {
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public TableSpace describeTableSpace(String name) throws MetadataStorageManagerException {
        try {
            Stat stat = new Stat();
            byte[] result = zooKeeper.getData(tableSpacesPath+"/" + name, mainWatcher, stat);
            return TableSpace.deserialize(result, stat.getVersion());
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {

        try {
            createTableSpaceNode(tableSpace);
            notifyMetadataChanged();
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            boolean result = updateTableSpaceNode(tableSpace, (Integer) previous.metadataStorageVersion);
            if (result) {
                notifyMetadataChanged();
            }
            return result;
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            boolean result = deleteTableSpaceNode(name, (Integer) previous.metadataStorageVersion);
            if (result) {
                notifyMetadataChanged();
            }
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public List<NodeMetadata> listNodes() throws MetadataStorageManagerException {
        try {
            List<String> children = zooKeeper.getChildren(nodesPath, mainWatcher, null);
            List<NodeMetadata> result = new ArrayList<>();
            for (String child : children) {
                NodeMetadata nodeMetadata = getNode(child);
                result.add(nodeMetadata);
            }
            return result;
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new MetadataStorageManagerException(err);
        }

    }

    @Override
    public void clear() throws MetadataStorageManagerException {
        try {
            List<String> children = zooKeeper.getChildren(nodesPath, false);
            for (String child : children) {
                zooKeeper.delete(nodesPath + "/" + child, -1);
            }
            children = zooKeeper.getChildren(tableSpacesPath, false);
            for (String child : children) {
                zooKeeper.delete(tableSpacesPath + "/" + child, -1);
            }
            children = zooKeeper.getChildren(ledgersPath, false);
            for (String child : children) {
                zooKeeper.delete(ledgersPath + "/" + child, -1);
            }
        } catch (InterruptedException | KeeperException error) {
            LOGGER.log(Level.SEVERE, "Cannot clear metadata", error);
            throw new MetadataStorageManagerException(error);
        }

    }

    private NodeMetadata getNode(String nodeId) throws KeeperException, IOException, InterruptedException {
        Stat stat = new Stat();
        byte[] node = zooKeeper.getData(nodesPath + "/" + nodeId, mainWatcher, stat);
        NodeMetadata nodeMetadata = NodeMetadata.deserialize(node, stat.getVersion());
        return nodeMetadata;
    }

    @Override
    public void registerNode(NodeMetadata nodeMetadata) throws MetadataStorageManagerException {
        try {
            byte[] data = nodeMetadata.serialize();
            try {
                zooKeeper.create(nodesPath + "/" + nodeMetadata.nodeId, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ok) {
                zooKeeper.setData(nodesPath + "/" + nodeMetadata.nodeId, data, -1);
            }
            notifyMetadataChanged();
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new MetadataStorageManagerException(err);
        }

    }

}
