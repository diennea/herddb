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
import herddb.model.TableSpace;
import java.io.IOException;
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
    private final CountDownLatch firstConnectionLatch = new CountDownLatch(1);
    private final Watcher mainWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            switch (event.getState()) {
                case SyncConnected:
                    firstConnectionLatch.countDown();
                    break;
            }
        }
    };

    public ZookeeperMetadataStorageManager(String zkAddress, int zkSessionTimeout, String basePath) {
        this.zkAddress = zkAddress;
        this.zkSessionTimeout = zkSessionTimeout;
        this.basePath = basePath;
        this.ledgersPath = basePath + "/ledgers";
        this.tableSpacesPath = basePath + "/tableSpaces";
    }

    @Override
    public void start() throws MetadataStorageManagerException {
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
    public static LedgersInfo readActualLedgersListFromZookeeper(ZooKeeper zooKeeper, String ledgersPath) throws LogNotAvailableException {
        while (zooKeeper.getState() != ZooKeeper.States.CLOSED) {
            try {
                Stat stat = new Stat();
                byte[] actualLedgers = zooKeeper.getData(ledgersPath, false, stat);
                return LedgersInfo.deserialize(actualLedgers, stat.getVersion());
            } catch (KeeperException.NoNodeException firstboot) {
                LOGGER.log(Level.SEVERE, "node " + ledgersPath + " not found");
                return LedgersInfo.deserialize(null, -1); // -1 is a special ZK version
            } catch (KeeperException.ConnectionLossException error) {
                LOGGER.log(Level.SEVERE, "error while loading actual ledgers list at " + ledgersPath, error);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException err) {
                    // maybe stopping
                    throw new LogNotAvailableException(err);
                }
            } catch (Exception error) {
                LOGGER.log(Level.SEVERE, "error while loading actual ledgers list at " + ledgersPath, error);
                throw new LogNotAvailableException(error);
            }
        }
        throw new LogNotAvailableException(new Exception("zk client closed"));
    }

    public LedgersInfo getActualLedgersList(String tableSpace) throws LogNotAvailableException {
        return readActualLedgersListFromZookeeper(zooKeeper, ledgersPath + "/" + tableSpace);
    }

    public void saveActualLedgersList(String tableSpace, LedgersInfo info) throws LogNotAvailableException {
        byte[] actualLedgers = info.serialize();
        try {
            while (true) {
                try {
                    try {
                        Stat newStat = zooKeeper.setData(ledgersPath + "/" + tableSpace, actualLedgers, info.getZkVersion());
                        info.setZkVersion(newStat.getVersion());
                        LOGGER.log(Level.SEVERE, "save new ledgers list " + info);
                        return;
                    } catch (KeeperException.NoNodeException firstboot) {
                        zooKeeper.create(ledgersPath + "/" + tableSpace, actualLedgers, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        List<String> children = zooKeeper.getChildren(tableSpacesPath, false, stat);
        return new TableSpaceList(stat.getVersion(), children);
    }

    private void createTableSpaceNode(TableSpace tableSpace) throws KeeperException, InterruptedException, IOException {
        zooKeeper.create(basePath + "/tableSpaces/" + tableSpace.name, tableSpace.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void updateTableSpaceNode(TableSpace tableSpace, int metadataStorageVersion) throws KeeperException, InterruptedException, IOException {
        zooKeeper.setData(basePath + "/tableSpaces/" + tableSpace.name, tableSpace.serialize(), metadataStorageVersion);
    }

    @Override
    public void ensureDefaultTableSpace(String localNodeId) throws MetadataStorageManagerException {
        try {
            TableSpaceList list = listTablesSpaces();
            if (!list.tableSpaces.contains(TableSpace.DEFAULT)) {
                TableSpace tableSpace = TableSpace.builder().leader(localNodeId).replica(localNodeId).name(TableSpace.DEFAULT).build();
                createTableSpaceNode(tableSpace);
            }
        } catch (KeeperException.NodeExistsException err) {
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
            byte[] result = zooKeeper.getData(basePath + "/tableSpaces/" + name, false, stat);
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
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            updateTableSpaceNode(tableSpace, (Integer) previous.metadataStorageVersion);
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new MetadataStorageManagerException(ex);
        }

    }

}
