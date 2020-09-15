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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.core.HerdDBInternalException;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.file.FileRecordSetFactory;
import herddb.index.KeyToPageIndex;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.server.ServerConfiguration;
import herddb.storage.DataPageDoesNotExistException;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.ByteArrayCursor;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.SystemInstrumentation;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import io.netty.util.Recycler;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * Data Storage on BookKeeper.
 *
 * Beware that this kind of storage is useful when you have most of the DB in
 * memory. Random access to BK is generally slower than when using local disk,
 * and we also have to deal with lots of metadata to be stored on Zookeeper.
 *
 * @author enrico.olivelli
 */
public class BookKeeperDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(BookKeeperDataStorageManager.class.getName());
    private static final byte[] EMPTY_ARRAY = new byte[0];

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Path tmpDirectory;
    private final int swapThreshold;
    private final Counter zkWrites;
    private final Counter zkReads;
    private final Counter zkGetChildren;
    private final OpStatsLogger dataPageReads;
    private final OpStatsLogger dataPageWrites;
    private final OpStatsLogger indexPageReads;
    private final OpStatsLogger indexPageWrites;
    private final ZookeeperMetadataStorageManager zk;
    private final BookkeeperCommitLogManager bk;
    private final String nodeId;

    private final ConcurrentHashMap<String, TableSpacePagesMapping> tableSpaceMappings = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> tableSpaceExpectedReplicaCount = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> tableSpaceZkNodeVersion = new ConcurrentHashMap<>();


    private final String rootZkNode;

    public static final String FILEEXTENSION_PAGE = ".page";

    private static final class PagesMapping {

        ConcurrentHashMap<Long, Long> pages = new ConcurrentHashMap<>();
        ConcurrentSkipListSet<Long> oldLedgers = new ConcurrentSkipListSet<>();

        private Long getLedgerIdForPage(Long pageId) {
            return pages.get(pageId);
        }

        private void removePageId(long pageId) {
            pages.remove(pageId);
        }

        private void writePageId(long pageId, long ledgerId) {
            Long oldLedger = pages.put(pageId, ledgerId);
            if (oldLedger != null) {
                oldLedgers.add(oldLedger);
            }
        }

        public ConcurrentHashMap<Long, Long> getPages() {
            return pages;
        }

        public void setPages(ConcurrentHashMap<Long, Long> pages) {
            this.pages = pages;
        }

        public ConcurrentSkipListSet<Long> getOldLedgers() {
            return oldLedgers;
        }

        public void setOldLedgers(ConcurrentSkipListSet<Long> oldLedgers) {
            this.oldLedgers = oldLedgers;
        }

        @Override
        public String toString() {
            return "PagesMapping{" + "pages=" + pages + ", oldLedgers=" + oldLedgers + '}';
        }

    }

    public static final class TableSpacePagesMapping {

        private ConcurrentHashMap<String, PagesMapping> tables = new ConcurrentHashMap<>();
        private ConcurrentHashMap<String, PagesMapping> indexes = new ConcurrentHashMap<>();

        private PagesMapping getTablePagesMapping(String tableName) {
            return tables.computeIfAbsent(tableName, t -> new PagesMapping());
        }

        private PagesMapping getIndexPagesMapping(String indexName) {
            return indexes.computeIfAbsent(indexName, t -> new PagesMapping());
        }

        public ConcurrentHashMap<String, PagesMapping> getTableMappings() {
            return tables;
        }

        public void setTableMappings(ConcurrentHashMap<String, PagesMapping> tableMappings) {
            this.tables = tableMappings;
        }

        public ConcurrentHashMap<String, PagesMapping> getIndexMappings() {
            return indexes;
        }

        public void setIndexMappings(ConcurrentHashMap<String, PagesMapping> indexMappings) {
            this.indexes = indexMappings;
        }

        @Override
        public String toString() {
            return "TableSpacePagesMapping{" + "tableMappings=" + tables + ", indexMappings=" + indexes + '}';
        }


    }

    private TableSpacePagesMapping getTableSpacePagesMapping(String tableSpace) {
        return tableSpaceMappings.computeIfAbsent(tableSpace, s -> new TableSpacePagesMapping());
    }

    private void persistTableSpaceMapping(String tableSpace) {
        try {
            TableSpacePagesMapping mapping = getTableSpacePagesMapping(tableSpace);
            byte[] serialized = MAPPER.writeValueAsBytes(mapping);
            String znode = getTableSpaceMappingZNode(tableSpace);
            LOGGER.log(Level.FINE, "persistTableSpaceMapping " + tableSpace + ", " + mapping + " to " + znode + " JSON " + new String(serialized, StandardCharsets.UTF_8));
            writeZNodeEnforceOwnership(tableSpace, znode, serialized, null);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void loadTableSpacesAtBoot() throws DataStorageManagerException {
        List<String> list = zkGetChildren(rootZkNode, false);
        LOGGER.log(Level.INFO, "tableSpaces to load: {0}", list);
        for (String tablespace : list) {
            loadTableSpaceMapping(tablespace);
        }
    }

    private void loadTableSpaceMapping(String tableSpace) throws DataStorageManagerException {
        String tableSpaceNode = getTableSpaceZNode(tableSpace);
        Stat stat = new Stat();
        byte[] exists = readZNode(tableSpaceNode, stat);
        if (exists == null) {
            throw new DataStorageManagerException("znode " + tableSpace + " does not exist");
        }
        tableSpaceZkNodeVersion.put(tableSpace, stat.getVersion());
        LOGGER.log(Level.INFO, "loadTableSpaceMapping " + tableSpace + ", tableSpaceZkNodeVersion is " + stat.getVersion());

        String znode = getTableSpaceMappingZNode(tableSpace);
        byte[] serialized = readZNode(znode, null);
        if (serialized == null) {
            LOGGER.log(Level.INFO, "loadTableSpaceMapping " + tableSpace + ", from " + znode + " was not found");
            tableSpaceMappings.put(tableSpace, new TableSpacePagesMapping());
        } else {
            try {
                TableSpacePagesMapping read = MAPPER.readValue(serialized, TableSpacePagesMapping.class);
                LOGGER.log(Level.INFO, "loadTableSpaceMapping " + tableSpace + ", " + read + " from " + znode + " JSON " + new String(serialized, StandardCharsets.UTF_8));
                tableSpaceMappings.put(tableSpace, read);
            } catch (IOException ex) {
                throw new DataStorageManagerException(ex);
            }
        }
    }

    private String getTableSpaceZNode(String tableSpaceUUID) {
        return rootZkNode + "/" + tableSpaceUUID;
    }

    public BookKeeperDataStorageManager(String nodeId, Path baseDirectory, ZookeeperMetadataStorageManager zk, BookkeeperCommitLogManager bk) {
        this(nodeId, baseDirectory.resolve("tmp"),
                ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT,
                zk,
                bk,
                new NullStatsLogger());
    }

    public BookKeeperDataStorageManager(
            String nodeId, Path tmpDirectory, int swapThreshold, ZookeeperMetadataStorageManager zk, BookkeeperCommitLogManager bk, StatsLogger logger
    ) {
        this.nodeId = nodeId;
        this.tmpDirectory = tmpDirectory;
        this.swapThreshold = swapThreshold;
        StatsLogger scope = logger.scope("bkdatastore");
        this.dataPageReads = scope.getOpStatsLogger("data_pagereads");
        this.dataPageWrites = scope.getOpStatsLogger("data_pagewrites");
        this.indexPageReads = scope.getOpStatsLogger("index_pagereads");
        this.indexPageWrites = scope.getOpStatsLogger("index_pagewrites");
        this.zkReads = scope.getCounter("zkReads");
        this.zkWrites = scope.getCounter("zkWrites");
        this.zkGetChildren = scope.getCounter("zkGetChildren");
        this.zk = zk;
        this.bk = bk;
        this.rootZkNode = zk.getBasePath() + "/data";
    }

    @Override
    public void start() throws DataStorageManagerException {
        try {
            LOGGER.log(Level.FINE, "preparing tmp directory {0}", tmpDirectory.toAbsolutePath().toString());
            FileUtils.cleanDirectory(tmpDirectory);
            Files.createDirectories(tmpDirectory);
            LOGGER.log(Level.INFO, "preparing root znode " + rootZkNode);
            try {
                zk.ensureZooKeeper().create(rootZkNode, EMPTY_ARRAY, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.log(Level.INFO, "first boot of this cluster, created " + rootZkNode);
            } catch (KeeperException.NodeExistsException exists) {
            }
            loadTableSpacesAtBoot();
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {
        LOGGER.log(Level.FINE, "cleaning tmp directory {0}", tmpDirectory.toAbsolutePath().toString());
        try {
            FileUtils.cleanDirectory(tmpDirectory);
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Cannot clean tmp directory", err);
        }
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        SystemInstrumentation.instrumentationPoint("eraseTablespaceData", tableSpace);
        String tableSpaceZNode = getTableSpaceZNode(tableSpace);

        LOGGER.log(Level.INFO, "erasing tablespace " + tableSpace + " znode {0}", tableSpaceZNode);
        try {
            ZKUtil.deleteRecursive(zk.ensureZooKeeper(), tableSpaceZNode);
        } catch (KeeperException | IOException err) {
            LOGGER.log(Level.SEVERE, "Cannot clean znode for tablespace " + tableSpace, err);
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.SEVERE, "Cannot clean znode for tablespace " + tableSpace, err);
            throw new DataStorageManagerException(err);
        }
    }

    public static final String EXTENSION_TABLEORINDExCHECKPOINTINFOFILE = ".checkpoint";

    private static boolean isTablespaceCheckPointInfoFile(String name) {
        name = getFilename(name);
        return (name.startsWith("checkpoint.") && name.endsWith(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE));
    }

    private String getTableSpaceMappingZNode(String tablespace) {
        return getTableSpaceZNode(tablespace) + "/" + "pagemap";
    }

    private String getTablespaceCheckPointInfoFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTableSpaceZNode(tablespace) + "/" + ("checkpoint." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + EXTENSION_TABLEORINDExCHECKPOINTINFOFILE);
    }

    private String getTablespaceTablesMetadataFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTableSpaceZNode(tablespace) + "/" + ("tables." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tablesmetadata");
    }

    private static boolean isTablespaceTablesMetadataFile(String filename) {
        filename = getFilename(filename);
        return filename != null && filename.startsWith("tables.")
                && filename.endsWith(".tablesmetadata");
    }

    private static boolean isTablespaceIndexesMetadataFile(String filename) {
        filename = getFilename(filename);
        return filename != null && filename.startsWith("indexes.")
                && filename.endsWith(".tablesmetadata");
    }

    private String getTablespaceIndexesMetadataFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTableSpaceZNode(tablespace) + "/" + ("indexes." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tablesmetadata");
    }

    private String getTablespaceTransactionsFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTableSpaceZNode(tablespace) + "/" + ("transactions." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tx");
    }

    private static boolean isTransactionsFile(String filename) {
        filename = getFilename(filename);
        return filename != null && filename.startsWith("transactions.")
                && filename.endsWith(".tx");
    }

    private String getTableDirectory(String tablespace, String tablename) {
        return getTableSpaceZNode(tablespace) + "/" + (tablename + ".table");
    }

    private String getIndexDirectory(String tablespace, String indexname) {
        return getTableSpaceZNode(tablespace) + "/" + (indexname + ".index");
    }

    private static String getCheckPointsFile(String tableDirectory, LogSequenceNumber sequenceNumber) {
        return tableDirectory + "/" + sequenceNumber.ledgerId + "." + sequenceNumber.offset + EXTENSION_TABLEORINDExCHECKPOINTINFOFILE;
    }

    private static boolean isTableOrIndexCheckpointsFile(String filename) {
        filename = getFilename(filename);
        return filename != null && filename.endsWith(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE);
    }

    @Override
    public void initIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        String indexDir = getIndexDirectory(tableSpace, uuid);
        LOGGER.log(Level.FINE, "initIndex {0} {1} at {2}", new Object[]{tableSpace, uuid, indexDir});
        createZNode(tableSpace, indexDir, EMPTY_ARRAY, false);
    }

    @Override
    public void initTablespace(String tableSpace) throws DataStorageManagerException {
        LOGGER.log(Level.FINE, "initTablespace {0}", tableSpace);

        blindEnsureTablespaceNodeExists(tableSpace);
        // load current status
        loadTableSpaceMapping(tableSpace);
        // fence out other nodes
        persistTableSpaceMapping(tableSpace);

    }

    private void blindEnsureTablespaceNodeExists(String tableSpace) throws DataStorageManagerException {
        try {
            zk.ensureZooKeeper().create(getTableSpaceZNode(tableSpace), EMPTY_ARRAY, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok){
        } catch (KeeperException | IOException err){
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void initTable(String tableSpace, String uuid) throws DataStorageManagerException {
        String tableDir = getTableDirectory(tableSpace, uuid);
        LOGGER.log(Level.FINE, "initTable {0} {1} at {2}", new Object[]{tableSpace, uuid, tableDir});
        createZNode(tableSpace, tableDir, EMPTY_ARRAY, false);
    }

    @Override
    public List<Record> readPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        TableSpacePagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace);
        Long ledgerId = tableSpacePagesMapping.getTablePagesMapping(tableName).getLedgerIdForPage(pageId);
        if (ledgerId == null) {
            throw new DataPageDoesNotExistException("No such page: " + tableSpace + "_" + tableName + "." + pageId);
        }
        byte[] data;
        try (ReadHandle read =
                FutureUtils.result(bk.getBookKeeper()
                        .newOpenLedgerOp()
                        .withLedgerId(ledgerId)
                        .withPassword(EMPTY_ARRAY)
                        .execute(), BKException.HANDLER);) {
            try (LedgerEntries entries = read.readUnconfirmed(0, 0);) {
                data = entries.getEntry(0).getEntryBytes();
            }
            List<Record> result = rawReadDataPage(data);
            long _stop = System.currentTimeMillis();
            long delta = _stop - _start;
            LOGGER.log(Level.FINE, "readPage {0}.{1} {2} ms", new Object[]{tableSpace, tableName, delta + ""});
            dataPageReads.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
            return result;
        } catch (BKException.BKNoSuchLedgerExistsException err) {
            throw new DataStorageManagerException(err);
        } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException err) {
            throw new DataStorageManagerException(err);
        } catch (org.apache.bookkeeper.client.api.BKException err) {
            throw new DataStorageManagerException(err);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }

    }

    private static List<Record> rawReadDataPage(byte[] dataPage) throws IOException, DataStorageManagerException {
        try (ByteArrayCursor dataIn = ByteArrayCursor.wrap(dataPage)) {
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted data");
            }
            int numRecords = dataIn.readInt();
            List<Record> result = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                Bytes key = dataIn.readBytesNoCopy();
                Bytes value = dataIn.readBytesNoCopy();
                result.add(new Record(key, value));
            }
            int pos = dataIn.getPosition();
            long hashFromFile = dataIn.readLong();
            // after the hash we will have zeroes or garbage
            // the hash is not at the end of file, but after data
            long hashFromDigest = XXHash64Utils.hash(dataPage, 0, pos);
            if (hashFromDigest != hashFromFile) {
                throw new DataStorageManagerException("Corrupted datafile. Bad hash " + hashFromFile + " <> " + hashFromDigest);
            }
            return result;
        }
    }

    private static <X> X readIndexPage(byte[] dataPage, DataReader<X> reader) throws IOException, DataStorageManagerException {
        try (ByteArrayCursor dataIn = ByteArrayCursor.wrap(dataPage)) {
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted data file");
            }
            X result = reader.read(dataIn);
            int pos = dataIn.getPosition();
            long hashFromFile = dataIn.readLong();
            // after the hash we will have zeroes or garbage
            // the hash is not at the end of file, but after data
            long hashFromDigest = XXHash64Utils.hash(dataPage, 0, pos);
            if (hashFromDigest != hashFromFile) {
                throw new DataStorageManagerException("Corrupted datafile . Bad hash " + hashFromFile + " <> " + hashFromDigest);
            }
            return result;
        }
    }

    @Override
    public <X> X readIndexPage(String tableSpace, String indexName, Long pageId, DataReader<X> reader) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();

        TableSpacePagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace);
        Long ledgerId = tableSpacePagesMapping.getIndexPagesMapping(indexName).getLedgerIdForPage(pageId);
        if (ledgerId == null) {
            throw new DataPageDoesNotExistException("No such page for index : " + tableSpace + "_" + indexName + "." + pageId);
        }
        byte[] data;
        try (ReadHandle read =
                FutureUtils.result(bk.getBookKeeper()
                        .newOpenLedgerOp()
                        .withLedgerId(ledgerId)
                        .withPassword(EMPTY_ARRAY)
                        .execute(), BKException.HANDLER);) {
            try (LedgerEntries entries = read.readUnconfirmed(0, 0);) {
                data = entries.getEntry(0).getEntryBytes();
            }
            X result = readIndexPage(data, reader);
            long _stop = System.currentTimeMillis();
            long delta = _stop - _start;

            LOGGER.log(Level.FINE, "readIndexPage {0}.{1} {2} ms", new Object[]{tableSpace, indexName, delta + ""});
            indexPageReads.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
            return result;
        } catch (BKException.BKNoSuchLedgerExistsException err) {
            throw new DataStorageManagerException(err);
        } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException err) {
            throw new DataStorageManagerException(err);
        } catch (org.apache.bookkeeper.client.api.BKException err) {
            throw new DataStorageManagerException(err);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void fullTableScan(String tableSpace, String tableName, FullTableScanConsumer consumer) throws DataStorageManagerException {
        try {
            TableStatus status = getLatestTableStatus(tableSpace, tableName);
            fullTableScan(tableSpace, tableName, status, consumer);
        } catch (HerdDBInternalException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void fullTableScan(String tableSpace, String tableUuid, LogSequenceNumber sequenceNumber, FullTableScanConsumer consumer) throws DataStorageManagerException {
        try {
            TableStatus status = getTableStatus(tableSpace, tableUuid, sequenceNumber);
            fullTableScan(tableSpace, tableUuid, status, consumer);
        } catch (HerdDBInternalException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private void fullTableScan(String tableSpace, String tableUuid, TableStatus status, FullTableScanConsumer consumer) {
        LOGGER.log(Level.FINE, "fullTableScan table {0}.{1}, status: {2}", new Object[]{tableSpace, tableUuid, status});
        consumer.acceptTableStatus(status);
        List<Long> activePages = new ArrayList<>(status.activePages.keySet());
        activePages.sort(null);
        for (long idpage : activePages) {
            List<Record> records = readPage(tableSpace, tableUuid, idpage);
            LOGGER.log(Level.FINE, "fullTableScan table {0}.{1}, page {2}, contains {3} records", new Object[]{tableSpace, tableUuid, idpage, records.size()});
            consumer.acceptPage(idpage, records);
        }
        consumer.endTable();
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String tableName) throws DataStorageManagerException {
        TableStatus latestStatus = getLatestTableStatus(tableSpace, tableName);
        return latestStatus.activePages.size();
    }

    @Override
    public IndexStatus getIndexStatus(String tableSpace, String indexName, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {

        String dir = getIndexDirectory(tableSpace, indexName);
        String checkpointFile = getCheckPointsFile(dir, sequenceNumber);

        checkExistsZNode(checkpointFile, "no such index checkpoint: " + checkpointFile);

        return readIndexStatusFromFile(checkpointFile);
    }

    private void checkExistsZNode(String checkpointFile, String message) throws DataStorageManagerException {
        try {
            if (zk.ensureZooKeeper().exists(checkpointFile, false) == null) {
                throw new DataStorageManagerException(message);
            }
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    private byte[] readZNode(String checkpointFile, Stat stat) throws DataStorageManagerException {
        try {
            zkReads.inc();
            return zk.ensureZooKeeper().getData(checkpointFile, false, stat);
        } catch (KeeperException.NoNodeException err) {
            return null;
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    private void deleteZNodeEnforceOwnership(String tableSpace, String path) throws DataStorageManagerException {
        try {
            zkWrites.inc();
            Op opUpdateRoot = createUpdateTableSpaceRootOp(tableSpace);
            Op opDelete = Op.delete(path, -1);
            List<OpResult> multi = zk.ensureZooKeeper().multi(Arrays.asList(opUpdateRoot, opDelete));
            updateRootZkNodeVersion(tableSpace, multi.get(0));
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    private void writeZNodeEnforceOwnership(String tableSpace, String path, byte[] content, Stat stat) throws DataStorageManagerException {
        try {
            zkWrites.inc();
            Op opUpdateRoot = createUpdateTableSpaceRootOp(tableSpace);
            Op opCreate = Op.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            try {
                List<OpResult> multi = zk.ensureZooKeeper().multi(Arrays.asList(opUpdateRoot, opCreate));
                updateRootZkNodeVersion(tableSpace, multi.get(0));
            } catch (KeeperException.NodeExistsException ok) {
                LOGGER.log(Level.FINE, "Node exists {0}", ok.getPath());
            }

            opUpdateRoot = createUpdateTableSpaceRootOp(tableSpace);
            Op opUpdate = Op.setData(path, content, stat != null ? stat.getVersion() : -1);
            List<OpResult> multi = zk.ensureZooKeeper().multi(Arrays.asList(opUpdateRoot, opUpdate));
            updateRootZkNodeVersion(tableSpace, multi.get(0));
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    private void createZNode(String tableSpace, String path, byte[] content, boolean errorIfExists) throws DataStorageManagerException {
        try {
            zkWrites.inc();
            Op opUpdateRoot = createUpdateTableSpaceRootOp(tableSpace);
            Op opCreate = Op.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<OpResult> multi = zk.ensureZooKeeper().multi(Arrays.asList(opUpdateRoot, opCreate));
            updateRootZkNodeVersion(tableSpace, multi.get(0));
        } catch (KeeperException.NodeExistsException err) {
            if (errorIfExists) {
                throw new DataStorageManagerException(err);
            }
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    private Op createUpdateTableSpaceRootOp(String tableSpace) {
        String tableSpaceRootNode = getTableSpaceZNode(tableSpace);
        int tableSpaceRootNodeVersion = tableSpaceZkNodeVersion.get(tableSpace);
        Op opUpdateRoot = Op.setData(tableSpaceRootNode, EMPTY_ARRAY, tableSpaceRootNodeVersion);
        LOGGER.log(Level.FINE, "createUpdateTableSpaceRootOp " + tableSpaceRootNode + " version " + tableSpaceRootNodeVersion);
        return opUpdateRoot;
    }

    private void updateRootZkNodeVersion(String tableSpace, OpResult ensureLeadership) throws DataStorageManagerException {
        OpResult.SetDataResult res = (OpResult.SetDataResult) ensureLeadership;
        LOGGER.log(Level.FINE, "updateRootZkNodeVersion " + tableSpace + " newversion " + res.getStat().getVersion());
        tableSpaceZkNodeVersion.put(tableSpace, res.getStat().getVersion());
    }

    private List<String> ensureZNodeDirectoryAndReturnChildren(String tableSpace, String znode) throws DataStorageManagerException {
        try {
            if (zk.ensureZooKeeper().exists(znode, false) != null) {
                return zkGetChildren(znode);
            } else {
                createZNode(tableSpace, znode, EMPTY_ARRAY, true);
                return Collections.emptyList();
            }
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    private List<String> zkGetChildren(String znode) throws DataStorageManagerException {
        return zkGetChildren(znode, true);
    }
    private List<String> zkGetChildren(String znode, boolean appendParent) throws DataStorageManagerException {
        try {
            zkGetChildren.inc();
            List<String> res = zk.ensureZooKeeper().getChildren(znode, false);
            if (appendParent) {
            return res.stream().map(s -> znode + "/" + s).collect(Collectors.toList());
            } else {
                return res;
            }
        } catch (KeeperException.NoNodeException err) {
            return Collections.emptyList();
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String tableUuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        try {

            String dir = getTableDirectory(tableSpace, tableUuid);
            String checkpointFile = getCheckPointsFile(dir, sequenceNumber);

            checkExistsZNode(checkpointFile, "no such table checkpoint: " + checkpointFile);

            return readTableStatusFromFile(checkpointFile);

        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String tableName) throws DataStorageManagerException {
        try {
            String lastFile = getLastTableCheckpointFile(tableSpace, tableName);
            TableStatus latestStatus;
            if (lastFile == null) {
                latestStatus = TableStatus.buildTableStatusForNewCreatedTable(tableName);
            } else {
                latestStatus = readTableStatusFromFile(lastFile);
            }
            return latestStatus;
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }

    public TableStatus readTableStatusFromFile(String checkpointsFile) throws IOException {
        byte[] fileContent = readZNode(checkpointsFile, new Stat());
        return readTableStatusFromFile(fileContent, checkpointsFile);
    }

    public TableStatus readTableStatusFromFile(byte[] fileContent, String znode) throws IOException {
        if (fileContent == null) {
            throw new IOException("Missing ZNode for TableStatus at " + znode);
        }
        XXHash64Utils.verifyBlockWithFooter(fileContent, 0, fileContent.length);
        try (InputStream input = new SimpleByteArrayInputStream(fileContent);
                ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted table status file " + znode);
            }
            return TableStatus.deserialize(dataIn);
        }
    }

    private String getLastTableCheckpointFile(String tableSpace, String tableName) throws IOException, KeeperException, InterruptedException {
        String dir = getTableDirectory(tableSpace, tableName);
        String result = getMostRecentCheckPointFile(tableSpace, dir);
        return result;
    }

    private String getMostRecentCheckPointFile(String tableSpace, String dir) throws IOException, KeeperException, InterruptedException {
        String result = null;
        long lastMod = -1;
        List<String> children = ensureZNodeDirectoryAndReturnChildren(tableSpace, dir);

        for (String fullpath : children) {
            if (isTableOrIndexCheckpointsFile(fullpath)) {
                LOGGER.log(Level.INFO, "getMostRecentCheckPointFile on " + dir + " -> ACCEPT " + fullpath);
                Stat stat = new Stat();
                zk.ensureZooKeeper().exists(fullpath, false, null, stat);
                long ts = stat.getMtime();
                if (lastMod < 0 || lastMod < ts) {
                    result = fullpath;
                    lastMod = ts;
                }
            } else {
                LOGGER.log(Level.INFO, "getMostRecentCheckPointFile on " + dir + " -> SKIP " + fullpath);
            }
        }

        LOGGER.log(Level.INFO, "getMostRecentCheckPointFile on " + dir + " -> " + result);
        return result;
    }

    public IndexStatus readIndexStatusFromFile(String checkpointsFile) throws DataStorageManagerException {
        byte[] fileContent = readZNode(checkpointsFile, new Stat());
        if (fileContent == null) {
            throw new DataStorageManagerException("Missing znode for " + checkpointsFile + " IndexStatusFile");
        }
        return readIndexStatusFromFile(fileContent, checkpointsFile);
    }

    public IndexStatus readIndexStatusFromFile(byte[] fileContent, String checkpointsFile) throws DataStorageManagerException {
        try {
            if (fileContent == null) {
                throw new DataStorageManagerException("Missing znode for " + checkpointsFile + " IndexStatusFile");
            }
            XXHash64Utils.verifyBlockWithFooter(fileContent, 0, fileContent.length);
            try (InputStream input = new SimpleByteArrayInputStream(fileContent);
                    ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
                long version = dataIn.readVLong(); // version
                long flags = dataIn.readVLong(); // flags for future implementations
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted index status file " + checkpointsFile);
                }
                return IndexStatus.deserialize(dataIn);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String tableName, TableStatus tableStatus, boolean pin) throws DataStorageManagerException {
        // ensure that current mapping has been persisted safely
        persistTableSpaceMapping(tableSpace);

        LogSequenceNumber logPosition = tableStatus.sequenceNumber;
        String dir = getTableDirectory(tableSpace, tableName);
        String checkpointFile = getCheckPointsFile(dir, logPosition);
        Stat stat = new Stat();
        try {
            byte[] exists = readZNode(checkpointFile, stat);
            if (exists != null) {
                TableStatus actualStatus = readTableStatusFromFile(checkpointFile);
                if (actualStatus != null && actualStatus.equals(tableStatus)) {
                    LOGGER.log(Level.FINE,
                            "tableCheckpoint " + tableSpace + ", " + tableName + ": " + tableStatus + " (pin:" + pin + ") already saved on file " + checkpointFile);
                    return Collections.emptyList();
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        LOGGER.log(Level.FINE, "tableCheckpoint " + tableSpace + ", " + tableName + ": " + tableStatus + " (pin:" + pin + ") to file " + checkpointFile);
        byte[] content;
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                XXHash64Utils.HashingOutputStream oo = new XXHash64Utils.HashingOutputStream(buffer);
                ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(oo)) {

            dataOutputKeys.writeVLong(1); // version
            dataOutputKeys.writeVLong(0); // flags for future implementations
            tableStatus.serialize(dataOutputKeys);

            dataOutputKeys.writeLong(oo.hash());
            dataOutputKeys.flush();
            content = buffer.toByteArray();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        writeZNodeEnforceOwnership(tableSpace, checkpointFile, content, stat);

        /* Checkpoint pinning */
        final Map<Long, Integer> pins = pinTableAndGetPages(tableSpace, tableName, tableStatus, pin);
        final Set<LogSequenceNumber> checkpoints = pinTableAndGetCheckpoints(tableSpace, tableName, tableStatus, pin);

        long maxPageId = tableStatus.activePages.keySet().stream().max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        List<PostCheckpointAction> result = new ArrayList<>();
        PagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace).getTablePagesMapping(tableName);
        // we can drop old page files now
        for (Map.Entry<Long, Long> pages : tableSpacePagesMapping.pages.entrySet()) {
            long pageId = pages.getKey();
            long ledgerId = pages.getValue();
            LOGGER.log(Level.FINEST, "checkpoint pageId {0} ledgerId {1}", new Object[]{pageId, ledgerId});
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !tableStatus.activePages.containsKey(pageId)
                    && pageId < maxPageId) {
                LOGGER.log(Level.FINEST, "checkpoint ledger " + ledgerId + " pageId " + pageId + ". will be deleted after checkpoint end");
                result.add(new DropLedgerForTableAction(tableSpace, tableName, "delete page " + pageId + " ledgerId " + ledgerId, pageId, ledgerId));
            }
        }

        // we can drop orphan ledgers
        for (Long ledgerId : tableSpacePagesMapping.oldLedgers) {
            LOGGER.log(Level.FINEST, "checkpoint ledger " + ledgerId + " without page. will be deleted after checkpoint end");
            result.add(new DropLedgerForTableAction(tableSpace, tableName, "delete unused ledgerId " + ledgerId, Long.MAX_VALUE, ledgerId));
        }

        List<String> children = zkGetChildren(dir);
        try {
            for (String p : children) {
                if (isTableOrIndexCheckpointsFile(p) && !p.equals(checkpointFile)) {
                    TableStatus status = readTableStatusFromFile(p);
                    if (logPosition.after(status.sequenceNumber) && !checkpoints.contains(status.sequenceNumber)) {
                        LOGGER.log(Level.FINEST, "checkpoint metadata znode " + p + ". will be deleted after checkpoint end");
                        result.add(new DeleteZNodeAction(tableSpace, tableName, "delete checkpoint metadata znode " + p, p));
                    }
                }
            }
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Could not list table dir " + dir, err);
        }
        return result;
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String indexName, IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
        String dir = getIndexDirectory(tableSpace, indexName);
        LogSequenceNumber logPosition = indexStatus.sequenceNumber;
        String checkpointFile = getCheckPointsFile(dir, logPosition);

        Stat stat = new Stat();
        byte[] exists = readZNode(checkpointFile, stat);
        if (exists != null) {
            IndexStatus actualStatus = readIndexStatusFromFile(exists, checkpointFile);
            if (actualStatus != null && actualStatus.equals(indexStatus)) {
                LOGGER.log(Level.INFO,
                        "indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " already saved on" + checkpointFile);
                return Collections.emptyList();
            }
        }
        LOGGER.log(Level.FINE, "indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " to file " + checkpointFile);
        byte[] content;
        try (
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                XXHash64Utils.HashingOutputStream oo = new XXHash64Utils.HashingOutputStream(buffer);
                ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(oo)) {

            dataOutputKeys.writeVLong(1); // version
            dataOutputKeys.writeVLong(0); // flags for future implementations
            indexStatus.serialize(dataOutputKeys);

            dataOutputKeys.writeLong(oo.hash());
            dataOutputKeys.flush();
            content = buffer.toByteArray();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        writeZNodeEnforceOwnership(tableSpace, checkpointFile, content, stat);

        /* Checkpoint pinning */
        final Map<Long, Integer> pins = pinIndexAndGetPages(tableSpace, indexName, indexStatus, pin);
        final Set<LogSequenceNumber> checkpoints = pinIndexAndGetCheckpoints(tableSpace, indexName, indexStatus, pin);

        long maxPageId = indexStatus.activePages.stream().max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        List<PostCheckpointAction> result = new ArrayList<>();
        // we can drop old page files now
        PagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace).getIndexPagesMapping(indexName);
        // we can drop old page files now
        for (Map.Entry<Long, Long> pages : tableSpacePagesMapping.pages.entrySet()) {
            long pageId = pages.getKey();
            long ledgerId = pages.getValue();
            LOGGER.log(Level.FINEST, "checkpoint pageId {0} ledgerId {1}", new Object[]{pageId, ledgerId});
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !indexStatus.activePages.contains(pageId)
                    && pageId < maxPageId) {
                LOGGER.log(Level.FINEST, "checkpoint ledger " + ledgerId + " pageId " + pageId + ". will be deleted after checkpoint end");
                result.add(new DropLedgerForIndexAction(tableSpace, indexName, "delete index page " + pageId + " ledgerId " + ledgerId, pageId, ledgerId));
            }
        }

        // we can drop orphan ledgers
        for (Long ledgerId : tableSpacePagesMapping.oldLedgers) {
            LOGGER.log(Level.FINEST, "checkpoint ledger " + ledgerId + " without page. will be deleted after checkpoint end");
            result.add(new DropLedgerForIndexAction(tableSpace, indexName, "delete unused ledgerId " + ledgerId, Long.MAX_VALUE, ledgerId));
        }

        List<String> children = zkGetChildren(dir);

        for (String p : children) {
            if (isTableOrIndexCheckpointsFile(p) && !p.equals(checkpointFile)) {
                IndexStatus status = readIndexStatusFromFile(p);
                if (logPosition.after(status.sequenceNumber) && !checkpoints.contains(status.sequenceNumber)) {
                    LOGGER.log(Level.FINEST, "checkpoint metadata file " + p + ". will be deleted after checkpoint end");
                    result.add(new DeleteZNodeAction(tableSpace, indexName, "delete checkpoint metadata file " + p, p));
                }
            }
        }

        return result;
    }

    private static String getFilename(String s) {
        int last = s.lastIndexOf("/");
        return s.substring(last + 1);
    }

    private static long getPageId(String p) {
        String filename = getFilename(p);
        if (filename.endsWith(FILEEXTENSION_PAGE)) {
            try {
                return Long.parseLong(filename.substring(0, filename.length() - FILEEXTENSION_PAGE.length()));
            } catch (NumberFormatException no) {
                return -1;
            }
        } else {
            return -1;
        }
    }

    @Override
    public void cleanupAfterTableBoot(String tableSpace, String tableName, Set<Long> activePagesAtBoot) throws DataStorageManagerException {
        // we have to drop old page files or page files partially written by checkpoint interrupted at JVM crash/reboot
        PagesMapping tablePagesMapping = getTableSpacePagesMapping(tableSpace).getTablePagesMapping(tableName);

        for (Iterator<Map.Entry<Long, Long>> entry = tablePagesMapping.pages.entrySet().iterator(); entry.hasNext();) {
            Map.Entry<Long, Long> next = entry.next();
            long pageId = next.getKey();
            long ledgerId = next.getValue();
            LOGGER.log(Level.FINER, "cleanupAfterTableBoot pageId " + pageId + " ledger id " + ledgerId);
            if (pageId > 0 && !activePagesAtBoot.contains(pageId)) {
                LOGGER.log(Level.INFO, "cleanupAfterTableBoot pageId " + pageId + " ledger id " + ledgerId + ". will be deleted");
                // dropLedgerForTable will remove the entry from the map in case of successful deletion of the ledger
                dropLedgerForTable(tableSpace, tableName, pageId, ledgerId, "cleanupAfterBoot " + tableSpace + "." + tableName + " pageId " + pageId);
            }
        }
    }

    /**
     * Write a record page
     *
     * @param newPage data to write
     * @param file managed file used for sync operations
     * @param stream output stream related to given managed file for write
     * operations
     * @return
     * @throws IOException
     */
    private static long writePage(Collection<Record> newPage, VisibleByteArrayOutputStream oo) throws IOException {

        try (ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo)) {

            dataOutput.writeVLong(1); // version
            dataOutput.writeVLong(0); // flags for future implementations
            dataOutput.writeInt(newPage.size());
            for (Record record : newPage) {
                dataOutput.writeArray(record.key);
                dataOutput.writeArray(record.value);
            }
            dataOutput.flush();
            long hash = XXHash64Utils.hash(oo.getBuffer(), 0, oo.size());
            dataOutput.writeLong(hash);
            dataOutput.flush();
            return oo.size();
        }

    }

    @Override
    public void writePage(String tableSpace, String tableName, long pageId, Collection<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long _start = System.currentTimeMillis();

        long size;
        long ledgerId;
        try {
            try (VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream()) {
                size = writePage(newPage, buffer);
                Map<String, byte[]> metadata = new HashMap<>();
                metadata.put("tablespaceuuid", tableSpace.getBytes(StandardCharsets.UTF_8));
                metadata.put("node", nodeId.getBytes(StandardCharsets.UTF_8));
                metadata.put("application", "herddb".getBytes(StandardCharsets.UTF_8));
                metadata.put("component", "datastore".getBytes(StandardCharsets.UTF_8));
                metadata.put("table", tableName.getBytes(StandardCharsets.UTF_8));
                metadata.put("type", "datapage".getBytes(StandardCharsets.UTF_8));
                int expectedReplicaCount = tableSpaceExpectedReplicaCount.getOrDefault(tableSpace, 1);
                int actualEnsembleSize = Math.max(expectedReplicaCount, bk.getEnsemble());
                int actualWriteQuorumSize = Math.max(expectedReplicaCount, bk.getWriteQuorumSize());
                int actualAckQuorumSize = Math.max(expectedReplicaCount, bk.getAckQuorumSize());
                try (WriteHandle result =
                         FutureUtils.result(bk.getBookKeeper()
                                .newCreateLedgerOp()
                                .withEnsembleSize(actualEnsembleSize)
                                .withWriteQuorumSize(actualWriteQuorumSize)
                                .withAckQuorumSize(actualAckQuorumSize)
                                .withPassword(EMPTY_ARRAY)
                                .withDigestType(DigestType.CRC32C)
                                .withCustomMetadata(metadata)
                                .execute(), BKException.HANDLER);) {
                    result.append(buffer.getBuffer(), 0, buffer.size());
                    ledgerId = result.getId();
                }
            }

        } catch (IOException | org.apache.bookkeeper.client.api.BKException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
        getTableSpacePagesMapping(tableSpace).getTablePagesMapping(tableName).writePageId(pageId, ledgerId);

        long now = System.currentTimeMillis();
        long delta = (now - _start);

        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, "writePage {0} KBytes,{1} records, time {2} ms", new Object[]{(size / 1024) + "", newPage.size(), delta + ""});
        }
        dataPageWrites.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
    }

    private static long writeIndexPage(DataWriter writer, VisibleByteArrayOutputStream stream) throws IOException {
        try (
                ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(stream)) {
            dataOutput.writeVLong(1); // version
            dataOutput.writeVLong(0); // flags for future implementations
            writer.write(dataOutput);
            dataOutput.flush();
            long hash = XXHash64Utils.hash(stream.getBuffer(), 0, stream.size());
            dataOutput.writeLong(hash);
            dataOutput.flush();
            return stream.size();
        }
    }

    @Override
    public void writeIndexPage(
            String tableSpace, String indexName,
            long pageId, DataWriter writer
    ) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();

        long size;
        long ledgerId;
        try {
            try (VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream()) {

                size = writeIndexPage(writer, buffer);
                Map<String, byte[]> metadata = new HashMap<>();
                metadata.put("tablespaceuuid", tableSpace.getBytes(StandardCharsets.UTF_8));
                metadata.put("node", nodeId.getBytes(StandardCharsets.UTF_8));
                metadata.put("application", "herddb".getBytes(StandardCharsets.UTF_8));
                metadata.put("component", "datastore".getBytes(StandardCharsets.UTF_8));
                metadata.put("index", indexName.getBytes(StandardCharsets.UTF_8));
                metadata.put("type", "indexpage".getBytes(StandardCharsets.UTF_8));
                ledgerId = writeToLedger(tableSpace, metadata, buffer);
            }

        } catch (IOException | org.apache.bookkeeper.client.api.BKException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
        LOGGER.log(Level.INFO, "writeIndexPage {0} pageId {1}, ledgerId {2}", new Object[]{indexName, pageId, ledgerId});
        getTableSpacePagesMapping(tableSpace).getIndexPagesMapping(indexName).writePageId(pageId, ledgerId);

        long now = System.currentTimeMillis();
        long delta = (now - _start);
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, "writePage {0} KBytes, time {2} ms", new Object[]{(size / 1024) + "", delta + ""});
        }
        indexPageWrites.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
    }

    private long writeToLedger(String tableSpace, Map<String, byte[]> metadata, final VisibleByteArrayOutputStream buffer) throws InterruptedException, org.apache.bookkeeper.client.api.BKException {
        long ledgerId;
        int expectedReplicaCount = tableSpaceExpectedReplicaCount.getOrDefault(tableSpace, 1);
        int actualEnsembleSize = Math.max(expectedReplicaCount, bk.getEnsemble());
        int actualWriteQuorumSize = Math.max(expectedReplicaCount, bk.getWriteQuorumSize());
        int actualAckQuorumSize = Math.max(expectedReplicaCount, bk.getAckQuorumSize());
        try (WriteHandle result =
                FutureUtils.result(bk.getBookKeeper()
                        .newCreateLedgerOp()
                        .withEnsembleSize(actualEnsembleSize)
                        .withWriteQuorumSize(actualWriteQuorumSize)
                        .withAckQuorumSize(actualAckQuorumSize)
                        .withPassword(EMPTY_ARRAY)
                        .withDigestType(DigestType.CRC32C)
                        .withCustomMetadata(metadata)
                        .execute(), BKException.HANDLER);) {
            result.append(buffer.getBuffer(), 0, buffer.size());
            ledgerId = result.getId();
        }
        return ledgerId;
    }

    private static LogSequenceNumber readLogSequenceNumberFromTablesMetadataFile(String tableSpace, byte[] data, String znode) throws DataStorageManagerException {
        try (InputStream input = new ByteArrayInputStream(data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted table list znode" + znode);
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("znode " + znode + " is not for spablespace " + tableSpace);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            return new LogSequenceNumber(ledgerId, offset);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private static LogSequenceNumber readLogSequenceNumberFromIndexMetadataFile(String tableSpace, byte[] data, String znode) throws DataStorageManagerException {
        try (InputStream input = new ByteArrayInputStream(data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted index list znode " + znode);
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("znode " + znode + " is not for spablespace " + tableSpace);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            return new LogSequenceNumber(ledgerId, offset);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        try {
            String file = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
            LOGGER.log(Level.INFO, "loadTables for tableSpace " + tableSpace + " from " + file + ", sequenceNumber:" + sequenceNumber);
            byte[] content = readZNode(file, new Stat());
            if (content == null) {
                if (sequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.INFO, "zode " + file + " not found");
                    return Collections.emptyList();
                } else {
                    throw new DataStorageManagerException("local table data not available for tableSpace " + tableSpace + ", recovering from sequenceNumber " + sequenceNumber);
                }
            }
            return readTablespaceStructure(content, tableSpace, sequenceNumber);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    public static List<Table> readTablespaceStructure(byte[] file, String tableSpace, LogSequenceNumber sequenceNumber) throws IOException, DataStorageManagerException {
        try (InputStream input = new ByteArrayInputStream(file);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted table list file");
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("file is not for spablespace " + tableSpace + " but for " + readname);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            if (sequenceNumber != null) {
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file is not for sequence number " + sequenceNumber);
                }
            }
            int numTables = din.readInt();
            List<Table> res = new ArrayList<>();
            for (int i = 0; i < numTables; i++) {
                byte[] tableData = din.readArray();
                Table table = Table.deserialize(tableData);
                res.add(table);
            }
            return Collections.unmodifiableList(res);
        }
    }

    @Override
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        try {
            String file = getTablespaceIndexesMetadataFile(tableSpace, sequenceNumber);

            LOGGER.log(Level.INFO, "loadIndexes for tableSpace " + tableSpace + " from " + file + ", sequenceNumber:" + sequenceNumber);
            byte[] content = readZNode(file, new Stat());
            if (content == null) {
                if (sequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.INFO, "file " + file + " not found");
                    return Collections.emptyList();
                } else {
                    throw new DataStorageManagerException("local index data not available for tableSpace " + tableSpace + ", recovering from sequenceNumber " + sequenceNumber);
                }
            }
            try (InputStream input = new ByteArrayInputStream(content);
                    ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
                long version = din.readVLong(); // version
                long flags = din.readVLong(); // flags for future implementations
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted index list file " + file);
                }
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file + " is not for tablespace " + tableSpace);
                }
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file " + file + " is not for sequence number " + sequenceNumber);
                }
                int numTables = din.readInt();
                List<Index> res = new ArrayList<>();
                for (int i = 0; i < numTables; i++) {
                    byte[] indexData = din.readArray();
                    Index table = Index.deserialize(indexData);
                    res.add(table);
                }
                return Collections.unmodifiableList(res);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public Collection<PostCheckpointAction> writeTables(
            String tableSpace, LogSequenceNumber sequenceNumber,
            List<Table> tables, List<Index> indexlist, boolean prepareActions
    ) throws DataStorageManagerException {
        if (sequenceNumber.isStartOfTime() && !tables.isEmpty()) {
            throw new DataStorageManagerException("impossible to write a non empty table list at start-of-time");
        }

        // we need to flush current mappings, because here we are flushing
        // the status of all of the tables and indexes
        persistTableSpaceMapping(tableSpace);

        String tableSpaceDirectory = getTableSpaceZNode(tableSpace);
        String fileTables = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
        String fileIndexes = getTablespaceIndexesMetadataFile(tableSpace, sequenceNumber);

        LOGGER.log(Level.FINE, "writeTables for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + fileTables);
        try (VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(buffer)) {

            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags for future implementations
            dout.writeUTF(tableSpace);
            dout.writeZLong(sequenceNumber.ledgerId);
            dout.writeZLong(sequenceNumber.offset);
            dout.writeInt(tables.size());
            for (Table t : tables) {
                byte[] tableSerialized = t.serialize();
                dout.writeArray(tableSerialized);
            }

            dout.flush();
            writeZNodeEnforceOwnership(tableSpace, fileTables, buffer.toByteArray(), null);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        try (VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(buffer)) {

            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags for future implementations
            dout.writeUTF(tableSpace);
            dout.writeZLong(sequenceNumber.ledgerId);
            dout.writeZLong(sequenceNumber.offset);
            if (indexlist != null) {
                dout.writeInt(indexlist.size());
                for (Index t : indexlist) {
                    byte[] indexSerialized = t.serialize();
                    dout.writeArray(indexSerialized);
                }
            } else {
                dout.writeInt(0);
            }

            dout.flush();
            writeZNodeEnforceOwnership(tableSpace, fileIndexes, buffer.toByteArray(), null);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Collection<PostCheckpointAction> result = new ArrayList<>();
        if (prepareActions) {
            List<String> stream = zkGetChildren(tableSpaceDirectory);

            for (String p : stream) {
                if (isTablespaceIndexesMetadataFile(p)) {
                    try {
                        byte[] content = readZNode(p, new Stat());
                        if (content != null) {
                            LogSequenceNumber logPositionInFile = readLogSequenceNumberFromIndexMetadataFile(tableSpace, content, p);
                            if (sequenceNumber.after(logPositionInFile)) {
                                LOGGER.log(Level.FINEST, "indexes metadata file " + p + ". will be deleted after checkpoint end");
                                result.add(new DeleteZNodeAction(tableSpace, "indexes", "delete indexesmetadata file " + p, p));
                            }
                        }
                    } catch (DataStorageManagerException ignore) {
                        LOGGER.log(Level.SEVERE, "Unparsable indexesmetadata file " + p, ignore);
                        result.add(new DeleteZNodeAction(tableSpace, "indexes", "delete unparsable indexesmetadata file " + p, p));
                    }
                } else if (isTablespaceTablesMetadataFile(p)) {
                    try {
                        byte[] content = readZNode(p, new Stat());
                        if (content != null) {
                            LogSequenceNumber logPositionInFile = readLogSequenceNumberFromTablesMetadataFile(tableSpace, content, p);
                            if (sequenceNumber.after(logPositionInFile)) {
                                LOGGER.log(Level.FINEST, "tables metadata file " + p + ". will be deleted after checkpoint end");
                                result.add(new DeleteZNodeAction(tableSpace, "tables", "delete tablesmetadata file " + p, p));
                            }
                        }
                    } catch (DataStorageManagerException ignore) {
                        LOGGER.log(Level.SEVERE, "Unparsable tablesmetadata file " + p, ignore);
                        result.add(new DeleteZNodeAction(tableSpace, "transactions", "delete unparsable tablesmetadata file " + p, p));
                    }
                }
            }
        }
        return result;
    }


    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        // ensure that current page mappings are persisted on ZK
        persistTableSpaceMapping(tableSpace);

        String checkPointFile = getTablespaceCheckPointInfoFile(tableSpace, sequenceNumber);

        LOGGER.log(Level.INFO, "checkpoint for {0} at {1} to {2}", new Object[]{tableSpace, sequenceNumber, checkPointFile});

        try (VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(buffer)) {

            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags for future implementations
            dout.writeUTF(tableSpace);
            dout.writeZLong(sequenceNumber.ledgerId);
            dout.writeZLong(sequenceNumber.offset);

            dout.flush();
            writeZNodeEnforceOwnership(tableSpace, checkPointFile, buffer.toByteArray(), null);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        String tableSpaceDirectory = getTableSpaceZNode(tableSpace);
        List<String> stream = zkGetChildren(tableSpaceDirectory);
        Collection<PostCheckpointAction> result = new ArrayList<>();
        for (String p : stream) {
            if (isTablespaceCheckPointInfoFile(p)) {
                try {
                    byte[] content = readZNode(p, new Stat());
                    if (content != null) {
                        LogSequenceNumber logPositionInFile = readLogSequenceNumberFromCheckpointInfoFile(tableSpace, content, p);
                        if (sequenceNumber.after(logPositionInFile)) {
                            LOGGER.log(Level.FINEST, "checkpoint info file " + p + ". will be deleted after checkpoint end");
                            result.add(new DeleteZNodeAction(tableSpace, "checkpoint", "delete checkpoint info file " + p, p));
                        }
                    }
                } catch (DataStorageManagerException | IOException ignore) {
                    LOGGER.log(Level.SEVERE, "unparsable checkpoint info file " + p, ignore);
                    // do not auto-delete checkpoint files
                }
            }
        }
        return result;

    }

    @Override
    public void dropTable(String tablespace, String tableName) throws DataStorageManagerException {
        persistTableSpaceMapping(tablespace);
        String tableDir = getTableDirectory(tablespace, tableName);
        LOGGER.log(Level.INFO, "dropTable {0}.{1} in {2}", new Object[]{tablespace, tableName, tableDir});
        try {
            ZKUtil.deleteRecursive(zk.ensureZooKeeper(), tableDir);
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    @Override
    public void truncateIndex(String tablespace, String name) throws DataStorageManagerException {
        persistTableSpaceMapping(tablespace);
        String tableDir = getIndexDirectory(tablespace, name);
        LOGGER.log(Level.INFO, "truncateIndex {0}.{1} in {2}", new Object[]{tablespace, name, tableDir});
        try {
            ZKUtil.deleteRecursive(zk.ensureZooKeeper(), tableDir);
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    @Override
    public void dropIndex(String tablespace, String name) throws DataStorageManagerException {
        persistTableSpaceMapping(tablespace);
        String tableDir = getIndexDirectory(tablespace, name);
        LOGGER.log(Level.INFO, "dropIndex {0}.{1} in {2}", new Object[]{tablespace, name, tableDir});
        try {
            ZKUtil.deleteRecursive(zk.ensureZooKeeper(), tableDir);
        } catch (KeeperException | InterruptedException | IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    private static LogSequenceNumber readLogSequenceNumberFromCheckpointInfoFile(String tableSpace, byte[] data, String checkPointFile) throws DataStorageManagerException, IOException {
        try (InputStream input = new ByteArrayInputStream(data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new IOException("corrupted checkpoint file");
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("zonde " + checkPointFile + " is not for spablespace " + tableSpace + " but for " + readname);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();

            return new LogSequenceNumber(ledgerId, offset);
        }
    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) throws DataStorageManagerException {
        try {
            String tableSpaceDirectory = getTableSpaceZNode(tableSpace);
            LogSequenceNumber max = LogSequenceNumber.START_OF_TIME;
            List<String> stream = zkGetChildren(tableSpaceDirectory);
            for (String p : stream) {
                if (isTablespaceCheckPointInfoFile(p)) {
                    try {
                        byte[] content = readZNode(p, new Stat());
                        if (content != null) {
                            LogSequenceNumber logPositionInFile = readLogSequenceNumberFromCheckpointInfoFile(tableSpace, content, p);
                            if (logPositionInFile.after(max)) {
                                max = logPositionInFile;
                            }
                        }
                    } catch (DataStorageManagerException ignore) {
                        LOGGER.log(Level.SEVERE, "unparsable checkpoint info file " + p, ignore);
                    }
                }
            }
            return max;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);

        }
    }

    @Override
    public KeyToPageIndex createKeyToPageMap(String tablespace, String name, MemoryManager memoryManager) throws DataStorageManagerException {
        return new BLinkKeyToPageIndex(tablespace, name, memoryManager, this);
    }

    @Override
    public void releaseKeyToPageMap(String tablespace, String name, KeyToPageIndex keyToPage) {
        if (keyToPage != null) {
            keyToPage.close();
        }
    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return new FileRecordSetFactory(tmpDirectory, swapThreshold);
    }

    private static LogSequenceNumber readLogSequenceNumberFromTransactionsFile(String tableSpace, byte[] data, String file) throws DataStorageManagerException {
        try (InputStream input = new ByteArrayInputStream(data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted transaction list znode " + file);
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("znode " + file + " is not for spablespace " + tableSpace);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            return new LogSequenceNumber(ledgerId, offset);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace, Consumer<Transaction> consumer) throws DataStorageManagerException {
        try {
            String file = getTablespaceTransactionsFile(tableSpace, sequenceNumber);
            byte[] content = readZNode(file, new Stat());
            boolean exists = content != null;
            LOGGER.log(Level.INFO, "loadTransactions " + sequenceNumber + " for tableSpace " + tableSpace + " from file " + file + " (exists: " + exists + ")");
            if (!exists) {
                return;
            }
            try (InputStream input = new ByteArrayInputStream(content);
                    ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
                long version = din.readVLong(); // version
                long flags = din.readVLong(); // flags for future implementations
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted transaction list file " + file);
                }
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file " + file + " is not for sequence number " + sequenceNumber);
                }
                int numTransactions = din.readInt();
                for (int i = 0; i < numTransactions; i++) {
                    Transaction tx = Transaction.deserialize(tableSpace, din);
                    consumer.accept(tx);
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void tableSpaceMetadataUpdated(String tableSpace, int expectedReplicaCount) {
        tableSpaceExpectedReplicaCount.put(tableSpace, expectedReplicaCount);
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions) throws DataStorageManagerException {
        if (sequenceNumber.isStartOfTime() && !transactions.isEmpty()) {
            throw new DataStorageManagerException("impossible to write a non empty transactions list at start-of-time");
        }
        String checkPointFile = getTablespaceTransactionsFile(tableSpace, sequenceNumber);

        LOGGER.log(Level.FINE, "writeTransactionsAtCheckpoint for tableSpace {0} sequenceNumber {1} to {2}, active transactions {3}", new Object[]{tableSpace, sequenceNumber, checkPointFile, transactions.size()});
        try (VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream();
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(buffer)) {

            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags for future implementations
            dout.writeUTF(tableSpace);
            dout.writeZLong(sequenceNumber.ledgerId);
            dout.writeZLong(sequenceNumber.offset);
            dout.writeInt(transactions.size());
            for (Transaction t : transactions) {
                t.serialize(dout);
            }

            dout.flush();
            writeZNodeEnforceOwnership(tableSpace, checkPointFile, buffer.toByteArray(), null);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Collection<PostCheckpointAction> result = new ArrayList<>();
        String tableSpaceDirectory = getTableSpaceZNode(tableSpace);
        List<String> stream = zkGetChildren(tableSpaceDirectory);
        for (String p : stream) {
            if (isTransactionsFile(p)) {
                try {
                    byte[] content = readZNode(checkPointFile, new Stat());
                    if (content != null) {
                        LogSequenceNumber logPositionInFile = readLogSequenceNumberFromTransactionsFile(tableSpace, content, p);
                        if (sequenceNumber.after(logPositionInFile)) {
                            LOGGER.log(Level.FINEST, "transactions metadata file " + p + ". will be deleted after checkpoint end");
                            result.add(new DeleteZNodeAction(tableSpace, "transactions", "delete transactions file " + p, p));
                        }
                    }
                } catch (DataStorageManagerException ignore) {
                    LOGGER.log(Level.SEVERE, "Unparsable transactions file " + p, ignore);
                    result.add(new DeleteZNodeAction(tableSpace, "transactions", "delete unparsable transactions file " + p, p));
                }
            }
        }

        return result;
    }

    private class DeleteZNodeAction extends PostCheckpointAction {

        private final String znode;

        public DeleteZNodeAction(String tableSpace, String tableName, String description, String znode) {
            super(tableSpace, tableName, description);
            this.znode = znode;
        }

        @Override
        public void run() {
            try {
                LOGGER.log(Level.FINE, description);
                deleteZNodeEnforceOwnership(tableSpace, znode);
            } catch (DataStorageManagerException err) {
                LOGGER.log(Level.SEVERE, "Could not delete znode " + znode + ":" + err, err);
            }
        }
    }

    private void dropLedgerForTable(String tableSpace, String tableName, long pageId, long ledgerId, String description) {
        try {
            LOGGER.log(Level.FINE, description);
            try {
                FutureUtils.result(bk.getBookKeeper()
                        .newDeleteLedgerOp()
                        .withLedgerId(ledgerId)
                        .execute(), BKException.HANDLER);
            } catch (BKNoSuchLedgerExistsOnMetadataServerException err) {
                LOGGER.log(Level.SEVERE, "ledger " + ledgerId + " already dropped:" + err, err);
            }
            getTableSpacePagesMapping(tableSpace).getTablePagesMapping(tableName).removePageId(pageId);

        } catch (BKException err) {
            LOGGER.log(Level.SEVERE, "Could not delete ledger " + ledgerId + ":" + err, err);
        }
    }

    private void dropLedgerForIndex(String tableSpace, String indexName, long pageId, long ledgerId, String description) {
        try {
            LOGGER.log(Level.FINE, description);
            try {
                FutureUtils.result(bk.getBookKeeper()
                        .newDeleteLedgerOp()
                        .withLedgerId(ledgerId)
                        .execute(), BKException.HANDLER);
            } catch (BKNoSuchLedgerExistsOnMetadataServerException err) {
                LOGGER.log(Level.SEVERE, "ledger " + ledgerId + " already dropped:" + err, err);
            }
            getTableSpacePagesMapping(tableSpace).getIndexPagesMapping(indexName).removePageId(pageId);

        } catch (BKException err) {
            LOGGER.log(Level.SEVERE, "Could not delete ledger " + ledgerId + ":" + err, err);
        }
    }

    private class DropLedgerForTableAction extends PostCheckpointAction {

        private final long ledgerId;
        private final long pageId;

        public DropLedgerForTableAction(String tableSpace, String tableName, String description, long pageId, long ledgerId) {
            super(tableSpace, tableName, description);
            this.pageId = pageId;
            this.ledgerId = ledgerId;
        }

        @Override
        public void run() {
            dropLedgerForTable(tableSpace, tableName, pageId, ledgerId, description);
        }
    }

    private class DropLedgerForIndexAction extends PostCheckpointAction {

        private final long ledgerId;
        private final long pageId;

        public DropLedgerForIndexAction(String tableSpace, String tableName, String description, long pageId, long ledgerId) {
            super(tableSpace, tableName, description);
            this.pageId = pageId;
            this.ledgerId = ledgerId;
        }

        @Override
        public void run() {
            dropLedgerForIndex(tableSpace, tableName, pageId, ledgerId, description);
        }
    }

    private static final Recycler<RecyclableByteArrayOutputStream> WRITE_BUFFERS_RECYCLER = new Recycler<RecyclableByteArrayOutputStream>() {

        @Override
        protected RecyclableByteArrayOutputStream newObject(
                Recycler.Handle<RecyclableByteArrayOutputStream> handle
        ) {
            return new RecyclableByteArrayOutputStream(handle);
        }

    };

    private static RecyclableByteArrayOutputStream getWriteBuffer() {
        RecyclableByteArrayOutputStream res = WRITE_BUFFERS_RECYCLER.get();
        res.closed = false;
        res.reset();
        return res;
    }

    /**
     * These buffers are useful only inside FileDataStorageManager, because they
     * will eventually be mostly of about maximum page size bytes large.
     */
    private static class RecyclableByteArrayOutputStream extends VisibleByteArrayOutputStream {

        private static final int DEFAULT_INITIAL_SIZE = 1024 * 1024;
        private final io.netty.util.Recycler.Handle<RecyclableByteArrayOutputStream> handle;
        private boolean closed;

        RecyclableByteArrayOutputStream(Recycler.Handle<RecyclableByteArrayOutputStream> handle) {
            super(DEFAULT_INITIAL_SIZE);
            this.handle = handle;
        }

        @Override
        public void close() {
            if (!closed) { // prevent double 'recycle'
                super.close();
                handle.recycle(this);
                closed = true;
            }
        }

    }
}
