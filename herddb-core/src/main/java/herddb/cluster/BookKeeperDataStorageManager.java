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

import static herddb.file.FileDataStorageManager.O_DIRECT_BLOCK_BATCH;
import herddb.file.*;
import herddb.core.HerdDBInternalException;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
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
import herddb.utils.CleanDirectoryFileVisitor;
import herddb.utils.DeleteFileVisitor;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.ManagedFile;
import herddb.utils.ODirectFileOutputStream;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.SystemInstrumentation;
import herddb.utils.SystemProperties;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import io.netty.util.Recycler;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * Data Storage on BookKeeper.
 *
 * Beware that this kind of storage is useful when you have most of the DB in memory.
 * Random access to BK is generally slower than when using local disk, and we also have to deal
 * with lots of metadata to be stored on Zookeeper.
 *
 * @author enrico.olivelli
 */
public class BookKeeperDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(BookKeeperDataStorageManager.class.getName());    
    private final Path tmpDirectory;
    private final int swapThreshold;
    private final StatsLogger logger;
    private final OpStatsLogger dataPageReads;
    private final OpStatsLogger dataPageWrites;
    private final OpStatsLogger indexPageReads;
    private final OpStatsLogger indexPageWrites;
    private final ZookeeperMetadataStorageManager zk;
    private final BookkeeperCommitLogManager bk;
    
    private final ConcurrentHashMap<String, TableSpacePagesMapping> tableSpaceMappings = new ConcurrentHashMap<>();
    
    private final String baseZkNode;

    public static final String FILEEXTENSION_PAGE = ".page";
    
    private static final class TableSpacePagesMapping {
        ConcurrentHashMap<Long, Long> pageIdToLedgerId = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Long> indexpageIdToLedgerId = new ConcurrentHashMap<>();

        private Long getLedgerIdForDatapage(Long pageId) {
            return pageIdToLedgerId.get(pageId);
        }
        private Long getLedgerIdForindexpage(Long pageId) {
            return indexpageIdToLedgerId.get(pageId);
        }
    }
    
    private TableSpacePagesMapping getTableSpacePagesMapping(String tableSpace) {
        return tableSpaceMappings.computeIfAbsent(tableSpace, s -> new TableSpacePagesMapping());
    }
    
    private String getTableSpaceZNode(String tableSpaceUUID) {
        return baseZkNode + "/" + tableSpaceUUID;
    }

    /**
     * Standard buffer size for data copies
     */
    public static final int COPY_BUFFERS_SIZE =
            SystemProperties.getIntSystemProperty("herddb.file.copybuffersize", 64 * 1024);

   
    public BookKeeperDataStorageManager(Path baseDirectory, ZookeeperMetadataStorageManager zk, BookkeeperCommitLogManager bk) {
        this(baseDirectory.resolve("tmp"),
                ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT,
                zk,
                bk,
                new NullStatsLogger());
    }

    public BookKeeperDataStorageManager(
            Path tmpDirectory, int swapThreshold, ZookeeperMetadataStorageManager zk, BookkeeperCommitLogManager bk, StatsLogger logger
    ) {
        this.tmpDirectory = tmpDirectory;
        this.swapThreshold = swapThreshold;
        this.logger = logger;
        StatsLogger scope = logger.scope("bkdatastore");
        this.dataPageReads = scope.getOpStatsLogger("data_pagereads");
        this.dataPageWrites = scope.getOpStatsLogger("data_pagewrites");
        this.indexPageReads = scope.getOpStatsLogger("index_pagereads");
        this.indexPageWrites = scope.getOpStatsLogger("index_pagewrites");
        this.zk = zk;
        this.bk = bk;
        this.baseZkNode = zk.getBasePath()+"/data";
    }

    @Override
    public void start() throws DataStorageManagerException {
        try {
            LOGGER.log(Level.INFO, "ensuring directory {0}", tmpDirectory.toAbsolutePath().toString());
            Files.createDirectories(tmpDirectory);
            LOGGER.log(Level.INFO, "preparing tmp directory {0}", tmpDirectory.toAbsolutePath().toString());
            FileUtils.cleanDirectory(tmpDirectory);
            Files.createDirectories(tmpDirectory);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "cleaning tmp directory {0}", tmpDirectory.toAbsolutePath().toString());
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
        return (name.startsWith("checkpoint.") && name.endsWith(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE))
                || name.equals(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE); // legacy 1.0 file
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

    private static String getPageFile(String tableDirectory, Long pageId) {
        return tableDirectory + "/" + (pageId + FILEEXTENSION_PAGE);
    }

    private static String getTableCheckPointsFile(String tableDirectory, LogSequenceNumber sequenceNumber) {
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
        
        try {
            zk.ensureZooKeeper().create(indexDir, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (IOException | KeeperException err) {
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
        try {
            zk.ensureZooKeeper().create(tableDir, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }


    @Override
    public List<Record> readPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();        
        TableSpacePagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace);
        Long ledgerId = tableSpacePagesMapping.getLedgerIdForDatapage(pageId);
        if (ledgerId == null) {
            throw new DataPageDoesNotExistException("No such page: " + tableSpace + "_" + tableName + "." + pageId);
        }
        byte[] data;
        try (ReadHandle read =
                FutureUtils.result(bk.getBookKeeper()
                        .newOpenLedgerOp()
                        .withLedgerId(ledgerId)
                        .withPassword(new byte[0])
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
        } catch (BKException.BKNoSuchLedgerExistsException err)  {
            throw new DataStorageManagerException(err);
        } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException err)  {
            throw new DataStorageManagerException(err);
        } catch (org.apache.bookkeeper.client.api.BKException err)  {
            throw new DataStorageManagerException(err);
        } catch (IOException err)  {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err)  {
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
        String tableDir = getIndexDirectory(tableSpace, indexName);
        long _start = System.currentTimeMillis();
        
        TableSpacePagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace);
        Long ledgerId = tableSpacePagesMapping.getLedgerIdForindexpage(pageId);
        if (ledgerId == null) {
            throw new DataPageDoesNotExistException("No such page for index : " + tableSpace + "_" + indexName + "." + pageId);
        }
        byte[] data;
        try (ReadHandle read =
                FutureUtils.result(bk.getBookKeeper()
                        .newOpenLedgerOp()
                        .withLedgerId(ledgerId)
                        .withPassword(new byte[0])
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
        } catch (BKException.BKNoSuchLedgerExistsException err)  {
            throw new DataStorageManagerException(err);
        } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException err)  {
            throw new DataStorageManagerException(err);
        } catch (org.apache.bookkeeper.client.api.BKException err)  {
            throw new DataStorageManagerException(err);
        } catch (IOException err)  {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err)  {
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
        LOGGER.log(Level.INFO, "fullTableScan table {0}.{1}, status: {2}", new Object[]{tableSpace, tableUuid, status});
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
        String checkpointFile = getTableCheckPointsFile(dir, sequenceNumber);

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
    
    private byte[] readZNode(String checkpointFile) throws DataStorageManagerException {
        try {
            return zk.ensureZooKeeper().getData(checkpointFile, false, new Stat());            
        } catch (KeeperException.NoNodeException err) {
            return null;
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }
    
    private void writeZNode(String checkpointFile, byte[] content) throws DataStorageManagerException {
        try {
            try {
                zk.ensureZooKeeper().create(checkpointFile, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException err) {
                zk.ensureZooKeeper().setData(checkpointFile, content, -1);            
            } 
        } catch (IOException | KeeperException err) {
            throw new DataStorageManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException(err);
        }
    }
    
    private List<String> ensureZNodeDirectoryAndReturnChildren(String znode) throws DataStorageManagerException {
        try {
            if (zk.ensureZooKeeper().exists(znode, false) == null) {
                return zk.ensureZooKeeper().getChildren(znode, false);
            } else {
                zk.ensureZooKeeper().create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        try {
            return zk.ensureZooKeeper().getChildren(znode, false);            
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
            String checkpointFile = getTableCheckPointsFile(dir, sequenceNumber);

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
                latestStatus = new TableStatus(tableName, LogSequenceNumber.START_OF_TIME,
                        Bytes.longToByteArray(1), 1, Collections.emptyMap());
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
        byte[] fileContent = readZNode(checkpointsFile);
        return readTableStatusFromFile(fileContent, checkpointsFile);        
    }
    
    public TableStatus readTableStatusFromFile(byte[] fileContent, String znode) throws IOException {
        if (fileContent == null) {
            throw new IOException("Missing ZNode for TableStatus at "+znode);
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
        String result = getMostRecentCheckPointFile(dir);
        return result;
    }

    private String getMostRecentCheckPointFile(String dir) throws IOException, KeeperException, InterruptedException {
        String result = null;
        long lastMod = -1;
        List<String> children = ensureZNodeDirectoryAndReturnChildren(dir);
        
         for (String path : children) {
             String fullpath = dir + "/"+path;
            if (isTableOrIndexCheckpointsFile(path)) {
                LOGGER.log(Level.INFO, "getMostRecentCheckPointFile on " + dir + " -> ACCEPT " + fullpath);
                Stat stat = new Stat();
                zk.ensureZooKeeper().exists(fullpath , false, null, stat);
                long ts = stat.getMtime();
                if (lastMod < 0 || lastMod < ts) {
                    result = fullpath;
                    lastMod = ts;
                }
            } else {
                LOGGER.log(Level.INFO, "getMostRecentCheckPointFile on " + dir  + " -> SKIP " + fullpath);
            }
        }
        
        LOGGER.log(Level.INFO, "getMostRecentCheckPointFile on " + dir + " -> " + result);
        return result;
    }

    public IndexStatus readIndexStatusFromFile(String checkpointsFile) throws DataStorageManagerException {
        try {
            byte[] fileContent = readZNode(checkpointsFile);
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
        LogSequenceNumber logPosition = tableStatus.sequenceNumber;
        String dir = getTableDirectory(tableSpace, tableName);
        String checkpointFile = getTableCheckPointsFile(dir, logPosition);
        try {
            byte[] exists = readZNode(checkpointFile);
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
        
        writeZNode(checkpointFile, content);

        /* Checkpoint pinning */
        final Map<Long, Integer> pins = pinTableAndGetPages(tableSpace, tableName, tableStatus, pin);
        final Set<LogSequenceNumber> checkpoints = pinTableAndGetCheckpoints(tableSpace, tableName, tableStatus, pin);

        long maxPageId = tableStatus.activePages.keySet().stream().max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        List<PostCheckpointAction> result = new ArrayList<>();
        TableSpacePagesMapping tableSpacePagesMapping = getTableSpacePagesMapping(tableSpace);
        // we can drop old page files now        
        for (Map.Entry<Long, Long> pages : tableSpacePagesMapping.pageIdToLedgerId.entrySet()) {
            long pageId = pages.getKey();
            long ledgerId = pages.getValue();
            LOGGER.log(Level.FINEST, "checkpoint pageId {0} ledgerId {1}", new Object[]{pageId, ledgerId});
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !tableStatus.activePages.containsKey(pageId)
                    && pageId < maxPageId) {
                LOGGER.log(Level.FINEST, "checkpoint file " + p.toAbsolutePath() + " pageId " + pageId + ". will be deleted after checkpoint end");
                result.add(new DropLedgerAction(tableName, "delete page " + pageId + " ledgerId " + ledgerId, ledgerId));
            }
        }

        List<String> children = zkGetChildren(dir);
        try {
            for (String p : children) {
                if (isTableOrIndexCheckpointsFile(p) && !p.equals(checkpointFile)) {
                    TableStatus status = readTableStatusFromFile(p);
                    if (logPosition.after(status.sequenceNumber) && !checkpoints.contains(status.sequenceNumber)) {
                        LOGGER.log(Level.FINEST, "checkpoint metadata file " + p + ". will be deleted after checkpoint end");
                        result.add(new DeleteZNodeAction(tableName, "delete checkpoint metadata file " + p, p));
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
        String checkpointFile = getTableCheckPointsFile(dir, logPosition);
        String parent = getParent(checkpointFile);
        String checkpointFileTemp = parent + "/" + checkpointFile.getFileName() + ".tmp";

        if (Files.isRegularFile(checkpointFile)) {
            IndexStatus actualStatus = readIndexStatusFromFile(checkpointFile);
            if (actualStatus != null && actualStatus.equals(indexStatus)) {
                LOGGER.log(Level.INFO,
                        "indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " already saved on" + checkpointFile);
                return Collections.emptyList();
            }
        }

        LOGGER.log(Level.FINE, "indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " to file " + checkpointFile);

        try (ManagedFile file = ManagedFile.open(checkpointFileTemp, requirefsync);
             SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE);
             XXHash64Utils.HashingOutputStream oo = new XXHash64Utils.HashingOutputStream(buffer);
             ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(oo)) {

            dataOutputKeys.writeVLong(1); // version
            dataOutputKeys.writeVLong(0); // flags for future implementations
            indexStatus.serialize(dataOutputKeys);

            dataOutputKeys.writeLong(oo.hash());
            dataOutputKeys.flush();
            file.sync();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        try {
            Files.move(checkpointFileTemp, checkpointFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }


        /* Checkpoint pinning */
        final Map<Long, Integer> pins = pinIndexAndGetPages(tableSpace, indexName, indexStatus, pin);
        final Set<LogSequenceNumber> checkpoints = pinIndexAndGetCheckpoints(tableSpace, indexName, indexStatus, pin);

        long maxPageId = indexStatus.activePages.stream().max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        List<PostCheckpointAction> result = new ArrayList<>();
        // we can drop old page files now
        List<Path> pageFiles = getIndexPageFiles(tableSpace, indexName);
        for (Path p : pageFiles) {
            long pageId = getPageId(p);
            LOGGER.log(Level.FINEST, "checkpoint file {0} pageId {1}", new Object[]{p.toAbsolutePath(), pageId});
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !indexStatus.activePages.contains(pageId)
                    && pageId < maxPageId) {
                LOGGER.log(Level.FINEST, "checkpoint file " + p.toAbsolutePath() + " pageId " + pageId + ". will be deleted after checkpoint end");
                result.add(new DeleteFileAction(indexName, "delete page " + pageId + " file " + p.toAbsolutePath(), p));
            }
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                if (isTableOrIndexCheckpointsFile(p) && !p.equals(checkpointFile)) {
                    IndexStatus status = readIndexStatusFromFile(p);
                    if (logPosition.after(status.sequenceNumber) && !checkpoints.contains(status.sequenceNumber)) {
                        LOGGER.log(Level.FINEST, "checkpoint metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                        result.add(new DeleteFileAction(indexName, "delete checkpoint metadata file " + p.toAbsolutePath(), p));
                    }
                }
            }
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Could not list indexName dir " + dir, err);
        }

        return result;
    }

//    private static String getParent(String file) throws DataStorageManagerException {
//        int last = file.lastIndexOf("/");
//        if (last >= 0) {
//            return file.substring(0, last - 1);
//        }
//        return "";
//    }

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

    private static boolean isPageFile(String path) {
        return getPageId(path) >= 0;
    }

    public List<String> getTablePageFiles(String tableSpace, String tableName) throws DataStorageManagerException {
        String tableDir = getTableDirectory(tableSpace, tableName);

        try (DirectoryStream<Path> files = Files.newDirectoryStream(tableDir, new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                return isPageFile(entry);
            }

        })) {
            List<Path> result = new ArrayList<>();
            files.forEach(result::add);
            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    public List<String> getIndexPageFiles(String tableSpace, String indexName) throws DataStorageManagerException {
        String indexDir = getIndexDirectory(tableSpace, indexName);

        try (DirectoryStream<Path> files = Files.newDirectoryStream(indexDir, new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                return isPageFile(entry);
            }

        })) {
            List<Path> result = new ArrayList<>();
            files.forEach(result::add);
            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void cleanupAfterBoot(String tableSpace, String tableName, Set<Long> activePagesAtBoot) throws DataStorageManagerException {
        // we have to drop old page files or page files partially written by checkpoint interrupted at JVM crash/reboot
        List<Path> pageFiles = getTablePageFiles(tableSpace, tableName);
        for (Path p : pageFiles) {
            long pageId = getPageId(p);
            LOGGER.log(Level.FINER, "cleanupAfterBoot file " + p.toAbsolutePath() + " pageId " + pageId);
            if (pageId > 0 && !activePagesAtBoot.contains(pageId)) {
                LOGGER.log(Level.INFO, "cleanupAfterBoot file " + p.toAbsolutePath() + " pageId " + pageId + ". will be deleted");
                try {
                    Files.deleteIfExists(p);
                } catch (IOException err) {
                    throw new DataStorageManagerException(err);
                }
            }
        }
    }

    /**
     * Write a record page
     *
     * @param newPage data to write
     * @param file    managed file used for sync operations
     * @param stream  output stream related to given managed file for write
     *                operations
     * @return
     * @throws IOException
     */
    private static long writePage(Collection<Record> newPage, ManagedFile file, OutputStream stream) throws IOException {

        try (RecyclableByteArrayOutputStream oo = getWriteBuffer();
             ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo)) {

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
            stream.write(oo.getBuffer(), 0, oo.size());
            if (file != null) { // O_DIRECT does not need fsync
                file.sync();
            }
            return oo.size();
        }

    }

    @Override
    public void writePage(String tableSpace, String tableName, long pageId, Collection<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long _start = System.currentTimeMillis();
        Path tableDir = getTableDirectory(tableSpace, tableName);
        Path pageFile = getPageFile(tableDir, pageId);
        long size;

        try {
            if (pageodirect) {
                try (ODirectFileOutputStream odirect = new ODirectFileOutputStream(pageFile, O_DIRECT_BLOCK_BATCH,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    size = writePage(newPage, null, odirect);
                }

            } else {
                try (ManagedFile file = ManagedFile.open(pageFile, requirefsync,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                     SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE)) {

                    size = writePage(newPage, file, buffer);
                }

            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        long now = System.currentTimeMillis();
        long delta = (now - _start);

        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, "writePage {0} KBytes,{1} records, time {2} ms", new Object[]{(size / 1024) + "", newPage.size(), delta + ""});
        }
        dataPageWrites.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
    }

    private static long writeIndexPage(DataWriter writer, ManagedFile file, OutputStream stream) throws IOException {
        try (RecyclableByteArrayOutputStream oo = getWriteBuffer();
             ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo)) {
            dataOutput.writeVLong(1); // version
            dataOutput.writeVLong(0); // flags for future implementations
            writer.write(dataOutput);
            dataOutput.flush();
            long hash = XXHash64Utils.hash(oo.getBuffer(), 0, oo.size());
            dataOutput.writeLong(hash);
            dataOutput.flush();
            stream.write(oo.getBuffer(), 0, oo.size());
            if (file != null) { // O_DIRECT does not need fsync
                file.sync();
            }
            return oo.size();
        }
    }

    @Override
    public void writeIndexPage(
            String tableSpace, String indexName,
            long pageId, DataWriter writer
    ) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        String tableDir = getIndexDirectory(tableSpace, indexName);

        String pageFile = getPageFile(tableDir, pageId);
        long size;
        try {
            if (indexodirect) {
                try (ODirectFileOutputStream odirect = new ODirectFileOutputStream(pageFile, O_DIRECT_BLOCK_BATCH,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    size = writeIndexPage(writer, null, odirect);
                }

            } else {
                try (ManagedFile file = ManagedFile.open(pageFile, requirefsync,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                     SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE)) {

                    size = writeIndexPage(writer, file, buffer);
                }
            }

        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Failed to write on path: {0}", pageFile);
            Path path = pageFile;
            boolean exists;
            do {
                exists = Files.exists(path);

                if (exists) {
                    LOGGER.log(Level.INFO,
                            "Path {0}: directory {1}, file {2}, link {3}, writable {4}, readable {5}, executable {6}",
                            new Object[] { path, Files.isDirectory(path), Files.isRegularFile(path),
                                    Files.isSymbolicLink(path), Files.isWritable(path), Files.isReadable(path),
                                    Files.isExecutable(path) });
                } else {
                    LOGGER.log(Level.INFO, "Path {0} doesn't exists", path);
                }

                path = path.getParent();
            } while (path != null && !exists);

            throw new DataStorageManagerException(err);
        }

        long now = System.currentTimeMillis();
        long delta = (now - _start);
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, "writePage {0} KBytes, time {2} ms", new Object[]{(size / 1024) + "", delta + ""});
        }
        indexPageWrites.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
    }

    private static LogSequenceNumber readLogSequenceNumberFromTablesMetadataFile(String tableSpace, Path file) throws DataStorageManagerException {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
             ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted table list file " + file.toAbsolutePath());
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            return new LogSequenceNumber(ledgerId, offset);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private static LogSequenceNumber readLogSequenceNumberFromIndexMetadataFile(String tableSpace, Path file) throws DataStorageManagerException {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
             ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted index list file " + file.toAbsolutePath());
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
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
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
            LOGGER.log(Level.INFO, "loadTables for tableSpace " + tableSpace + " from " + file.toAbsolutePath().toString() + ", sequenceNumber:" + sequenceNumber);
            if (!Files.isRegularFile(file)) {
                if (sequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.INFO, "file " + file.toAbsolutePath().toString() + " not found");
                    return Collections.emptyList();
                } else {
                    throw new DataStorageManagerException("local table data not available for tableSpace " + tableSpace + ", recovering from sequenceNumber " + sequenceNumber);
                }
            }
            return readTablespaceStructure(file, tableSpace, sequenceNumber);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    public static List<Table> readTablespaceStructure(Path file, String tableSpace, LogSequenceNumber sequenceNumber) throws IOException, DataStorageManagerException {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
             ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted table list file " + file.toAbsolutePath());
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();
            if (sequenceNumber != null) {
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for sequence number " + sequenceNumber);
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
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceIndexesMetadataFile(tableSpace, sequenceNumber);
            LOGGER.log(Level.INFO, "loadIndexes for tableSpace " + tableSpace + " from " + file.toAbsolutePath().toString() + ", sequenceNumber:" + sequenceNumber);
            if (!Files.isRegularFile(file)) {
                if (sequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.INFO, "file " + file.toAbsolutePath().toString() + " not found");
                    return Collections.emptyList();
                } else {
                    throw new DataStorageManagerException("local index data not available for tableSpace " + tableSpace + ", recovering from sequenceNumber " + sequenceNumber);
                }
            }
            try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
                 ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
                long version = din.readVLong(); // version
                long flags = din.readVLong(); // flags for future implementations
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted index list file " + file.toAbsolutePath());
                }
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for sequence number " + sequenceNumber);
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
        Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
        try {
            Files.createDirectories(tableSpaceDirectory);
            Path fileTables = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
            Path fileIndexes = getTablespaceIndexesMetadataFile(tableSpace, sequenceNumber);
            Path parent = getParent(fileTables);
            Files.createDirectories(parent);

            LOGGER.log(Level.FINE, "writeTables for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + fileTables.toAbsolutePath().toString());
            try (ManagedFile file = ManagedFile.open(fileTables, requirefsync);
                 SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE);
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
                file.sync();
            } catch (IOException err) {
                throw new DataStorageManagerException(err);
            }

            try (ManagedFile file = ManagedFile.open(fileIndexes, requirefsync);
                 SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE);
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
                file.sync();
            } catch (IOException err) {
                throw new DataStorageManagerException(err);
            }

        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Collection<PostCheckpointAction> result = new ArrayList<>();
        if (prepareActions) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableSpaceDirectory)) {
                for (Path p : stream) {
                    if (isTablespaceIndexesMetadataFile(p)) {
                        try {
                            LogSequenceNumber logPositionInFile = readLogSequenceNumberFromIndexMetadataFile(tableSpace, p);
                            if (sequenceNumber.after(logPositionInFile)) {
                                LOGGER.log(Level.FINEST, "indexes metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                                result.add(new DeleteFileAction("indexes", "delete indexesmetadata file " + p.toAbsolutePath(), p));
                            }
                        } catch (DataStorageManagerException ignore) {
                            LOGGER.log(Level.SEVERE, "Unparsable indexesmetadata file " + p.toAbsolutePath(), ignore);
                            result.add(new DeleteFileAction("indexes", "delete unparsable indexesmetadata file " + p.toAbsolutePath(), p));
                        }
                    } else if (isTablespaceTablesMetadataFile(p)) {
                        try {
                            LogSequenceNumber logPositionInFile = readLogSequenceNumberFromTablesMetadataFile(tableSpace, p);
                            if (sequenceNumber.after(logPositionInFile)) {
                                LOGGER.log(Level.FINEST, "tables metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                                result.add(new DeleteFileAction("tables", "delete tablesmetadata file " + p.toAbsolutePath(), p));
                            }
                        } catch (DataStorageManagerException ignore) {
                            LOGGER.log(Level.SEVERE, "Unparsable tablesmetadata file " + p.toAbsolutePath(), ignore);
                            result.add(new DeleteFileAction("transactions", "delete unparsable tablesmetadata file " + p.toAbsolutePath(), p));
                        }
                    }
                }
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Could not list dir " + tableSpaceDirectory, err);
            }
        }
        return result;
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
        try {
            Files.createDirectories(tableSpaceDirectory);
            Path checkPointFile = getTablespaceCheckPointInfoFile(tableSpace, sequenceNumber);
            Path parent = getParent(checkPointFile);
            Files.createDirectories(parent);

            Path checkpointFileTemp = parent.resolve(checkPointFile.getFileName() + ".tmp");
            LOGGER.log(Level.INFO, "checkpoint for " + tableSpace + " at " + sequenceNumber + " to " + checkPointFile.toAbsolutePath().toString());

            try (ManagedFile file = ManagedFile.open(checkpointFileTemp, requirefsync);
                 SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE);
                 ExtendedDataOutputStream dout = new ExtendedDataOutputStream(buffer)) {

                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags for future implementations
                dout.writeUTF(tableSpace);
                dout.writeZLong(sequenceNumber.ledgerId);
                dout.writeZLong(sequenceNumber.offset);

                dout.flush();
                file.sync();
            } catch (IOException err) {
                throw new DataStorageManagerException(err);
            }

            // write file atomically
            Files.move(checkpointFileTemp, checkPointFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Collection<PostCheckpointAction> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableSpaceDirectory)) {
            for (Path p : stream) {
                if (isTablespaceCheckPointInfoFile(p)) {
                    try {
                        LogSequenceNumber logPositionInFile = readLogSequenceNumberFromCheckpointInfoFile(tableSpace, p);
                        if (sequenceNumber.after(logPositionInFile)) {
                            LOGGER.log(Level.FINEST, "checkpoint info file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                            result.add(new DeleteFileAction("checkpoint", "delete checkpoint info file " + p.toAbsolutePath(), p));
                        }
                    } catch (DataStorageManagerException ignore) {
                        LOGGER.log(Level.SEVERE, "unparsable checkpoint info file " + p.toAbsolutePath(), ignore);
                        // do not auto-delete checkpoint files
                    }
                }
            }
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Could not list dir " + tableSpaceDirectory, err);
        }
        return result;

    }

    @Override
    public void dropTable(String tablespace, String tableName) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tablespace, tableName);
        LOGGER.log(Level.INFO, "dropTable {0}.{1} in {2}", new Object[]{tablespace, tableName, tableDir});
        try {
            deleteDirectory(tableDir);
        } catch (IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    @Override
    public void truncateIndex(String tablespace, String name) throws DataStorageManagerException {
        Path tableDir = getIndexDirectory(tablespace, name);
        LOGGER.log(Level.INFO, "truncateIndex {0}.{1} in {2}", new Object[]{tablespace, name, tableDir});
        try {
            cleanDirectory(tableDir);
        } catch (IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    @Override
    public void dropIndex(String tablespace, String name) throws DataStorageManagerException {
        Path tableDir = getIndexDirectory(tablespace, name);
        LOGGER.log(Level.INFO, "dropIndex {0}.{1} in {2}", new Object[]{tablespace, name, tableDir});
        try {
            deleteDirectory(tableDir);
        } catch (IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    private static LogSequenceNumber readLogSequenceNumberFromCheckpointInfoFile(String tableSpace, Path checkPointFile) throws DataStorageManagerException, IOException {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(checkPointFile, StandardOpenOption.READ), 4 * 1024 * 1024);
             ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new IOException("corrupted checkpoint file");
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("file " + checkPointFile.toAbsolutePath() + " is not for spablespace " + tableSpace);
            }
            long ledgerId = din.readZLong();
            long offset = din.readZLong();

            return new LogSequenceNumber(ledgerId, offset);
        }
    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) throws DataStorageManagerException {
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            LogSequenceNumber max = LogSequenceNumber.START_OF_TIME;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableSpaceDirectory)) {
                for (Path p : stream) {
                    if (isTablespaceCheckPointInfoFile(p)) {
                        try {
                            LogSequenceNumber logPositionInFile = readLogSequenceNumberFromCheckpointInfoFile(tableSpace, p);
                            if (logPositionInFile.after(max)) {
                                max = logPositionInFile;
                            }
                        } catch (DataStorageManagerException ignore) {
                            LOGGER.log(Level.SEVERE, "unparsable checkpoint info file " + p.toAbsolutePath(), ignore);
                        }
                    }
                }
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Could not list dir " + tableSpaceDirectory, err);
            }
            return max;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);

        }
    }

    public static void deleteDirectory(Path f) throws IOException {
        deleteDirectoryContent(f, false);
    }

    public static void cleanDirectory(Path f) throws IOException {
        deleteDirectoryContent(f, true);
    }

    private static void deleteDirectoryContent(Path f, boolean keepDirectory) throws IOException {
        if (Files.isDirectory(f)) {
            final SimpleFileVisitor<Path> visitor =
                    keepDirectory ? new CleanDirectoryFileVisitor() : DeleteFileVisitor.INSTANCE;
            Files.walkFileTree(f, visitor);
        } else if (Files.isRegularFile(f)) {
            throw new IOException("name " + f.toAbsolutePath() + " is not a directory");
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

    private static LogSequenceNumber readLogSequenceNumberFromTransactionsFile(String tableSpace, Path file) throws DataStorageManagerException {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
             ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
            long version = din.readVLong(); // version
            long flags = din.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted transaction list file " + file.toAbsolutePath());
            }
            String readname = din.readUTF();
            if (!readname.equals(tableSpace)) {
                throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
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
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceTransactionsFile(tableSpace, sequenceNumber);
            boolean exists = Files.isRegularFile(file);
            LOGGER.log(Level.INFO, "loadTransactions " + sequenceNumber + " for tableSpace " + tableSpace + " from file " + file + " (exists: " + exists + ")");
            if (!exists) {
                return;
            }
            try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
                 ExtendedDataInputStream din = new ExtendedDataInputStream(input)) {
                long version = din.readVLong(); // version
                long flags = din.readVLong(); // flags for future implementations
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted transaction list file " + file.toAbsolutePath());
                }
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for sequence number " + sequenceNumber);
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
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions) throws DataStorageManagerException {
        if (sequenceNumber.isStartOfTime() && !transactions.isEmpty()) {
            throw new DataStorageManagerException("impossible to write a non empty transactions list at start-of-time");
        }
        Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
        try {
            Files.createDirectories(tableSpaceDirectory);
            Path checkPointFile = getTablespaceTransactionsFile(tableSpace, sequenceNumber);
            Path parent = getParent(checkPointFile);
            Files.createDirectories(parent);

            Path checkpointFileTemp = parent.resolve(checkPointFile.getFileName() + ".tmp");
            LOGGER.log(Level.FINE, "writeTransactionsAtCheckpoint for tableSpace {0} sequenceNumber {1} to {2}, active transactions {3}", new Object[]{tableSpace, sequenceNumber, checkPointFile.toAbsolutePath().toString(), transactions.size()});
            try (ManagedFile file = ManagedFile.open(checkpointFileTemp, requirefsync);
                 SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE);
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
                file.sync();
            } catch (IOException err) {
                throw new DataStorageManagerException(err);
            }

            try {
                Files.move(checkpointFileTemp, checkPointFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException err) {
                throw new DataStorageManagerException(err);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Collection<PostCheckpointAction> result = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableSpaceDirectory)) {
            for (Path p : stream) {
                if (isTransactionsFile(p)) {
                    try {
                        LogSequenceNumber logPositionInFile = readLogSequenceNumberFromTransactionsFile(tableSpace, p);
                        if (sequenceNumber.after(logPositionInFile)) {
                            LOGGER.log(Level.FINEST, "transactions metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                            result.add(new DeleteFileAction("transactions", "delete transactions file " + p.toAbsolutePath(), p));
                        }
                    } catch (DataStorageManagerException ignore) {
                        LOGGER.log(Level.SEVERE, "Unparsable transactions file " + p.toAbsolutePath(), ignore);
                        result.add(new DeleteFileAction("transactions", "delete unparsable transactions file " + p.toAbsolutePath(), p));
                    }
                }
            }
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Could not list dir " + tableSpaceDirectory, err);
        }
        return result;
    }

        
    private class DeleteZNodeAction extends PostCheckpointAction {

        private final String znode;

        public DeleteZNodeAction(String tableName, String description, String znode) {
            super(tableName, description);
            this.znode = znode;
        }

        @Override
        public void run() {
            try {
                LOGGER.log(Level.FINE, description);
                zk.ensureZooKeeper().delete(znode, -1);
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.SEVERE, "Could not delete znode " + znode + ":" + err, err);
            } catch (IOException | KeeperException err) {
                LOGGER.log(Level.SEVERE, "Could not delete znode " + znode + ":" + err, err);
            }
        }
    }
        
    private class DropLedgerAction extends PostCheckpointAction {

        private final long ledgerId;

        public DropLedgerAction(String tableName, String description, long ledgerId) {
            super(tableName, description);
            this.ledgerId = ledgerId;
        }

        @Override
        public void run() {
            try {
                LOGGER.log(Level.FINE, description);
                FutureUtils.result(bk.getBookKeeper()
                        .newDeleteLedgerOp()
                        .withLedgerId(ledgerId)
                        .execute(), BKException.HANDLER);
            } catch (BKException err) {
                LOGGER.log(Level.SEVERE, "Could not delete ledger " + ledgerId + ":" + err, err);
            }
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
