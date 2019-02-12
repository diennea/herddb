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
package herddb.file;

import java.io.BufferedInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

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
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.ManagedFile;
import herddb.utils.ODirectFileInputStream;
import herddb.utils.ODirectFileOutputStream;
import herddb.utils.OpenFileUtils;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.SystemProperties;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import io.netty.util.Recycler;

/**
 * Data Storage on local filesystem
 *
 * @author enrico.olivelli
 */
public class FileDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(FileDataStorageManager.class.getName());
    private final Path baseDirectory;
    private final Path tmpDirectory;
    private final int swapThreshold;
    private final boolean requirefsync;
    private final boolean pageodirect;
    private final boolean indexodirect;
    private final StatsLogger logger;
    private final OpStatsLogger dataPageReads;
    private final OpStatsLogger dataPageWrites;
    private final OpStatsLogger indexPageReads;
    private final OpStatsLogger indexPageWrites;

    public static final String FILEEXTENSION_PAGE = ".page";

    /**
     * Standard buffer size for data copies
     */
    public static final int COPY_BUFFERS_SIZE
            = SystemProperties.getIntSystemProperty("herddb.file.copybuffersize", 64 * 1024);

    /**
     * Standard blocks batch number for o_direct procedures
     */
    public static final int O_DIRECT_BLOCK_BATCH
            = SystemProperties.getIntSystemProperty("herddb.file.odirectblockbatch", 16);

    public FileDataStorageManager(Path baseDirectory) {
        this(baseDirectory, baseDirectory.resolve("tmp"),
                ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT,
                ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT,
                ServerConfiguration.PROPERTY_PAGE_USE_ODIRECT_DEFAULT,
                ServerConfiguration.PROPERTY_INDEX_USE_ODIRECT_DEFAULT,
                new NullStatsLogger());
    }

    public FileDataStorageManager(Path baseDirectory, Path tmpDirectory, int swapThreshold,
            boolean requirefsync, boolean pageodirect, boolean indexodirect, StatsLogger logger) {
        this.baseDirectory = baseDirectory;
        this.tmpDirectory = tmpDirectory;
        this.swapThreshold = swapThreshold;
        this.logger = logger;
        this.requirefsync = requirefsync;
        this.pageodirect = pageodirect && OpenFileUtils.isO_DIRECT_Supported();
        this.indexodirect = indexodirect && OpenFileUtils.isO_DIRECT_Supported();
        StatsLogger scope = logger.scope("filedatastore");
        this.dataPageReads = scope.getOpStatsLogger("data_pagereads");
        this.dataPageWrites = scope.getOpStatsLogger("data_pagewrites");
        this.indexPageReads = scope.getOpStatsLogger("index_pagereads");
        this.indexPageWrites = scope.getOpStatsLogger("index_pagewrites");
    }

    @Override
    public void start() throws DataStorageManagerException {
        try {
            LOGGER.log(Level.SEVERE, "ensuring directory {0}", baseDirectory.toAbsolutePath().toString());
            Files.createDirectories(baseDirectory);
            LOGGER.log(Level.SEVERE, "preparing tmp directory {0}", tmpDirectory.toAbsolutePath().toString());
            FileUtils.cleanDirectory(tmpDirectory);
            Files.createDirectories(tmpDirectory);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "cleaning tmp directory {0}", tmpDirectory.toAbsolutePath().toString());
        try {
            FileUtils.cleanDirectory(tmpDirectory);
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Cannot clean tmp directory", err);
        }
    }

    private Path getTablespaceDirectory(String tablespace) {
        return baseDirectory.resolve(tablespace + ".tablespace");
    }

    private Path getTablespaceCheckPointInfoFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("checkpoint." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + EXTENSION_TABLEORINDExCHECKPOINTINFOFILE);
    }
    public static final String EXTENSION_TABLEORINDExCHECKPOINTINFOFILE = ".checkpoint";

    private static boolean isTablespaceCheckPointInfoFile(Path path) {
        Path filename = path.getFileName();

        if (filename == null) {
            return false;
        }

        final String name = filename.toString();
        return (name.startsWith("checkpoint.") && name.endsWith(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE))
                || name.equals(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE); // legacy 1.0 file
    }

    private Path getTablespaceTablesMetadataFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("tables." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tablesmetadata");
    }

    private static boolean isTablespaceTablesMetadataFile(Path path) {
        Path filename = path.getFileName();
        return filename != null && filename.toString().startsWith("tables.")
                && filename.toString().endsWith(".tablesmetadata");
    }

    private static boolean isTablespaceIndexesMetadataFile(Path path) {
        Path filename = path.getFileName();
        return filename != null && filename.toString().startsWith("indexes.")
                && filename.toString().endsWith(".tablesmetadata");
    }

    private Path getTablespaceIndexesMetadataFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("indexes." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tablesmetadata");
    }

    private Path getTablespaceTransactionsFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("transactions." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tx");
    }

    private static boolean isTransactionsFile(Path path) {
        Path filename = path.getFileName();
        return filename != null && filename.toString().startsWith("transactions.")
                && filename.toString().endsWith(".tx");
    }

    private Path getTableDirectory(String tableSpace, String tablename) {
        return getTablespaceDirectory(tableSpace).resolve(tablename + ".table");
    }

    private Path getIndexDirectory(String tableSpace, String indexname) {
        return getTablespaceDirectory(tableSpace).resolve(indexname + ".index");
    }

    private static Path getPageFile(Path tableDirectory, Long pageId) {
        return tableDirectory.resolve(pageId + FILEEXTENSION_PAGE);
    }

    private static Path getTableCheckPointsFile(Path tableDirectory, LogSequenceNumber sequenceNumber) {
        return tableDirectory.resolve(sequenceNumber.ledgerId + "." + sequenceNumber.offset + EXTENSION_TABLEORINDExCHECKPOINTINFOFILE);
    }

    private static boolean isTableOrIndexCheckpointsFile(Path path) {
        Path filename = path.getFileName();
        return filename != null && filename.toString().endsWith(EXTENSION_TABLEORINDExCHECKPOINTINFOFILE);
    }

    @Override
    public List<Record> readPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException, DataPageDoesNotExistException {
        long _start = System.currentTimeMillis();
        Path tableDir = getTableDirectory(tableSpace, tableName);
        Path pageFile = getPageFile(tableDir, pageId);
        List<Record> result;
        try {
            if (pageodirect) {
                try (ODirectFileInputStream odirect = new ODirectFileInputStream(pageFile, O_DIRECT_BLOCK_BATCH)) {
                    result = rawReadDataPage(pageFile, odirect);
                }
            } else {
                try (InputStream input = Files.newInputStream(pageFile);
                        BufferedInputStream buffer = new BufferedInputStream(input, COPY_BUFFERS_SIZE);) {
                    result = rawReadDataPage(pageFile, buffer);
                }
            }
        } catch (NoSuchFileException nsfe) {
            throw new DataPageDoesNotExistException("No such page: " + tableSpace + "_" + tableName + "." + pageId, nsfe);
        } catch (IOException err) {
            throw new DataStorageManagerException("error reading data page: " + tableSpace + "_" + tableName + "." + pageId, err);
        }
        long _stop = System.currentTimeMillis();
        long delta = _stop - _start;
        LOGGER.log(Level.FINE, "readPage {0}.{1} {2} ms", new Object[]{tableSpace, tableName, delta + ""});
        dataPageReads.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
        return result;
    }

    private static List<Record> rawReadDataPage(Path pageFile, InputStream stream) throws IOException, DataStorageManagerException {
        int size = (int) Files.size(pageFile);
        byte[] dataPage = new byte[size];
        int read = stream.read(dataPage);
        if (read != size) {
            throw new IOException("short read, read " + read + " instead of " + size + " bytes from " + pageFile);
        }
        try (ByteArrayCursor dataIn = ByteArrayCursor.wrap(dataPage)) {
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted data file " + pageFile.toAbsolutePath());
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
                throw new DataStorageManagerException("Corrupted datafile " + pageFile + ". Bad hash " + hashFromFile + " <> " + hashFromDigest);
            }
            return result;
        }
    }

    public static List<Record> rawReadDataPage(Path pageFile) throws DataStorageManagerException,
            NoSuchFileException, IOException {
        List<Record> result;
        long hashFromFile;
        long hashFromDigest;
        try (ODirectFileInputStream odirect = new ODirectFileInputStream(pageFile, O_DIRECT_BLOCK_BATCH);
                XXHash64Utils.HashingStream hash = new XXHash64Utils.HashingStream(odirect);
                ExtendedDataInputStream dataIn = new ExtendedDataInputStream(hash)) {
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted data file " + pageFile.toAbsolutePath());
            }
            int numRecords = dataIn.readInt();
            result = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                Bytes key = dataIn.readBytes();
                Bytes value = dataIn.readBytes();
                result.add(new Record(key, value));
            }
            hashFromDigest = hash.hash();
            hashFromFile = dataIn.readLong();
        }
        if (hashFromDigest != hashFromFile) {
            throw new DataStorageManagerException("Corrupted datafile " + pageFile + ". Bad hash " + hashFromFile + " <> " + hashFromDigest);
        }
        return result;
    }

    private static <X> X readIndexPage(DataReader<X> reader, Path pageFile, InputStream stream) throws IOException, DataStorageManagerException {
        int size = (int) Files.size(pageFile);
        byte[] dataPage = new byte[size];
        int read = stream.read(dataPage);
        if (read != size) {
            throw new IOException("short read, read " + read + " instead of " + size + " bytes from " + pageFile);
        }
        try (ByteArrayCursor dataIn = ByteArrayCursor.wrap(dataPage)) {
            /*
             * When writing with O_DIRECT this stream will be zero padded at the end. It isn't a problem: reader
             * must already handle his stop condition without reading file till the end because it contains an
             * hash after data end that must not be read by the reader.
             */
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted data file " + pageFile.toAbsolutePath());
            }
            X result = reader.read(dataIn);
            int pos = dataIn.getPosition();
            long hashFromFile = dataIn.readLong();
            // after the hash we will have zeroes or garbage
            // the hash is not at the end of file, but after data
            long hashFromDigest = XXHash64Utils.hash(dataPage, 0, pos);
            if (hashFromDigest != hashFromFile) {
                throw new DataStorageManagerException("Corrupted datafile " + pageFile + ". Bad hash " + hashFromFile + " <> " + hashFromDigest);
            }
            return result;
        }
    }

    @Override
    public <X> X readIndexPage(String tableSpace, String indexName, Long pageId, DataReader<X> reader) throws DataStorageManagerException {
        Path tableDir = getIndexDirectory(tableSpace, indexName);
        Path pageFile = getPageFile(tableDir, pageId);
        long _start = System.currentTimeMillis();
        X result;
        try {
            if (indexodirect) {
                try (ODirectFileInputStream odirect = new ODirectFileInputStream(pageFile, O_DIRECT_BLOCK_BATCH)) {
                    result = readIndexPage(reader, pageFile, odirect);
                }
            } else {
                try (InputStream input = Files.newInputStream(pageFile);
                        BufferedInputStream buffer = new BufferedInputStream(input, COPY_BUFFERS_SIZE)) {
                    result = readIndexPage(reader, pageFile, buffer);
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        long _stop = System.currentTimeMillis();
        long delta = _stop - _start;
        LOGGER.log(Level.FINE, "readIndexPage {0}.{1} {2} ms", new Object[]{tableSpace, indexName, delta + ""});
        indexPageReads.registerSuccessfulEvent(delta, TimeUnit.MILLISECONDS);
        return result;
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
        LOGGER.log(Level.FINER, "fullTableScan table " + tableSpace + "." + tableUuid + ", status: " + status);
        consumer.acceptTableStatus(status);
        List<Long> activePages = new ArrayList<>(status.activePages.keySet());
        activePages.sort(null);
        for (long idpage : activePages) {
            List<Record> records = readPage(tableSpace, tableUuid, idpage);
            consumer.startPage(idpage);
            LOGGER.log(Level.FINER, "fullTableScan table " + tableSpace + "." + tableUuid + ", page " + idpage + ", contains " + records.size() + " records");
            for (Record record : records) {
                consumer.acceptRecord(record);
            }
            consumer.endPage();
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

        Path dir = getIndexDirectory(tableSpace, indexName);
        Path checkpointFile = getTableCheckPointsFile(dir, sequenceNumber);

        if (!Files.exists(checkpointFile)) {
            throw new DataStorageManagerException("no such index checkpoint: " + checkpointFile);
        }

        return readIndexStatusFromFile(checkpointFile);
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String tableUuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        try {

            Path dir = getTableDirectory(tableSpace, tableUuid);
            Path checkpointFile = getTableCheckPointsFile(dir, sequenceNumber);

            if (!Files.exists(checkpointFile)) {
                throw new DataStorageManagerException("no such table checkpoint: " + checkpointFile);
            }

            return readTableStatusFromFile(checkpointFile);

        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String tableName) throws DataStorageManagerException {
        try {
            Path lastFile = getLastTableCheckpointFile(tableSpace, tableName);
            TableStatus latestStatus;
            if (lastFile == null) {
                latestStatus = new TableStatus(tableName, LogSequenceNumber.START_OF_TIME,
                        Bytes.longToByteArray(1), 1, Collections.emptyMap());
            } else {
                latestStatus = readTableStatusFromFile(lastFile);
            }
            return latestStatus;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    public static TableStatus readTableStatusFromFile(Path checkpointsFile) throws IOException {
        byte[] fileContent = FileUtils.fastReadFile(checkpointsFile);
        XXHash64Utils.verifyBlockWithFooter(fileContent, 0, fileContent.length);
        try (InputStream input = new SimpleByteArrayInputStream(fileContent);
                ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
            long version = dataIn.readVLong(); // version
            long flags = dataIn.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted table status file " + checkpointsFile.toAbsolutePath());
            }
            return TableStatus.deserialize(dataIn);
        }
    }

    private Path getLastTableCheckpointFile(String tableSpace, String tableName) throws IOException {
        Path dir = getTableDirectory(tableSpace, tableName);
        Path result = getMostRecentCheckPointFile(dir);
        return result;
    }

    private Path getMostRecentCheckPointFile(Path dir) throws IOException {
        Path result = null;
        long lastMod = -1;
        Files.createDirectories(dir);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream) {
                if (isTableOrIndexCheckpointsFile(path)) {
                    LOGGER.log(Level.FINER, "getMostRecentCheckPointFile on " + dir.toAbsolutePath() + " -> ACCEPT " + path);
                    FileTime lastModifiedTime = Files.getLastModifiedTime(path);
                    long ts = lastModifiedTime.toMillis();
                    if (lastMod < 0 || lastMod < ts) {
                        result = path;
                        lastMod = ts;
                    }
                } else {
                    LOGGER.log(Level.FINER, "getMostRecentCheckPointFile on " + dir.toAbsolutePath() + " -> SKIP " + path);
                }
            }
        }
        LOGGER.log(Level.FINER, "getMostRecentCheckPointFile on " + dir.toAbsolutePath() + " -> " + result);
        return result;
    }

    public static IndexStatus readIndexStatusFromFile(Path checkpointsFile) throws DataStorageManagerException {
        try {
            byte[] fileContent = FileUtils.fastReadFile(checkpointsFile);
            XXHash64Utils.verifyBlockWithFooter(fileContent, 0, fileContent.length);
            try (InputStream input = new SimpleByteArrayInputStream(fileContent);
                    ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
                long version = dataIn.readVLong(); // version
                long flags = dataIn.readVLong(); // flags for future implementations
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted index status file " + checkpointsFile.toAbsolutePath());
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
        Path dir = getTableDirectory(tableSpace, tableName);
        Path checkpointFile = getTableCheckPointsFile(dir, logPosition);
        try {
            Files.createDirectories(dir);
            if (Files.isRegularFile(checkpointFile)) {
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

        Path parent = getParent(checkpointFile);
        Path checkpointFileTemp = parent.resolve(checkpointFile.getFileName() + ".tmp");
        LOGGER.log(Level.FINE, "tableCheckpoint " + tableSpace + ", " + tableName + ": " + tableStatus + " (pin:" + pin + ") to file " + checkpointFile);

        try (ManagedFile file = ManagedFile.open(checkpointFileTemp, requirefsync);
                SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE);
                XXHash64Utils.HashingOutputStream oo = new XXHash64Utils.HashingOutputStream(buffer);
                ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(oo)) {

            dataOutputKeys.writeVLong(1); // version
            dataOutputKeys.writeVLong(0); // flags for future implementations
            tableStatus.serialize(dataOutputKeys);

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
        final Map<Long, Integer> pins = pinTableAndGetPages(tableSpace, tableName, tableStatus, pin);
        final Set<LogSequenceNumber> checkpoints = pinTableAndGetCheckpoints(tableSpace, tableName, tableStatus, pin);

        long maxPageId = tableStatus.activePages.keySet().stream().max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        List<PostCheckpointAction> result = new ArrayList<>();
        // we can drop old page files now
        List<Path> pageFiles = getTablePageFiles(tableSpace, tableName);
        for (Path p : pageFiles) {
            long pageId = getPageId(p);
            LOGGER.log(Level.FINEST, "checkpoint file {0} pageId {1}", new Object[]{p.toAbsolutePath(), pageId});
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !tableStatus.activePages.containsKey(pageId)
                    && pageId < maxPageId) {
                LOGGER.log(Level.FINEST, "checkpoint file " + p.toAbsolutePath() + " pageId " + pageId + ". will be deleted after checkpoint end");
                result.add(new DeleteFileAction(tableName, "delete page " + pageId + " file " + p.toAbsolutePath(), p));
            }
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                if (isTableOrIndexCheckpointsFile(p) && !p.equals(checkpointFile)) {
                    TableStatus status = readTableStatusFromFile(p);
                    if (logPosition.after(status.sequenceNumber) && !checkpoints.contains(status.sequenceNumber)) {
                        LOGGER.log(Level.FINEST, "checkpoint metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                        result.add(new DeleteFileAction(tableName, "delete checkpoint metadata file " + p.toAbsolutePath(), p));
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
        Path dir = getIndexDirectory(tableSpace, indexName);
        LogSequenceNumber logPosition = indexStatus.sequenceNumber;
        Path checkpointFile = getTableCheckPointsFile(dir, logPosition);
        Path parent = getParent(checkpointFile);
        Path checkpointFileTemp = parent.resolve(checkpointFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(dir);
            if (Files.isRegularFile(checkpointFile)) {
                IndexStatus actualStatus = readIndexStatusFromFile(checkpointFile);
                if (actualStatus != null && actualStatus.equals(indexStatus)) {
                    LOGGER.log(Level.SEVERE, "indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " already saved on" + checkpointFile);
                    return Collections.emptyList();
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
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

    /**
     * Returns a {@link Path} parent
     *
     * @param file path from which lookup parent
     * @return parent path
     * @throws DataStorageManagerException if no parent cannot be resolved (even
     * checking absolute Path)
     */
    private static Path getParent(Path file) throws DataStorageManagerException {

        final Path path = file.getParent();

        if (path != null) {
            return path;
        }

        if (file.isAbsolute()) {
            throw new DataStorageManagerException("Invalid path " + file);
        }

        try {

            return getParent(file.toAbsolutePath());

        } catch (IOError | SecurityException e) {

            throw new DataStorageManagerException("Invalid path " + file);
        }

    }

    private static long getPageId(Path p) {
        String filename = p.getFileName() + "";
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

    private static boolean isPageFile(Path path) {
        return getPageId(path) >= 0;
    }

    public List<Path> getTablePageFiles(String tableSpace, String tableName) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

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

    public List<Path> getIndexPageFiles(String tableSpace, String indexName) throws DataStorageManagerException {
        Path indexDir = getIndexDirectory(tableSpace, indexName);
        try {
            Files.createDirectories(indexDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

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
                LOGGER.log(Level.SEVERE, "cleanupAfterBoot file " + p.toAbsolutePath() + " pageId " + pageId + ". will be deleted");
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
     * @param file managed file used for sync operations
     * @param stream output stream related to given managed file for write
     * operations
     * @return
     * @throws IOException
     */
    private static long writePage(Collection<Record> newPage, ManagedFile file, OutputStream stream) throws IOException {

        try (RecyclableByteArrayOutputStream oo = getWriteBuffer();
                ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo);) {

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
            file.sync();
            return oo.size();
        }

    }

    @Override
    public void writePage(String tableSpace, String tableName, long pageId, Collection<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long _start = System.currentTimeMillis();
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        long size;

        try {
            if (pageodirect) {
                try (ODirectFileOutputStream odirect = new ODirectFileOutputStream(pageFile, O_DIRECT_BLOCK_BATCH,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        ManagedFile file = ManagedFile.open(odirect.getFc(), requirefsync)) {

                    size = writePage(newPage, file, odirect);
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
                ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo);) {
            dataOutput.writeVLong(1); // version
            dataOutput.writeVLong(0); // flags for future implementations
            writer.write(dataOutput);
            dataOutput.flush();
            long hash = XXHash64Utils.hash(oo.getBuffer(), 0, oo.size());
            dataOutput.writeLong(hash);
            dataOutput.flush();
            stream.write(oo.getBuffer(), 0, oo.size());
            file.sync();
            return oo.size();
        }
    }

    @Override
    public void writeIndexPage(String tableSpace, String indexName,
            long pageId, DataWriter writer) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        Path tableDir = getIndexDirectory(tableSpace, indexName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        long size;
        try {
            if (indexodirect) {
                try (ODirectFileOutputStream odirect = new ODirectFileOutputStream(pageFile, O_DIRECT_BLOCK_BATCH,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        ManagedFile file = ManagedFile.open(odirect.getFc(), requirefsync)) {

                    size = writeIndexPage(writer, file, odirect);
                }

            } else {
                try (ManagedFile file = ManagedFile.open(pageFile, requirefsync,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(), COPY_BUFFERS_SIZE)) {

                    size = writeIndexPage(writer, file, buffer);
                }
            }

        } catch (IOException err) {
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
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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
            LOGGER.log(Level.SEVERE, "loadTables for tableSpace " + tableSpace + " from " + file.toAbsolutePath().toString() + ", sequenceNumber:" + sequenceNumber);
            if (!Files.isRegularFile(file)) {
                if (sequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.SEVERE, "file " + file.toAbsolutePath().toString() + " not found");
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
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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
            LOGGER.log(Level.SEVERE, "loadIndexes for tableSpace " + tableSpace + " from " + file.toAbsolutePath().toString() + ", sequenceNumber:" + sequenceNumber);
            if (!Files.isRegularFile(file)) {
                if (sequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.SEVERE, "file " + file.toAbsolutePath().toString() + " not found");
                    return Collections.emptyList();
                } else {
                    throw new DataStorageManagerException("local index data not available for tableSpace " + tableSpace + ", recovering from sequenceNumber " + sequenceNumber);
                }
            }
            try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
                    ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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
    public Collection<PostCheckpointAction> writeTables(String tableSpace, LogSequenceNumber sequenceNumber,
            List<Table> tables, List<Index> indexlist) throws DataStorageManagerException {
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
        LOGGER.log(Level.SEVERE, "dropTable {0}.{1}", new Object[]{tablespace, tableName});
        Path tableDir = getTableDirectory(tablespace, tableName);
        try {
            deleteDirectory(tableDir);
        } catch (IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    @Override
    public void dropIndex(String tablespace, String name) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "dropIndex {0}.{1}", new Object[]{tablespace, name});
        Path tableDir = getIndexDirectory(tablespace, name);
        try {
            deleteDirectory(tableDir);
        } catch (IOException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    private static LogSequenceNumber readLogSequenceNumberFromCheckpointInfoFile(String tableSpace, Path checkPointFile) throws DataStorageManagerException, IOException {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(checkPointFile, StandardOpenOption.READ), 4 * 1024 * 1024);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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

    private static class FileDeleter extends SimpleFileVisitor<Path> {

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
//            println("delete file " + file.toAbsolutePath());
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
//            println("delete directory " + dir);
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
        }
    }

    public static void deleteDirectory(Path f) throws IOException {
        if (Files.isDirectory(f)) {
            Files.walkFileTree(f, new FileDeleter());
            Files.deleteIfExists(f);
        } else if (Files.isRegularFile(f)) {
            throw new IOException("name " + f.toAbsolutePath() + " is not a directory");
        }
    }

    @Override
    public KeyToPageIndex createKeyToPageMap(String tablespace, String name, MemoryManager memoryManager) throws DataStorageManagerException {

        return new BLinkKeyToPageIndex(tablespace, name, memoryManager, this);

//        return new ConcurrentMapKeyToPageIndex(new ConcurrentHashMap<>());
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
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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
                    ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
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

    private static class DeleteFileAction extends PostCheckpointAction {

        private final Path p;

        public DeleteFileAction(String tableName, String description, Path p) {
            super(tableName, description);
            this.p = p;
        }

        @Override
        public void run() {
            try {
                LOGGER.log(Level.FINE, description);
                Files.deleteIfExists(p);
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Could not delete file " + p.toAbsolutePath() + ":" + err, err);
            }
        }
    }

    private static Recycler<RecyclableByteArrayOutputStream> WRITE_BUFFERS_RECYCLER = new Recycler<RecyclableByteArrayOutputStream>() {

        @Override
        protected RecyclableByteArrayOutputStream newObject(
                Recycler.Handle<RecyclableByteArrayOutputStream> handle) {
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
    private static final class RecyclableByteArrayOutputStream extends VisibleByteArrayOutputStream {

        private final static int DEFAULT_INITIAL_SIZE = 1024 * 1024;
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
