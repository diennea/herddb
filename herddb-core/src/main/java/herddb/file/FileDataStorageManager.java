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

import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.index.ConcurrentMapKeyToPageIndex;
import herddb.index.KeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullIndexScanConsumer;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public FileDataStorageManager(Path baseDirectory) {
        this(baseDirectory, baseDirectory.resolve("tmp"), ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
    }

    public FileDataStorageManager(Path baseDirectory, Path tmpDirectory, int swapThreshold) {
        this.baseDirectory = baseDirectory;
        this.tmpDirectory = tmpDirectory;
        this.swapThreshold = swapThreshold;
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

    private Path getTablespaceCheckPointInfoFile(String tablespace) {
        return getTablespaceDirectory(tablespace).resolve(".checkpoint");
    }

    private Path getTablespaceTablesMetadataFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("tables." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tablesmetadata");
    }

    private Path getTablespaceIndexesMetadataFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("indexes." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tablesmetadata");
    }

    private Path getTablespaceTransactionsFile(String tablespace, LogSequenceNumber sequenceNumber) {
        return getTablespaceDirectory(tablespace).resolve("transactions." + sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".tx");
    }

    private Path getTableDirectory(String tableSpace, String tablename) {
        return getTablespaceDirectory(tableSpace).resolve(tablename + ".table");
    }

    private Path getIndexDirectory(String tableSpace, String indexname) {
        return getTablespaceDirectory(tableSpace).resolve(indexname + ".index");
    }

    private Path getPageFile(Path tableDirectory, Long pageId) {
        return tableDirectory.resolve(pageId + ".page");
    }

    private Path getCheckPointsFile(Path tableDirectory, LogSequenceNumber sequenceNumber) {
        return tableDirectory.resolve(sequenceNumber.ledgerId + "." + sequenceNumber.offset + ".checkpoint");
    }

    private boolean isCheckpointsFile(Path path) {
        return path.getFileName().toString().endsWith(".checkpoint");
    }

    @Override
    public List<Record> readPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        Path pageFile = getPageFile(tableDir, pageId);
        byte[] pageData;
        try {
            pageData = FileUtils.fastReadFile(pageFile);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        boolean okHash = XXHash64Utils.verifyBlockWithFooter(pageData, 0, pageData.length);
        if (!okHash) {
            throw new DataStorageManagerException("corrutped data file " + pageFile.toAbsolutePath() + ", checksum failed");
        }
        List<Record> result;
        try (InputStream input = new ByteArrayInputStream(pageData);
            ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
            int flags = dataIn.readVInt(); // flags for future implementations
            if (flags != 0) {
                throw new DataStorageManagerException("corrupted data file " + pageFile.toAbsolutePath());
            }
            int numRecords = dataIn.readInt();
            result = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                byte[] key = dataIn.readArray();
                byte[] value = dataIn.readArray();
                result.add(new Record(new Bytes(key), new Bytes(value)));
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        return result;
    }

    @Override
    public byte[] readIndexPage(String tableSpace, String indexName, Long pageId) throws DataStorageManagerException {
        Path tableDir = getIndexDirectory(tableSpace, indexName);
        Path pageFile = getPageFile(tableDir, pageId);
        byte[] pageData;
        try {
            pageData = FileUtils.fastReadFile(pageFile);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        boolean okHash = XXHash64Utils.verifyBlockWithFooter(pageData, 0, pageData.length);
        if (!okHash) {
            throw new DataStorageManagerException("corrutped data file " + pageFile.toAbsolutePath() + ", checksum failed");
        }
        try (InputStream input = new ByteArrayInputStream(pageData);
            ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
            int flags = dataIn.readVInt(); // flags for future implementations
            if (flags != 0) {
                throw new DataStorageManagerException("corrupted data file " + pageFile.toAbsolutePath());
            }
            byte[] data = dataIn.readArray();
            return data;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void fullIndexScan(String tableSpace, String indexName, FullIndexScanConsumer consumer) throws DataStorageManagerException {
        try {
            Path lastFile = getLastIndexCheckpointFile(tableSpace, indexName);
            IndexStatus latestStatus;
            if (lastFile == null) {
                latestStatus = new IndexStatus(indexName, LogSequenceNumber.START_OF_TIME, null, null);
            } else {
                latestStatus = readIndexStatusFromFile(lastFile);
            }
            LOGGER.log(Level.SEVERE, "fullIndexScan index " + tableSpace + "." + indexName + ", status: " + latestStatus);
            consumer.acceptIndexStatus(latestStatus);

            List<Long> activePages = new ArrayList<>(latestStatus.activePages);
            activePages.sort(null);
            for (long idpage : activePages) {
                byte[] records = readIndexPage(tableSpace, indexName, idpage);
                consumer.acceptPage(idpage, records);
                LOGGER.log(Level.SEVERE, "fullIndexScan index " + tableSpace + "." + indexName + ", page " + idpage + ", " + records.length + " bytes");
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void fullTableScan(String tableSpace, String tableName, FullTableScanConsumer consumer) throws DataStorageManagerException {
        try {
            Path lastFile = getLastTableCheckpointFile(tableSpace, tableName);
            TableStatus latestStatus;
            if (lastFile == null) {
                latestStatus = new TableStatus(tableName, LogSequenceNumber.START_OF_TIME, Bytes.from_long(1).data, 1, new HashSet<>());
            } else {
                latestStatus = readTableStatusFromFile(lastFile);
            }

            LOGGER.log(Level.SEVERE, "fullTableScan table " + tableSpace + "." + tableName + ", status: " + latestStatus);
            consumer.acceptTableStatus(latestStatus);
            List<Long> activePages = new ArrayList<>(latestStatus.activePages);
            activePages.sort(null);
            for (long idpage : activePages) {
                List<Record> records = readPage(tableSpace, tableName, idpage);
                consumer.startPage(idpage);
                LOGGER.log(Level.SEVERE, "fullTableScan table " + tableSpace + "." + tableName + ", page " + idpage + ", contains " + records.size() + " records");
                for (Record record : records) {
                    consumer.acceptRecord(record);
                }
                consumer.endPage();
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

    }

    private TableStatus readTableStatus(String tableSpace, String tableName, LogSequenceNumber logPosition) throws DataStorageManagerException {
        Path dir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(dir);
            Path checkpointsFile = getCheckPointsFile(dir, logPosition);
            LOGGER.log(Level.FINER, "readActualTableStatus " + tableSpace + "." + tableName + " at " + logPosition + " from " + checkpointsFile);
            return readTableStatusFromFile(checkpointsFile);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private TableStatus readTableStatusFromFile(Path checkpointsFile) throws IOException {
        byte[] fileContent = FileUtils.fastReadFile(checkpointsFile);
        XXHash64Utils.verifyBlockWithFooter(fileContent, 0, fileContent.length);
        try (InputStream input = new ByteArrayInputStream(fileContent);
            ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
            return TableStatus.deserialize(dataIn);
        }
    }

    private Path getLastIndexCheckpointFile(String tableSpace, String indexName) throws IOException {
        Path dir = getIndexDirectory(tableSpace, indexName);
        Path result = getMostRecentCheckPointFile(dir);
        return result;
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
                if (isCheckpointsFile(path)) {
                    LOGGER.log(Level.SEVERE, "getMostRecentCheckPointFile on " + dir.toAbsolutePath() + " -> ACCEPT " + path);
                    FileTime lastModifiedTime = Files.getLastModifiedTime(path);
                    long ts = lastModifiedTime.toMillis();
                    if (lastMod < 0 || lastMod < ts) {
                        result = path;
                        lastMod = ts;
                    }
                } else {
                    LOGGER.log(Level.SEVERE, "getMostRecentCheckPointFile on " + dir.toAbsolutePath() + " -> SKIP " + path);
                }
            }
        }
        LOGGER.log(Level.SEVERE, "getMostRecentCheckPointFile on " + dir.toAbsolutePath() + " -> " + result);
        return result;
    }

    private IndexStatus readIndexStatus(String tableSpace, String indexName, LogSequenceNumber logPosition) throws DataStorageManagerException {
        Path dir = getIndexDirectory(tableSpace, indexName);
        try {
            Files.createDirectories(dir);
            Path checkpointsFile = getCheckPointsFile(dir, logPosition);
            LOGGER.log(Level.FINER, "readIndexStatus " + tableSpace + "." + indexName + " at " + indexName + " from " + checkpointsFile);
            return readIndexStatusFromFile(checkpointsFile);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private IndexStatus readIndexStatusFromFile(Path checkpointsFile) throws DataStorageManagerException {
        try {
            byte[] fileContent = FileUtils.fastReadFile(checkpointsFile);
            XXHash64Utils.verifyBlockWithFooter(fileContent, 0, fileContent.length);
            try (InputStream input = new ByteArrayInputStream(fileContent);
                ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
                return IndexStatus.deserialize(dataIn);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String tableName, TableStatus tableStatus) throws DataStorageManagerException {
        LogSequenceNumber logPosition = tableStatus.sequenceNumber;
        Path dir = getTableDirectory(tableSpace, tableName);
        Path checkpointFile = getCheckPointsFile(dir, logPosition);
        try {
            Files.createDirectories(dir);
            if (Files.isRegularFile(checkpointFile)) {
                TableStatus actualStatus = readTableStatusFromFile(checkpointFile);
                if (actualStatus != null && actualStatus.equals(tableStatus)) {
                    LOGGER.log(Level.SEVERE, Thread.currentThread().getName() + " tableCheckpoint " + tableSpace + ", " + tableName + ": " + tableStatus + " already saved on file " + checkpointFile);
                    return Collections.emptyList();
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path checkpointFileTemp = checkpointFile.getParent().resolve(checkpointFile.getFileName() + ".tmp");
        LOGGER.log(Level.SEVERE, Thread.currentThread().getName() + " tableCheckpoint " + tableSpace + ", " + tableName + ": " + tableStatus + " to file " + checkpointFile);

        VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1024);
        try (ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(oo)) {
            tableStatus.serialize(dataOutputKeys);
            dataOutputKeys.flush();
            oo.write(oo.xxhash64());
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        try {
            FileUtils.fastWriteFile(checkpointFileTemp, oo.getBuffer(), 0, oo.size());
            Files.move(checkpointFileTemp, checkpointFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        long maxPageId = tableStatus.activePages.stream().max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        List<PostCheckpointAction> result = new ArrayList<>();
        // we can drop old page files now
        List<Path> pageFiles = getTablePageFiles(tableSpace, tableName);
        for (Path p : pageFiles) {
            long pageId = getPageId(p);
            LOGGER.log(Level.FINEST, "checkpoint file {0} pageId {1}", new Object[]{p.toAbsolutePath(), pageId});
            if (pageId > 0
                && !tableStatus.activePages.contains(pageId)
                && pageId < maxPageId) {
                LOGGER.log(Level.FINEST, "checkpoint file " + p.toAbsolutePath() + " pageId " + pageId + ". will be deleted after checkpoint end");
                result.add(new PostCheckpointAction(tableName, "delete page " + pageId + " file " + p.toAbsolutePath()) {
                    @Override
                    public void run() {
                        try {
                            LOGGER.log(Level.SEVERE, "checkpoint table " + tableName + " file " + p.toAbsolutePath() + " delete pageId " + pageId);
                            Files.deleteIfExists(p);
                        } catch (IOException err) {
                            LOGGER.log(Level.SEVERE, "Could not delete file " + p.toAbsolutePath() + ":" + err, err);
                        }
                    }
                });
            }
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                if (isCheckpointsFile(p) && !p.equals(checkpointFile)) {
                    TableStatus status = readTableStatusFromFile(p);
                    if (logPosition.after(status.sequenceNumber)) {
                        LOGGER.log(Level.FINEST, "checkpoint metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                        result.add(new PostCheckpointAction(tableName, "delete checkpoint metadata file " + p.toAbsolutePath()) {
                            @Override
                            public void run() {
                                try {
                                    LOGGER.log(Level.SEVERE, "checkpoint table " + tableName + " metadata file " + p.toAbsolutePath() + " delete");
                                    Files.deleteIfExists(p);
                                } catch (IOException err) {
                                    LOGGER.log(Level.SEVERE, "Could not delete file " + p.toAbsolutePath() + ":" + err, err);
                                }
                            }
                        });
                    }
                }
            }
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Could not list table dir " + dir, err);
        }
        return result;
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String indexName, IndexStatus indexStatus) throws DataStorageManagerException {
        Path dir = getIndexDirectory(tableSpace, indexName);
        LogSequenceNumber logPosition = indexStatus.sequenceNumber;
        Path checkpointFile = getCheckPointsFile(dir, logPosition);
        Path checkpointFileTemp = checkpointFile.getParent().resolve(checkpointFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(dir);
            if (Files.isRegularFile(checkpointFile)) {
                IndexStatus actualStatus = readIndexStatusFromFile(checkpointFile);
                if (actualStatus != null && actualStatus.equals(indexStatus)) {
                    LOGGER.log(Level.SEVERE, Thread.currentThread().getName() + " indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " already saved on" + checkpointFile);
                    return Collections.emptyList();
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        LOGGER.log(Level.SEVERE, Thread.currentThread().getName() + " indexCheckpoint " + tableSpace + ", " + indexName + ": " + indexStatus + " to file " + checkpointFile);

        VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1024);
        try (ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(oo)) {
            indexStatus.serialize(dataOutputKeys);
            dataOutputKeys.flush();
            oo.write(oo.xxhash64());
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        try {
            FileUtils.fastWriteFile(checkpointFileTemp, oo.getBuffer(), 0, oo.size());
            Files.move(checkpointFileTemp, checkpointFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        List<PostCheckpointAction> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                if (isCheckpointsFile(p) && !p.equals(checkpointFile)) {
                    IndexStatus status = readIndexStatusFromFile(p);
                    if (logPosition.after(status.sequenceNumber)) {
                        LOGGER.log(Level.FINEST, "checkpoint metadata file " + p.toAbsolutePath() + ". will be deleted after checkpoint end");
                        result.add(new PostCheckpointAction(indexName, "delete checkpoint metadata file " + p.toAbsolutePath()) {
                            @Override
                            public void run() {
                                try {
                                    LOGGER.log(Level.SEVERE, "checkpoint index " + indexName + " metadata file " + p.toAbsolutePath() + " delete");
                                    Files.deleteIfExists(p);
                                } catch (IOException err) {
                                    LOGGER.log(Level.SEVERE, "Could not delete file " + p.toAbsolutePath() + ":" + err, err);
                                }
                            }
                        });
                    }
                }
            }
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "Could not list indexName dir " + dir, err);
        }

        return result;
    }

    private static long getPageId(Path p) {
        String filename = p.getFileName().toString();
        if (filename.endsWith(".page")) {
            try {
                return Long.parseLong(filename.substring(0, filename.length() - ".page".length()));
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

    @Override
    public void cleanupAfterBoot(String tableSpace, String tableName, Set<Long> activePagesAtBoot) throws DataStorageManagerException {
        // we have to drop old page files or page files partially written by checkpoint interrupted at JVM crash/reboot
        List<Path> pageFiles = getTablePageFiles(tableSpace, tableName);
        for (Path p : pageFiles) {
            long pageId = getPageId(p);
            LOGGER.log(Level.INFO, "cleanupAfterBoot file " + p.toAbsolutePath() + " pageId " + pageId);
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

    @Override
    public void writePage(String tableSpace, String tableName, long pageId, List<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long _start = System.currentTimeMillis();
        long _endhash;
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        int size;
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(10 * 1024 * 1024);) {
            try (ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo);) {
                dataOutput.writeVInt(0); // flags for future implementations
                dataOutput.writeInt(newPage.size());
                for (Record record : newPage) {
                    dataOutput.writeArray(record.key.data);
                    dataOutput.writeArray(record.value.data);
                }
            }
            byte[] digest = oo.xxhash64();
            _endhash = System.currentTimeMillis();

            // footer
            oo.write(digest);
            size = oo.size();

            FileUtils.fastWriteFile(pageFile, oo.getBuffer(), 0, oo.size());
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        long now = System.currentTimeMillis();
        LOGGER.log(Level.INFO,
            "writePage " + (size / 1024) + " KBytes," + newPage.size() + " records, "
            + "time " + (now - _start) + " ms (" + (now - _endhash) + " disk)");

    }

    @Override
    public void writeIndexPage(String tableSpace, String indexName, long pageId, byte[] page) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long _start = System.currentTimeMillis();
        long _endhash;
        Path tableDir = getIndexDirectory(tableSpace, indexName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        int size;
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(10 * 1024 * 1024);) {
            try (ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(oo);) {
                dataOutput.writeVInt(0); // flags for future implementations
                dataOutput.writeArray(page);
            }
            byte[] digest = oo.xxhash64();
            _endhash = System.currentTimeMillis();

            // footer
            oo.write(digest);
            size = oo.size();

            FileUtils.fastWriteFile(pageFile, oo.getBuffer(), 0, oo.size());
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        long now = System.currentTimeMillis();
        LOGGER.log(Level.INFO,
            "writeIndexPage " + indexName + " page " + pageId + " " + (size / 1024) + " KBytes, "
            + "time " + (now - _start) + " ms (" + (now - _endhash) + " disk)");
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String tableName) throws DataStorageManagerException {
        try {
            Path lastFile = getLastTableCheckpointFile(tableSpace, tableName);
            TableStatus latestStatus;
            if (lastFile == null) {
                latestStatus = new TableStatus(tableName, LogSequenceNumber.START_OF_TIME, Bytes.from_long(1).data, 1, new HashSet<>());
            } else {
                latestStatus = readTableStatusFromFile(lastFile);
            }
            return latestStatus.activePages.size();
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
            try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readLong();
                long offset = din.readLong();
                if (ledgerId != sequenceNumber.ledgerId || offset != sequenceNumber.offset) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for sequence number " + sequenceNumber);
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
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
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
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readLong();
                long offset = din.readLong();
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
    public void writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist) throws DataStorageManagerException {
        if (sequenceNumber.isStartOfTime() && !tables.isEmpty()) {
            throw new DataStorageManagerException("impossible to write a non empty table list at start-of-time");
        }
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file_tables = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
            Path file_indexes = getTablespaceIndexesMetadataFile(tableSpace, sequenceNumber);
            Files.createDirectories(file_tables.getParent());
            LOGGER.log(Level.SEVERE, "writeTables for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + file_tables.toAbsolutePath().toString());
            try (OutputStream out = Files.newOutputStream(file_tables, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(out)) {
                dout.writeUTF(tableSpace);
                dout.writeLong(sequenceNumber.ledgerId);
                dout.writeLong(sequenceNumber.offset);
                dout.writeInt(tables.size());
                for (Table t : tables) {
                    byte[] tableSerialized = t.serialize();
                    dout.writeArray(tableSerialized);
                }
            }
            try (OutputStream out = Files.newOutputStream(file_indexes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(out)) {
                dout.writeUTF(tableSpace);
                dout.writeLong(sequenceNumber.ledgerId);
                dout.writeLong(sequenceNumber.offset);
                if (indexlist != null) {
                    dout.writeInt(indexlist.size());
                    for (Index t : indexlist) {
                        byte[] indexSerialized = t.serialize();
                        dout.writeArray(indexSerialized);
                    }
                } else {
                    dout.writeInt(0);
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

    }

    @Override
    public void writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path checkPointFile = getTablespaceCheckPointInfoFile(tableSpace);
            Files.createDirectories(checkPointFile.getParent());
            LOGGER.log(Level.SEVERE, "checkpoint for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + checkPointFile.toAbsolutePath().toString());
            try (OutputStream out = Files.newOutputStream(checkPointFile, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                DataOutputStream dout = new DataOutputStream(out)) {
                dout.writeUTF(tableSpace);
                dout.writeLong(sequenceNumber.ledgerId);
                dout.writeLong(sequenceNumber.offset);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
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

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) throws DataStorageManagerException {
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path checkPointFile = getTablespaceCheckPointInfoFile(tableSpace);
            LOGGER.log(Level.SEVERE, "getLastcheckpointSequenceNumber for tableSpace " + tableSpace + " from " + checkPointFile.toAbsolutePath().toString());
            if (!Files.isRegularFile(checkPointFile)) {
                return LogSequenceNumber.START_OF_TIME;
            }
            try (InputStream input = new BufferedInputStream(Files.newInputStream(checkPointFile, StandardOpenOption.READ), 4 * 1024 * 1024);
                DataInputStream din = new DataInputStream(input);) {
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + checkPointFile.toAbsolutePath() + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readLong();
                long offset = din.readLong();

                return new LogSequenceNumber(ledgerId, offset);
            }
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
    public KeyToPageIndex createKeyToPageMap(String tablespace, String name) throws DataStorageManagerException {
        return new ConcurrentMapKeyToPageIndex(new ConcurrentHashMap<>());
//        try {            
//              return new MapDBKeyToPageIndex(tmpDirectory, tablespace + "." + name);
//        } catch (IOException err) {
//            throw new DataStorageManagerException(err);
//        }
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

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace, Consumer<Transaction> consumer) throws DataStorageManagerException {
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceTransactionsFile(tableSpace, sequenceNumber);
            if (!Files.isRegularFile(file)) {
                return;
            }
            try (InputStream input = new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), 4 * 1024 * 1024);
                ExtendedDataInputStream din = new ExtendedDataInputStream(input);) {
                String readname = din.readUTF();
                if (!readname.equals(tableSpace)) {
                    throw new DataStorageManagerException("file " + file.toAbsolutePath() + " is not for spablespace " + tableSpace);
                }
                long ledgerId = din.readLong();
                long offset = din.readLong();
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
    public void writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions) throws DataStorageManagerException {
        if (sequenceNumber.isStartOfTime() && !transactions.isEmpty()) {
            throw new DataStorageManagerException("impossible to write a non empty transactions list at start-of-time");
        }
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceTransactionsFile(tableSpace, sequenceNumber);
            Files.createDirectories(file.getParent());
            LOGGER.log(Level.SEVERE, "writeTransactionsAtCheckpoint for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + file.toAbsolutePath().toString() + ", active transactions " + transactions.size());
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(out)) {
                dout.writeUTF(tableSpace);
                dout.writeLong(sequenceNumber.ledgerId);
                dout.writeLong(sequenceNumber.offset);
                dout.writeInt(transactions.size());
                for (Transaction t : transactions) {
                    t.serialize(dout);
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

}
