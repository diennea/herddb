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

import herddb.client.HDBConnection;
import herddb.utils.EnsureIncrementAccumulator;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
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
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
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
    private final AtomicLong newPageId = new AtomicLong();

    public FileDataStorageManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory.resolve("data");
    }

    @Override
    public void start() throws DataStorageManagerException {
        try {
            LOGGER.log(Level.SEVERE, "ensuring directory {0}", baseDirectory.toAbsolutePath().toString());
            Files.createDirectories(baseDirectory);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {

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

    private Path getTableDirectory(String tableSpace, String tablename) {
        return getTablespaceDirectory(tableSpace).resolve(tablename + ".table");
    }

    private Path getPageFile(Path tableDirectory, Long pageId) {
        return tableDirectory.resolve(pageId + ".page");
    }

    private Path getTableMetadataFile(Path tableDirectory) {
        return tableDirectory.resolve("keys");
    }

    @Override
    public List<Record> loadPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        Path pageFile = getPageFile(tableDir, pageId);
        try (InputStream input = Files.newInputStream(pageFile, StandardOpenOption.READ);
                DataInputStream dataIn = new DataInputStream(input)) {
            int numRecords = dataIn.readInt();
            List<Record> result = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                int keySize = dataIn.readInt();
                int valueSize = dataIn.readInt();
                byte[] key = new byte[keySize];
                byte[] value = new byte[valueSize];
                dataIn.readFully(key);
                dataIn.readFully(value);
                result.add(new Record(new Bytes(key), new Bytes(value)));
            }
            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void restore(String tableSpace, String tableName, Consumer<TableStatus> tableStatusConsumer, BiConsumer<Bytes, Long> consumer) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path keys = getTableMetadataFile(tableDir);
        if (!Files.isRegularFile(keys)) {
            return;
        }
        try (InputStream input = Files.newInputStream(keys, StandardOpenOption.READ);
                DataInputStream dataIn = new DataInputStream(input)) {
            while (true) {
                int marker;
                try {
                    marker = dataIn.readInt();
                } catch (EOFException ok) {
                    break;
                }
                if (marker == TABLE_STATUS_MARKER) {
                    TableStatus tableStatus = TableStatus.deserialize(dataIn);
                    tableStatusConsumer.accept(tableStatus);
                } else {
                    throw new IOException("corrupted file " + keys + ", missing marker");
                }
                int numRecords = dataIn.readInt();
                for (int i = 0; i < numRecords; i++) {
                    int keySize = dataIn.readInt();
                    byte[] key = new byte[keySize];
                    dataIn.readFully(key);
                    long pageId = dataIn.readLong();
                    consumer.accept(new Bytes(key), pageId);
                    newPageId.accumulateAndGet(pageId + 1, new EnsureIncrementAccumulator());
                }

            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private static final int TABLE_STATUS_MARKER = 1233;

    @Override
    public Long writePage(String tableSpace, String tableName, TableStatus tableStatus, List<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long pageId = newPageId.incrementAndGet();
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        try (OutputStream output = Files.newOutputStream(pageFile, StandardOpenOption.CREATE_NEW);
                DataOutputStream dataOutput = new DataOutputStream(output)) {
            dataOutput.writeInt(newPage.size());
            for (Record record : newPage) {
                dataOutput.writeInt(record.key.data.length);
                dataOutput.writeInt(record.value.data.length);
                dataOutput.write(record.key.data);
                dataOutput.write(record.value.data);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Path keys = getTableMetadataFile(tableDir);
        try (OutputStream outputKeys = Files.newOutputStream(keys, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                DataOutputStream dataOutputKeys = new DataOutputStream(outputKeys)) {

            dataOutputKeys.writeInt(TABLE_STATUS_MARKER);
            tableStatus.serialize(dataOutputKeys);

            dataOutputKeys.writeInt(newPage.size());

            for (Record record : newPage) {
                dataOutputKeys.writeInt(record.key.data.length);
                dataOutputKeys.write(record.key.data);
                dataOutputKeys.writeLong(pageId);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        return pageId;
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String tableName) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);

            AtomicInteger count = new AtomicInteger();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir, (path) -> {
                return path.toString().endsWith(".page");
            });) {
                stream.forEach(p -> {
                    count.incrementAndGet();
                });
            }
            return count.get();
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
                    throw new DataStorageManagerException("local table data not available, recovering from sequenceNumber " + sequenceNumber);
                }
            }
            try (InputStream in = Files.newInputStream(file);
                    DataInputStream din = new DataInputStream(in);) {
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
                    int size = din.readInt();
                    byte[] tableData = new byte[size];
                    din.readFully(tableData);
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
    public void writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables) throws DataStorageManagerException {

        writeCheckpointSequenceNumber(tableSpace, sequenceNumber);

        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
            Files.createDirectories(file.getParent());
            LOGGER.log(Level.SEVERE, "writeTables for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + file.toAbsolutePath().toString());
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                    DataOutputStream dout = new DataOutputStream(out)) {
                dout.writeUTF(tableSpace);
                dout.writeLong(sequenceNumber.ledgerId);
                dout.writeLong(sequenceNumber.offset);
                dout.writeInt(tables.size());
                for (Table t : tables) {
                    byte[] tableSerialized = t.serialize();
                    dout.writeInt(tableSerialized.length);
                    dout.write(tableSerialized);
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
        LOGGER.log(Level.SEVERE, "dropTable " + tablespace + "." + tableName);
        Path tableDir = getTableDirectory(tablespace, tableName);
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
            try (InputStream in = Files.newInputStream(checkPointFile);
                    DataInputStream din = new DataInputStream(in);) {
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
}
