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

import herddb.core.RecordSetFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

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
        this(baseDirectory, ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
    }

    public FileDataStorageManager(Path baseDirectory, int swapThreshold) {
        this.baseDirectory = baseDirectory.resolve("data");
        this.tmpDirectory = baseDirectory.resolve("tmp");
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

    private Path getTableDirectory(String tableSpace, String tablename) {
        return getTablespaceDirectory(tableSpace).resolve(tablename + ".table");
    }

    private Path getPageFile(Path tableDirectory, Long pageId) {
        return tableDirectory.resolve(pageId + ".page");
    }

    private Path getTableCheckPointsFile(Path tableDirectory) {
        return tableDirectory.resolve("checkpoints");
    }

    private static final Provider DIGEST_PROVIDER = Security.getProvider("SUN");

    static MessageDigest createMD5()  {
        try {
            MessageDigest result = DIGEST_PROVIDER != null ? MessageDigest.getInstance("md5", DIGEST_PROVIDER) : MessageDigest.getInstance("md5");
            return result;
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public List<Record> readPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        Path pageFile = getPageFile(tableDir, pageId);
        try (InputStream input = Files.newInputStream(pageFile, StandardOpenOption.READ);
                DigestInputStream digest = new DigestInputStream(input, createMD5());
                ExtendedDataInputStream dataIn = new ExtendedDataInputStream(digest)) {
            int flags = dataIn.readVInt(); // flags for future implementations
            if (flags != 0) {
                throw new DataStorageManagerException("corrupted data file " + pageFile.toAbsolutePath());
            }
            int numRecords = dataIn.readInt();
            List<Record> result = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                byte[] key = dataIn.readArray();
                byte[] value = dataIn.readArray();
                result.add(new Record(new Bytes(key), new Bytes(value)));
            }
            byte[] computedDigest = digest.getMessageDigest().digest();
            byte[] writtenDigest = dataIn.readArray();
            if (!Arrays.equals(computedDigest, writtenDigest)) {
                throw new DataStorageManagerException("corrutped data file " + pageFile.toAbsolutePath() + ", MD5 checksum failed");
            }
            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void fullTableScan(String tableSpace, String tableName, FullTableScanConsumer consumer) throws DataStorageManagerException {

        TableStatus latestStatus = readActualTableStatus(tableSpace, tableName);

        LOGGER.log(Level.SEVERE, "fullTableScan table " + tableSpace + "." + tableName + ", status: " + latestStatus);
        consumer.acceptTableStatus(latestStatus);
        for (long idpage : latestStatus.activePages) {
            List<Record> records = readPage(tableSpace, tableName, idpage);
            consumer.startPage(idpage);
            LOGGER.log(Level.SEVERE, "fullTableScan table " + tableSpace + "." + tableName + ", page " + idpage + ", contains " + records.size() + " records");
            for (Record record : records) {
                consumer.acceptRecord(record);
            }
            consumer.endPage();
        }

    }

    private TableStatus readActualTableStatus(String tableSpace, String tableName) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path keys = getTableCheckPointsFile(tableDir);
        LOGGER.log(Level.SEVERE, "readActualTableStatus " + tableSpace + "." + tableName + " from " + keys);
        if (!Files.isRegularFile(keys)) {
            LOGGER.log(Level.SEVERE, "readActualTableStatus " + tableSpace + "." + tableName + " from " + keys + ". file does not exist");
            return new TableStatus(tableName, LogSequenceNumber.START_OF_TIME, Bytes.from_long(1).data, 1, new HashSet<>());
        }
        TableStatus latestStatus = null;
        try (InputStream input = Files.newInputStream(keys, StandardOpenOption.READ);
                ExtendedDataInputStream dataIn = new ExtendedDataInputStream(input)) {
            while (true) {
                int marker;
                try {
                    marker = dataIn.readInt();
                } catch (EOFException ok) {
                    break;
                }
                if (marker == TABLE_STATUS_MARKER) {
                    TableStatus tableStatus = TableStatus.deserialize(dataIn);
                    latestStatus = tableStatus;
                } else {
                    throw new IOException("corrupted file " + keys + ", missing marker");
                }

            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        if (latestStatus == null) {
            throw new DataStorageManagerException("table status not found on disk");
        }
        return latestStatus;
    }

    private static final int TABLE_STATUS_MARKER = 1233;

    @Override
    public void tableCheckpoint(String tableSpace, String tableName, TableStatus tableStatus) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path keys = getTableCheckPointsFile(tableDir);
        LOGGER.log(Level.SEVERE, "tableCheckpoint " + tableSpace + ", " + tableName + ": " + tableStatus + " to file " + keys);
        try (OutputStream outputKeys = Files.newOutputStream(keys, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                ExtendedDataOutputStream dataOutputKeys = new ExtendedDataOutputStream(outputKeys)) {
            dataOutputKeys.writeInt(TABLE_STATUS_MARKER);
            tableStatus.serialize(dataOutputKeys);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        // we can drop old page files now
        List<Path> pageFiles = getTablePageFiles(tableSpace, tableName);
        for (Path p : pageFiles) {
            long pageId = getPageId(p);
            LOGGER.log(Level.INFO, "checkpoint file " + p.toAbsolutePath() + " pageId " + pageId);
            if (pageId > 0 && !tableStatus.activePages.contains(pageId)) {
                LOGGER.log(Level.SEVERE, "checkpoint file " + p.toAbsolutePath() + " pageId " + pageId + ". to be deleted");
                try {
                    Files.deleteIfExists(p);
                } catch (IOException err) {
                    LOGGER.log(Level.SEVERE, "Could not delete file " + p.toAbsolutePath() + ":" + err, err);
                }
            }
        }
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
    public void writePage(String tableSpace, String tableName, long pageId, List<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager

        Path tableDir = getTableDirectory(tableSpace, tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        try (OutputStream output = Files.newOutputStream(pageFile, StandardOpenOption.CREATE_NEW);
                DigestOutputStream digest = new DigestOutputStream(output, createMD5());
                ExtendedDataOutputStream dataOutput = new ExtendedDataOutputStream(digest);) {
            dataOutput.writeVInt(0); // flags for future implementations
            dataOutput.writeInt(newPage.size());
            for (Record record : newPage) {
                dataOutput.writeArray(record.key.data);
                dataOutput.writeArray(record.value.data);
            }
            dataOutput.writeArray(digest.getMessageDigest().digest());
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String tableName) throws DataStorageManagerException {
        TableStatus readActualTableStatus = readActualTableStatus(tableSpace, tableName);
        return readActualTableStatus.activePages.size();
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
                    ExtendedDataInputStream din = new ExtendedDataInputStream(in);) {
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
    public void writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables) throws DataStorageManagerException {       
        if (sequenceNumber.isStartOfTime() && !tables.isEmpty()) {
            throw new DataStorageManagerException("impossibile to write a non empty table list at start-of-time");
        }
        try {
            Path tableSpaceDirectory = getTablespaceDirectory(tableSpace);
            Files.createDirectories(tableSpaceDirectory);
            Path file = getTablespaceTablesMetadataFile(tableSpace, sequenceNumber);
            Files.createDirectories(file.getParent());
            LOGGER.log(Level.SEVERE, "writeTables for tableSpace " + tableSpace + " sequenceNumber " + sequenceNumber + " to " + file.toAbsolutePath().toString());
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
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

    @Override
    public ConcurrentMap<Bytes, Long> createKeyToPageMap(String tablespace, String name) throws DataStorageManagerException {
        try {
            File temporarySwapFile = File.createTempFile("keysswap", ".bin", tmpDirectory.toFile());
            DB db = DBMaker
                    .newFileDB(temporarySwapFile)
                    .asyncWriteEnable()
                    .cacheHardRefEnable()
                    .mmapFileEnableIfSupported()
                    .transactionDisable()
                    .commitFileSyncDisable()
                    .deleteFilesAfterClose()
                    .make();
            return db.createHashMap("keys")
                    .keySerializer(new BytesSerializer())
                    .make();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void releaseKeyToPageMap(String tablespace, String name, Map<Bytes, Long> keyToPage) {
        if (keyToPage != null) {
            HTreeMap treeMap = (HTreeMap) keyToPage;
            treeMap.close();
        }
    }

    private static final class BytesSerializer implements Serializable, Serializer<Bytes> {

        private static final long serialVersionUID = 0;

        @Override
        public void serialize(DataOutput arg0, Bytes arg1) throws IOException {
            arg0.writeInt(arg1.data.length);
            arg0.write(arg1.data);
        }

        @Override
        public Bytes deserialize(DataInput arg0, int arg1) throws IOException {
            int len = arg0.readInt();
            byte[] data = new byte[len];
            arg0.readFully(data);
            return Bytes.from_array(data);
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return new FileRecordSetFactory(tmpDirectory, swapThreshold);
    }

    @Override
    public List<Transaction> loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, List<Transaction> transactions) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
}
