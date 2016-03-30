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

import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Data Storage on local filesystem
 *
 * @author enrico.olivelli
 */
public class SimpleFileDataStorageManager extends DataStorageManager {

    private final Path baseDirectory;
    private final AtomicLong newPageId = new AtomicLong();

    public SimpleFileDataStorageManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public void start() throws DataStorageManagerException {
        try {
            Files.createDirectories(baseDirectory);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {

    }

    private Path getTableDirectory(String tablename) {
        return baseDirectory.resolve(tablename);
    }

    private Path getPageFile(Path tableDirectory, Long pageId) {
        return tableDirectory.resolve(pageId + ".page");
    }

    private Path getTableKeysMetadataFile(Path tableDirectory) {
        return tableDirectory.resolve("keys");
    }

    @Override
    public List<Record> loadPage(String tableName, Long pageId) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableName);
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
    public void loadExistingKeys(String tableName, BiConsumer<Bytes, Long> consumer) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path keys = getTableKeysMetadataFile(tableDir);
        if (!Files.isRegularFile(keys)) {
            return;
        }
        try (InputStream input = Files.newInputStream(keys, StandardOpenOption.READ);
                DataInputStream dataIn = new DataInputStream(input)) {

            while (true) {
                int keySize = dataIn.readInt();
                byte[] key = new byte[keySize];
                dataIn.readFully(key);
                long pageId = dataIn.readLong();
                consumer.accept(new Bytes(key), pageId);
            }
        } catch (EOFException err) {
            // OK
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public Long writePage(String tableName, LogSequenceNumber sequenceNumber, List<Record> newPage) throws DataStorageManagerException {
        // synch on table is done by the TableManager
        long pageId = newPageId.incrementAndGet();
        Path tableDir = getTableDirectory(tableName);
        try {
            Files.createDirectories(tableDir);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        Path pageFile = getPageFile(tableDir, pageId);
        Path keys = getTableKeysMetadataFile(tableDir);
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
        try (OutputStream outputKeys = Files.newOutputStream(keys, StandardOpenOption.APPEND,StandardOpenOption.CREATE);
                DataOutputStream dataOutputKeys = new DataOutputStream(outputKeys)) {
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
    public int getActualNumberOfPages(String tableName) throws DataStorageManagerException {
        Path tableDir = getTableDirectory(tableName);
        try {
            Files.createDirectories(tableDir);

            AtomicInteger count = new AtomicInteger();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir, (path) -> {
                return path.toString().endsWith(".page");
            });) {
                stream.forEach(p -> {
                    System.out.println("GOT PAGE " + p.toString());
                    count.incrementAndGet();
                });
            }
            return count.get();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

}
