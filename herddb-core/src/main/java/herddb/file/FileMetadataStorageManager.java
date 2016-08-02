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

import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Metadata on local files. Useful for single instance deplyments
 *
 * @author enrico.olivelli
 */
public class FileMetadataStorageManager extends MetadataStorageManager {

    private final Path baseDirectory;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<String, TableSpace> tableSpaces = new HashMap<>();
    private final static Logger LOGGER = Logger.getLogger(FileMetadataStorageManager.class.getName());
    private final ConcurrentMap<String, Map<String, TableSpaceReplicaState>> statesForTableSpace = new ConcurrentHashMap<>();

    public FileMetadataStorageManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public void start() throws MetadataStorageManagerException {
        try {
            Files.createDirectories(baseDirectory);
            reloadFromDisk();
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public Collection<String> listTableSpaces() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(tableSpaces.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public TableSpace describeTableSpace(String name) {
        lock.readLock().lock();
        try {
            return tableSpaces.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            if (tableSpaces.containsKey(tableSpace.name)) {
                throw new TableSpaceAlreadyExistsException("a tablespace named " + tableSpace.name + " already exists");
            }
            persistTableSpaceOnDisk(tableSpace);
            tableSpaces.put(tableSpace.name, tableSpace);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException {

        lock.writeLock().lock();
        try {
            if (!tableSpaces.containsKey(name)) {
                throw new TableSpaceDoesNotExistException("a tablespace named " + name + " does not exist");
            }
            removeTableSpaceFromDisk(name);
            tableSpaces.remove(name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            if (!tableSpaces.containsKey(tableSpace.name)) {
                throw new TableSpaceDoesNotExistException("a tablespace named " + tableSpace.name + " does not exist");
            }
            persistTableSpaceOnDisk(tableSpace);
            tableSpaces.put(tableSpace.name, tableSpace);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void reloadFromDisk() throws MetadataStorageManagerException {
        tableSpaces.clear();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDirectory)) {
            for (Path p : stream) {
                String filename = p.getFileName().toString();
                LOGGER.log(Level.SEVERE, "reading metadata file " + p.toAbsolutePath().toString());
                if (filename.endsWith(".metadata")) {
                    try (InputStream in = Files.newInputStream(p);
                            DigestInputStream diin = new DigestInputStream(in, FileDataStorageManager.createMD5());
                            ExtendedDataInputStream iin = new ExtendedDataInputStream(diin);) {
                        TableSpace ts = TableSpace.deserialize(iin, 0);
                        byte[] computedDigest = diin.getMessageDigest().digest();
                        byte[] storedDigest = iin.readArray();
                        if (!Arrays.equals(computedDigest, storedDigest)) {
                            throw new MetadataStorageManagerException("Corrupted metadata file " + p.toAbsolutePath().toString() + ", bad md5");
                        }
                        if (filename.equals(ts.name + ".metadata")) {
                            tableSpaces.put(ts.name, ts);
                        }
                    }
                }

            }
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    private void persistTableSpaceOnDisk(TableSpace tableSpace) throws MetadataStorageManagerException {
        Path file_tmp = baseDirectory.resolve(tableSpace.name + "." + System.nanoTime() + ".tmpmetadata");
        Path file = baseDirectory.resolve(tableSpace.name + ".metadata");
        try (OutputStream out = Files.newOutputStream(file_tmp, StandardOpenOption.CREATE_NEW);
                DigestOutputStream diout = new DigestOutputStream(out, FileDataStorageManager.createMD5());
                ExtendedDataOutputStream dout = new ExtendedDataOutputStream(diout)) {
            tableSpace.serialize(dout);
            dout.writeArray(diout.getMessageDigest().digest());
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }
        try {
            Files.move(file_tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void clear() throws MetadataStorageManagerException {
        lock.writeLock().lock();
        try {
            FileUtils.cleanDirectory(baseDirectory);
            Files.createDirectories(baseDirectory);
            tableSpaces.clear();
        } catch (IOException err) {
            LOGGER.log(Level.SEVERE, "cannot clear local data", err);
            throw new MetadataStorageManagerException(err);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void removeTableSpaceFromDisk(String tableSpace) throws MetadataStorageManagerException {
        try {
            Path file = baseDirectory.resolve(tableSpace + ".metadata");
            Files.deleteIfExists(file);
        } catch (IOException error) {
            throw new MetadataStorageManagerException(error);
        }
    }

    @Override
    public void ensureDefaultTableSpace(String localNodeId) throws MetadataStorageManagerException {
        lock.writeLock().lock();
        try {
            TableSpace exists = tableSpaces.get(TableSpace.DEFAULT);
            if (exists == null) {
                TableSpace defaultTableSpace = TableSpace
                        .builder()
                        .leader(localNodeId)
                        .replica(localNodeId)
                        .name(TableSpace.DEFAULT)
                        .build();
                registerTableSpace(defaultTableSpace);
            }
        } catch (DDLException err) {
            throw new MetadataStorageManagerException(err);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<TableSpaceReplicaState> getTableSpaceReplicaState(String tableSpaceUuid) throws MetadataStorageManagerException {
        Map<String, TableSpaceReplicaState> result = statesForTableSpace.get(tableSpaceUuid);
        if (result == null) {
            return Collections.emptyList();
        } else {
            return new ArrayList<>(result.values());
        }
    }

    @Override
    public void updateTableSpaceReplicaState(TableSpaceReplicaState state) throws MetadataStorageManagerException {
        Map<String, TableSpaceReplicaState> result = statesForTableSpace.get(state.uuid);
        if (result == null) {
            result = new ConcurrentHashMap<>();
            Map<String, TableSpaceReplicaState> failed = statesForTableSpace.putIfAbsent(state.uuid, result);
            if (failed != null) {
                throw new MetadataStorageManagerException("concurrent modification to " + state.uuid + " tableSpace");
            }
        }
        result.put(state.nodeId, state);
    }

}
