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
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import herddb.server.ServerConfiguration;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.ManagedFile;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.XXHash64Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
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
    private final List<NodeMetadata> nodes = new ArrayList<>();
    private static final Logger LOGGER = Logger.getLogger(FileMetadataStorageManager.class.getName());
    private final ConcurrentMap<String, Map<String, TableSpaceReplicaState>> statesForTableSpace = new ConcurrentHashMap<>();
    private volatile boolean started;

    public FileMetadataStorageManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public void start() throws MetadataStorageManagerException {
        if (started) {
            return;
        }
        try {
            Files.createDirectories(baseDirectory);
            reloadFromDisk();
            started = true;
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
            return tableSpaces.get(name.toLowerCase());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void registerNode(NodeMetadata nodeMetadata) throws MetadataStorageManagerException {
        lock.writeLock().lock();
        try {
            if (nodes.stream().filter(s -> s.nodeId.equals(nodeMetadata.nodeId)).findAny().isPresent()) {
                throw new MetadataStorageManagerException("node " + nodeMetadata.nodeId + " already exists");
            }
            nodes.add(nodeMetadata);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            if (tableSpaces.containsKey(tableSpace.name.toLowerCase())) {
                throw new TableSpaceAlreadyExistsException("a tablespace named " + tableSpace.name + " already exists");
            }
            persistTableSpaceOnDisk(tableSpace);
            tableSpaces.put(tableSpace.name.toLowerCase(), tableSpace);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException {

        name = name.toLowerCase();
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
        String name = tableSpace.name.toLowerCase();
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            if (!tableSpaces.containsKey(name)) {
                throw new TableSpaceDoesNotExistException("a tablespace named " + tableSpace.name.toLowerCase() + " does not exist");
            }
            persistTableSpaceOnDisk(tableSpace);
            tableSpaces.put(name, tableSpace);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void reloadFromDisk() throws MetadataStorageManagerException {
        tableSpaces.clear();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDirectory)) {
            for (Path p : stream) {
                Path filename = p.getFileName();
                if (filename == null) {
                    continue;
                }
                String _filename = filename.toString();
                LOGGER.log(Level.SEVERE, "reading metadata file {0}", p.toAbsolutePath().toString());
                if (_filename.endsWith(".metadata")) {
                    TableSpace ts = readTableSpaceMetadataFile(p);
                    if (_filename.equals(ts.name.toLowerCase() + ".metadata")) {
                        tableSpaces.put(ts.name.toLowerCase(), ts);
                    }
                }
            }
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    public static TableSpace readTableSpaceMetadataFile(Path p) throws IOException, MetadataStorageManagerException {
        TableSpace ts;
        byte[] pageData;
        try {
            pageData = FileUtils.fastReadFile(p);
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }
        boolean okHash = XXHash64Utils.verifyBlockWithFooter(pageData, 0, pageData.length);
        if (!okHash) {
            throw new MetadataStorageManagerException("corrutped data file " + p.toAbsolutePath() + ", checksum failed");
        }
        try (InputStream in = new SimpleByteArrayInputStream(pageData);
             ExtendedDataInputStream iin = new ExtendedDataInputStream(in)) {
            long version = iin.readVLong(); // version
            long flags = iin.readVLong(); // flags for future implementations
            if (version != 1 || flags != 0) {
                throw new IOException("corrupted data file " + p.toAbsolutePath());
            }
            ts = TableSpace.deserialize(iin, 0, 0);
        }
        return ts;
    }

    private void persistTableSpaceOnDisk(TableSpace tableSpace) throws MetadataStorageManagerException {
        Path tablespaceMetaTmp = baseDirectory.resolve(tableSpace.name.toLowerCase() + "." + System.nanoTime() + ".tmpmetadata");
        Path tablespaceMeta = baseDirectory.resolve(tableSpace.name.toLowerCase() + ".metadata");

        try (ManagedFile file = ManagedFile.open(tablespaceMetaTmp, ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT);
             SimpleBufferedOutputStream buffer = new SimpleBufferedOutputStream(file.getOutputStream(),
                     FileDataStorageManager.COPY_BUFFERS_SIZE);
             XXHash64Utils.HashingOutputStream oo = new XXHash64Utils.HashingOutputStream(buffer);
             ExtendedDataOutputStream dout = new ExtendedDataOutputStream(oo)) {

            dout.writeVLong(1); // version
            dout.writeVLong(0); // flags for future implementations
            tableSpace.serialize(dout);

            // footer
            dout.writeLong(oo.hash());
            dout.flush();
            file.sync();
        } catch (IOException err) {
            throw new MetadataStorageManagerException(err);
        }

        try {
            Files.move(tablespaceMetaTmp, tablespaceMeta, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
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
            nodes.clear();
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
    public List<NodeMetadata> listNodes() throws MetadataStorageManagerException {
        lock.readLock().lock();
        try {
            return new ArrayList<>(nodes);
        } finally {
            lock.readLock().unlock();
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
