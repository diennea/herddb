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

package herddb.mem;

import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory MetadataStorageManager implementation. For tests
 *
 * @author enrico.olivelli
 */
public class MemoryMetadataStorageManager extends MetadataStorageManager {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, TableSpace> tableSpaces = new HashMap<>();
    private final ConcurrentMap<String, Map<String, TableSpaceReplicaState>> statesForTableSpace = new ConcurrentHashMap<>();

    private final List<NodeMetadata> nodes = new ArrayList<>();

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            tableSpaces.clear();
            nodes.clear();
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
        name = name.toLowerCase();
        lock.readLock().lock();
        try {
            return tableSpaces.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            if (tableSpaces.putIfAbsent(tableSpace.name.toLowerCase(), tableSpace) != null) {
                throw new TableSpaceAlreadyExistsException(tableSpace.name.toLowerCase());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        lock.writeLock().lock();
        try {
            tableSpaces.remove(name.toLowerCase());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            TableSpace prev = tableSpaces.get(tableSpace.name.toLowerCase());
            if (prev == null) {
                throw new TableSpaceDoesNotExistException(tableSpace.name.toLowerCase());
            }
            tableSpaces.put(tableSpace.name.toLowerCase(), tableSpace);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void start() {

    }

    @Override
    public boolean ensureDefaultTableSpace(String localNodeId,
                                        String initialReplicaList,
                                        long maxLeaderInactivityTime,
                                        int expectedReplicaCount) throws MetadataStorageManagerException {
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
                return true;
            } else {
                return false;
            }
        } catch (DDLException err) {
            throw new MetadataStorageManagerException(err);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {

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
