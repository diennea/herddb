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

package herddb.metadata;

import herddb.model.DDLException;
import herddb.model.InvalidTableException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import herddb.server.ServerConfiguration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Store of all metadata of the system: definition of tables, tablesets, available nodes
 *
 * @author enrico.olivelli
 */
public abstract class MetadataStorageManager implements AutoCloseable {

    private MetadataChangeListener listener;

    public abstract void start() throws MetadataStorageManagerException;

    public abstract void ensureDefaultTableSpace(String localNodeId, String initialReplicaList, long maxLeaderInactivityTime) throws MetadataStorageManagerException;

    public abstract void close() throws MetadataStorageManagerException;

    /**
     * Enumerates all the available TableSpaces in the system
     *
     * @return
     */
    public abstract Collection<String> listTableSpaces() throws MetadataStorageManagerException;

    /**
     * Describe a single TableSpace
     *
     * @param name
     * @return
     */
    public abstract TableSpace describeTableSpace(String name) throws MetadataStorageManagerException;

    /**
     * Registers a new table space on the metadata storage
     *
     * @param tableSpace
     */
    public abstract void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException;

    /**
     * Drop a tablespace
     *
     * @param name
     * @param previous
     * @throws DDLException
     * @throws MetadataStorageManagerException
     */
    public abstract void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException;

    /**
     * Updates table space metadata on the metadata storage
     *
     * @param tableSpace
     */
    public abstract boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException;

    protected void validateTableSpace(TableSpace tableSpace) throws DDLException {
        // TODO: implement sensible validations
        if (tableSpace.name == null || tableSpace.name.trim().isEmpty()) {
            throw new InvalidTableException("null tablespace name");
        }
    }

    /**
     * Registers a node in the system
     *
     * @param nodeMetadata
     * @throws MetadataStorageManagerException
     */
    public void registerNode(NodeMetadata nodeMetadata) throws MetadataStorageManagerException {
    }

    /**
     * Notifies on metadata storage manage the state of the node against a given tablespace
     *
     * @param state
     * @throws MetadataStorageManagerException
     */
    public abstract void updateTableSpaceReplicaState(TableSpaceReplicaState state) throws MetadataStorageManagerException;

    public abstract List<TableSpaceReplicaState> getTableSpaceReplicaState(String tableSpaceUuid) throws MetadataStorageManagerException;

    /**
     * Enumerates all known nodes
     *
     * @return
     * @throws herddb.metadata.MetadataStorageManagerException
     */
    public List<NodeMetadata> listNodes() throws MetadataStorageManagerException {
        return Collections.emptyList();
    }

    public final void setMetadataChangeListener(MetadataChangeListener listener) {
        this.listener = listener;
    }

    public final MetadataChangeListener getListener() {
        return listener;
    }


    protected final void notifyMetadataChanged(String description) {
        if (listener != null) {
            listener.metadataChanged(description);
        }
    }

    public void clear() throws MetadataStorageManagerException {

    }

    public String generateNewNodeId(ServerConfiguration config) throws MetadataStorageManagerException {
        List<NodeMetadata> actualNodes = listNodes();
        NodeIdGenerator generator = new NodeIdGenerator();
        for (int i = 0; i < 10000; i++) {
            String _nodeId = generator.nextId();
            if (!actualNodes.stream().filter(node -> _nodeId.equals(node.nodeId)).findFirst().isPresent()) {
                return _nodeId;
            }
        }
        throw new MetadataStorageManagerException("cannot find a new node id");
    }

}
