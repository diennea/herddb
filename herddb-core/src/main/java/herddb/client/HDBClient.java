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
package herddb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HerdDB Client
 *
 * @author enrico.olivelli
 */
public class HDBClient implements AutoCloseable {

    private final ClientConfiguration configuration;
    private final Map<Long, ClientSideConnection> connections = new ConcurrentHashMap<>();
    private ClientSideMetadataProvider clientSideMetadataProvider;
    private final ReentrantLock metadataLock = new ReentrantLock(true);
    private ClientSideMetadata metadata;

    public HDBClient(ClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public ClientSideMetadataProvider getClientSideMetadataProvider() {
        return clientSideMetadataProvider;
    }

    public void setClientSideMetadataProvider(ClientSideMetadataProvider clientSideMetadataProvider) {
        this.clientSideMetadataProvider = clientSideMetadataProvider;
    }

    public ClientSideMetadata getMetadata() throws ClientSideMetadataProviderException {
        metadataLock.lock();
        try {
            if (metadata == null) {
                metadata = clientSideMetadataProvider.readMetadata();
            }
            return metadata;
        } finally {
            metadataLock.unlock();
        }
    }

    public void invalidateMetadata() {
        metadataLock.lock();
        try {
            metadata = null;
        } finally {
            metadataLock.unlock();
        }
    }

    @Override
    public void close() {
        List<ClientSideConnection> connectionsAtClose = new ArrayList<>(this.connections.values());
        for (ClientSideConnection connection : connectionsAtClose) {
            connection.close();
        }
    }

    public ClientSideConnection openConnection() {
        ClientSideConnection con = new ClientSideConnection(this);
        connections.put(con.getId(), con);
        return con;
    }

    void releaseConnection(ClientSideConnection connection) {
        connections.remove(connection.getId());
    }

}
