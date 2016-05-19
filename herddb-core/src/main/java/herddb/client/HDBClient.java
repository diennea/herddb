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

import static herddb.client.ClientConfiguration.PROPERTY_MODE;
import static herddb.client.ClientConfiguration.PROPERTY_MODE_LOCAL;
import herddb.server.LoopbackClientSideMetadataProvider;
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
    private final Map<Long, HDBConnection> connections = new ConcurrentHashMap<>();
    private ClientSideMetadataProvider clientSideMetadataProvider;

    public HDBClient(ClientConfiguration configuration) {
        this.configuration = configuration;
        init();
    }

    private void init() {
        String mode = configuration.getString(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_LOCAL);
        switch (mode) {
            case ClientConfiguration.PROPERTY_MODE_LOCAL:
                break;
            case ClientConfiguration.PROPERTY_MODE_STANDALONE:
                break;
            case ClientConfiguration.PROPERTY_MODE_CLUSTER:
                this.clientSideMetadataProvider = new ZookeeperClientSideMetadataProvider(
                        configuration.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT),
                        configuration.getInt(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT),
                        configuration.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT)
                );
                break;
        }
    }

    public ClientSideMetadataProvider getClientSideMetadataProvider() {
        return clientSideMetadataProvider;
    }

    public void setClientSideMetadataProvider(ClientSideMetadataProvider clientSideMetadataProvider) {
        this.clientSideMetadataProvider = clientSideMetadataProvider;
    }

    public ClientConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void close() {
        List<HDBConnection> connectionsAtClose = new ArrayList<>(this.connections.values());
        for (HDBConnection connection : connectionsAtClose) {
            connection.close();
        }
    }

    public HDBConnection openConnection() {
        HDBConnection con = new HDBConnection(this);
        connections.put(con.getId(), con);
        return con;
    }

    void releaseConnection(HDBConnection connection) {
        connections.remove(connection.getId());
    }

}
