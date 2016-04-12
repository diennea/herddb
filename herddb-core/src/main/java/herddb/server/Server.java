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
package herddb.server;

import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.network.Channel;
import herddb.network.ServerHostData;
import herddb.network.ServerSideConnection;
import herddb.network.ServerSideConnectionAcceptor;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.storage.DataStorageManager;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * HerdDB Server
 *
 * @author enrico.olivelli
 */
public class Server implements AutoCloseable, ServerSideConnectionAcceptor<ServerSideConnection> {

    private static final Logger LOGGER = Logger.getLogger(Server.class.getName());
    private final DBManager manager;
    private final NettyChannelAcceptor networkServer;
    private final ServerConfiguration configuration;
    private final Path baseDirectory;
    private final ServerHostData serverHostData;
    private final Map<Long, ServerSideConnectionPeer> connections = new ConcurrentHashMap<>();

    public DBManager getManager() {
        return manager
                ;
    }

    public Server(ServerConfiguration configuration) {
        this.configuration = configuration;
        String nodeId = configuration.getString(ServerConfiguration.PROPERTY_NODEID, "");
        this.baseDirectory = Paths.get(configuration.getString(ServerConfiguration.PROPERTY_BASEDIR, ".")).toAbsolutePath();
        this.manager = new DBManager(nodeId,
                buildMetadataStorageManager(),
                buildDataStorageManager(),
                buildFileCommitLogManager());
        this.serverHostData = new ServerHostData(
                configuration.getString(ServerConfiguration.PROPERTY_HOST, ServerConfiguration.PROPERTY_HOST_DEFAULT),
                configuration.getInt(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_DEFAULT),
                "",
                configuration.getBoolean(ServerConfiguration.PROPERTY_SSL, false),
                new HashMap<>());
        this.networkServer = buildChannelAcceptor();
        this.networkServer.setAcceptor(this);
    }

    private NettyChannelAcceptor buildChannelAcceptor() {
        return new NettyChannelAcceptor(serverHostData.getHost(), serverHostData.getPort(), serverHostData.isSsl());
    }

    private MetadataStorageManager buildMetadataStorageManager() {
        return new FileMetadataStorageManager(baseDirectory);
    }

    private DataStorageManager buildDataStorageManager() {
        return new FileDataStorageManager(baseDirectory);
    }

    private FileCommitLogManager buildFileCommitLogManager() {
        return new FileCommitLogManager(baseDirectory);
    }

    public void start() throws Exception {
        this.manager.start();
        this.networkServer.start();
    }

    @Override
    public void close() throws Exception {
        try {
            networkServer.close();
        } finally {
            manager.close();
        }
    }

    @Override
    public ServerSideConnection createConnection(Channel channel) {
        ServerSideConnectionPeer peer = new ServerSideConnectionPeer(channel, this);
        connections.put(peer.getConnectionId(), peer);
        return peer;
    }

    void connectionClosed(ServerSideConnectionPeer connection) {
        connections.remove(connection.getConnectionId());
    }

    public String getNodeId() {
        return manager.getNodeId();
    }

    public ServerHostData getServerHostData() {
        return serverHostData;
    }

}
