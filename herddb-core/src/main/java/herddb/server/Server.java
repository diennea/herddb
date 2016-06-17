
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

import herddb.client.ClientConfiguration;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.EmbeddedBookie;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.DBManager;
import herddb.core.stats.ConnectionsInfo;
import herddb.core.stats.ConnectionsInfoProvider;
import herddb.file.FileBasedUserManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.log.CommitLogManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.model.TableSpace;
import herddb.network.Channel;
import herddb.network.ServerHostData;
import herddb.network.ServerSideConnection;
import herddb.network.ServerSideConnectionAcceptor;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.security.SimpleSingleUserManager;
import herddb.security.UserManager;
import herddb.storage.DataStorageManager;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * HerdDB Server
 *
 * @author enrico.olivelli
 */
public class Server implements AutoCloseable, ServerSideConnectionAcceptor<ServerSideConnection>, ConnectionsInfoProvider {

    private static final Logger LOGGER = Logger.getLogger(Server.class.getName());
    private final DBManager manager;
    private final NettyChannelAcceptor networkServer;
    private final ServerConfiguration configuration;
    private final Path baseDirectory;
    private final ServerHostData serverHostData;
    private final Map<Long, ServerSideConnectionPeer> connections = new ConcurrentHashMap<>();
    private final String mode;
    private final MetadataStorageManager metadataStorageManager;
    private UserManager userManager;
    private EmbeddedBookie embeddedBookie;

    public UserManager getUserManager() {
        return userManager;
    }

    public void setUserManager(UserManager userManager) {
        this.userManager = userManager;
    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    public DBManager getManager() {
        return manager;
    }

    public NettyChannelAcceptor getNetworkServer() {
        return networkServer;
    }

    public Server(ServerConfiguration configuration) {
        this.configuration = configuration;

        String nodeId = configuration.getString(ServerConfiguration.PROPERTY_NODEID, "");

        this.mode = configuration.getString(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        this.baseDirectory = Paths.get(configuration.getString(ServerConfiguration.PROPERTY_BASEDIR, ".")).toAbsolutePath();
        String usersfile = configuration.getString(ServerConfiguration.PROPERTY_USERS_FILE, ServerConfiguration.PROPERTY_USERS_FILE_DEFAULT);
        if (usersfile.isEmpty()) {
            this.userManager = new SimpleSingleUserManager(configuration);
        } else {
            try {
                Path userDirectoryFile = baseDirectory.resolve(usersfile).toAbsolutePath();
                LOGGER.log(Level.SEVERE, "Reading users from file " + userDirectoryFile);
                this.userManager = new FileBasedUserManager(userDirectoryFile);
            } catch (IOException error) {
                throw new RuntimeException(error);
            }
        }
        this.metadataStorageManager = buildMetadataStorageManager();
        String host = configuration.getString(ServerConfiguration.PROPERTY_HOST, ServerConfiguration.PROPERTY_HOST_DEFAULT);
        int port = configuration.getInt(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_DEFAULT);
        this.serverHostData = new ServerHostData(
                host,
                port,
                "",
                configuration.getBoolean(ServerConfiguration.PROPERTY_SSL, false),
                new HashMap<>());
        if (nodeId.isEmpty()) {
            nodeId = host + ":" + port;
        }
        LOGGER.log(Level.SEVERE, "local nodeID is " + nodeId);
        this.manager = new DBManager(nodeId,
                metadataStorageManager,
                buildDataStorageManager(),
                buildFileCommitLogManager(),
                baseDirectory, serverHostData
        );
        this.manager.setConnectionsInfoProvider(this);
        this.manager.setServerConfiguration(configuration);
        this.manager.setServerToServerUsername(configuration.getString(ServerConfiguration.PROPERTY_SERVER_TO_SERVER_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT));
        this.manager.setServerToServerPassword(configuration.getString(ServerConfiguration.PROPERTY_SERVER_TO_SERVER_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT));

        boolean enforeLeadership = configuration.getBoolean(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP_DEFAULT);
        this.manager.setErrorIfNotLeader(enforeLeadership);

        this.networkServer = buildChannelAcceptor();
        this.networkServer.setAcceptor(this);

        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                break;
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                LOGGER.severe("JDBC URL: jdbc:herddb:server:" + serverHostData.getHost() + ":" + serverHostData.getPort());
                break;
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
                LOGGER.severe("JDBC URL: jdbc:herddb:zookeeper:" + configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT) + configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT));
                this.embeddedBookie = new EmbeddedBookie(baseDirectory, configuration);
                break;
        }

    }

    private NettyChannelAcceptor buildChannelAcceptor() {
        NettyChannelAcceptor acceptor = new NettyChannelAcceptor(serverHostData.getHost(), serverHostData.getPort(), serverHostData.isSsl());
        if (ServerConfiguration.PROPERTY_MODE_LOCAL.equals(mode)) {
            acceptor.setEnableRealNetwork(false);
        }
        return acceptor;
    }

    private MetadataStorageManager buildMetadataStorageManager() {
        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryMetadataStorageManager();
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                return new FileMetadataStorageManager(baseDirectory);
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
                return new ZookeeperMetadataStorageManager(configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT),
                        configuration.getInt(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT),
                        configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT));
            default:
                throw new RuntimeException();
        }
    }

    private DataStorageManager buildDataStorageManager() {
        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryDataStorageManager();
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
                int diskswapThreshold = configuration.getInt(ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS, ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
                return new FileDataStorageManager(baseDirectory, diskswapThreshold);
            default:
                throw new RuntimeException();
        }
    }

    private CommitLogManager buildFileCommitLogManager() {

        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryCommitLogManager();
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                return new FileCommitLogManager(baseDirectory);
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
                BookkeeperCommitLogManager bkmanager = new BookkeeperCommitLogManager((ZookeeperMetadataStorageManager) this.metadataStorageManager);
                bkmanager.setAckQuorumSize(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE_DEFAULT));
                bkmanager.setEnsemble(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE, ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE_DEFAULT));
                bkmanager.setWriteQuorumSize(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE_DEFAULT));
                bkmanager.setLedgersRetentionPeriod(configuration.getLong(ServerConfiguration.PROPERTY_BOOKKEEPER_LOGRETENTION_PERIOD, ServerConfiguration.PROPERTY_LOG_RETENTION_PERIOD_DEFAULT));
                return bkmanager;
            default:
                throw new RuntimeException();
        }
    }

    public void start() throws Exception {
        boolean startBookie = configuration.getBoolean(ServerConfiguration.PROPERTY_BOOKKEEPER_START, ServerConfiguration.PROPERTY_BOOKKEEPER_START_DEFAULT);
        if (startBookie) {
            this.embeddedBookie.start();
        }
        this.manager.start();
        this.networkServer.start();
    }

    public void waitForStandaloneBoot() throws Exception {
        waitForTableSpaceBoot(TableSpace.DEFAULT, true);
    }

    public void waitForTableSpaceBoot(String tableSpace, boolean leader) throws Exception {
        if (!this.manager.waitForTablespace(tableSpace, 10000, leader)) {
            throw new Exception("TableSpace " + tableSpace + " not started");
        }
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

    public int getConnectionCount() {
        return connections.size();
    }

    @Override
    public ConnectionsInfo getActualConnections() {
        return new ConnectionsInfo(connections
                .values()
                .stream()
                .map(c -> {
                    return c.toConnectionInfo();
                })
                .collect(Collectors.toList()));
    }

}
