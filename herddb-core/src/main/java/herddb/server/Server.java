
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.TableSpace;
import herddb.network.Channel;
import herddb.network.ServerHostData;
import herddb.network.ServerSideConnection;
import herddb.network.ServerSideConnectionAcceptor;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.network.netty.NetworkUtils;
import herddb.security.SimpleSingleUserManager;
import herddb.security.UserManager;
import herddb.storage.DataStorageManager;
import herddb.utils.Version;

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
    private final Path dataDirectory;
    private final Path tmpDirectory;
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
        this.baseDirectory = Paths.get(configuration.getString(ServerConfiguration.PROPERTY_BASEDIR, ServerConfiguration.PROPERTY_BASEDIR_DEFAULT)).toAbsolutePath();
        try {
            Files.createDirectories(this.baseDirectory);
        } catch (IOException ignore) {
            LOGGER.log(Level.SEVERE, "Cannot create baseDirectory " + this.baseDirectory, ignore);
        }
        this.dataDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_DATADIR, ServerConfiguration.PROPERTY_DATADIR_DEFAULT));
        this.tmpDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_TMPDIR, ServerConfiguration.PROPERTY_TMPDIR_DEFAULT));
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
        LOGGER.severe("Configured network parameters: " + ServerConfiguration.PROPERTY_HOST + "=" + host + ", " + ServerConfiguration.PROPERTY_PORT + "=" + port);
        if (host.trim().isEmpty()) {
            String _host = "0.0.0.0";
            LOGGER.log(Level.SEVERE, "As configuration parameter "
                + ServerConfiguration.PROPERTY_HOST + " is {0}, I have choosen to use {1}."
                + " Set to a non-empty value in order to use a fixed hostname", new Object[]{host, _host});
            host = _host;
        }
        if (port <= 0) {
            try {
                int _port = NetworkUtils.assignFirstFreePort();
                LOGGER.log(Level.SEVERE, "As configuration parameter "
                    + ServerConfiguration.PROPERTY_PORT + " is {0},I have choosen to listen on port {1}."
                    + " Set to a positive number in order to use a fixed port", new Object[]{Integer.toString(port), Integer.toString(_port)});
                port = _port;
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Cannot find a free port", err);
                throw new RuntimeException(err);
            }
        }
        String advertised_host = configuration.getString(ServerConfiguration.PROPERTY_ADVERTISED_HOST, host);
        if (advertised_host.trim().isEmpty() || advertised_host.equals("0.0.0.0")) {
            try {
                String _host = NetworkUtils.getLocalNetworkAddress();
                LOGGER.log(Level.SEVERE, "As configuration parameter "
                    + ServerConfiguration.PROPERTY_ADVERTISED_HOST + " is {0}, I have choosen to use {1}."
                    + " Set to a non-empty value in order to use a fixed hostname", new Object[]{advertised_host, _host});
                advertised_host = _host;
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Cannot get local host name", err);
                throw new RuntimeException(err);
            }
        }
        int advertised_port = configuration.getInt(ServerConfiguration.PROPERTY_ADVERTISED_PORT, port);

        HashMap<String, String> realData = new HashMap<>();
        realData.put(ServerConfiguration.PROPERTY_HOST, host);
        realData.put(ServerConfiguration.PROPERTY_PORT, port + "");
        LOGGER.severe("Public endpoint: " + ServerConfiguration.PROPERTY_ADVERTISED_HOST + "=" + advertised_host);
        LOGGER.severe("Public endpoint: " + ServerConfiguration.PROPERTY_ADVERTISED_PORT + "=" + advertised_port);
        this.serverHostData = new ServerHostData(
            advertised_host,
            advertised_port,
            "",
            configuration.getBoolean(ServerConfiguration.PROPERTY_SSL, false),
            realData);

        if (nodeId.isEmpty()) {
            LocalNodeIdManager localNodeIdManager = new LocalNodeIdManager(dataDirectory);
            try {
                nodeId = localNodeIdManager.readLocalNodeId();
                if (nodeId == null) {
                    // we need to eagerly start the metadataStorageManager, for instance to open the connection to ZK
                    metadataStorageManager.start();
                    nodeId = metadataStorageManager.generateNewNodeId(configuration);
                    LOGGER.severe("Generated new node id " + nodeId);
                    localNodeIdManager.persistLocalNodeId(nodeId);
                }
            } catch (IOException | MetadataStorageManagerException error) {
                LOGGER.log(Level.SEVERE, "Fatal error while generating the local node ID", error);
                throw new RuntimeException(new Exception("Fatal error while generating the local node ID: " + error, error));
            }
        }

        this.manager = new DBManager(nodeId,
            metadataStorageManager,
            buildDataStorageManager(),
            buildCommitLogManager(),
            tmpDirectory, serverHostData, configuration
        );

        this.manager.setClearAtBoot(configuration.getBoolean(ServerConfiguration.PROPERTY_CLEAR_AT_BOOT, ServerConfiguration.PROPERTY_CLEAR_AT_BOOT_DEFAULT));
        this.manager.setMaxLogicalPageSize(configuration.getLong(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE_DEFAULT));

        this.manager.setMaxMemoryReference(configuration.getLong(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE, ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT));
        this.manager.setMaxDataUsedMemory(configuration.getLong(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, ServerConfiguration.PROPERTY_MAX_DATA_MEMORY_DEFAULT));
        this.manager.setMaxIndexUsedMemory(configuration.getLong(ServerConfiguration.PROPERTY_MAX_INDEX_MEMORY, ServerConfiguration.PROPERTY_MAX_INDEX_MEMORY_DEFAULT));

        this.manager.setHaltOnTableSpaceBootError(configuration.getBoolean(ServerConfiguration.PROPERTY_HALT_ON_TABLESPACEBOOT_ERROR, ServerConfiguration.PROPERTY_HALT_ON_TABLESPACEBOOT_ERROR_DEAULT));
        this.manager.setConnectionsInfoProvider(this);
        this.manager.setServerToServerUsername(configuration.getString(ServerConfiguration.PROPERTY_SERVER_TO_SERVER_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT));
        this.manager.setServerToServerPassword(configuration.getString(ServerConfiguration.PROPERTY_SERVER_TO_SERVER_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT));
        this.manager.setCheckpointPeriod(configuration.getLong(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD_DEFAULT));

        boolean enforeLeadership = configuration.getBoolean(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP_DEFAULT);
        this.manager.setErrorIfNotLeader(enforeLeadership);

        this.networkServer = buildChannelAcceptor();
        this.networkServer.setAcceptor(this);

        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                LOGGER.severe("JDBC URL is not available. This server will not be accessible outside the JVM");
                break;
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                LOGGER.log(Level.SEVERE, "Use this JDBC URL to connect to this server: jdbc:herddb:server:{0}:{1}", new Object[]{serverHostData.getHost(), Integer.toString(serverHostData.getPort())});
                break;
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
                this.embeddedBookie = new EmbeddedBookie(baseDirectory, configuration);
                LOGGER.log(Level.SEVERE, "Use this JDBC URL to connect to this HerdDB cluster: jdbc:herddb:zookeeper:{0}{1}", new Object[]{configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT), configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT)});
                break;
            default:
                throw new IllegalStateException("invalid " + ServerConfiguration.PROPERTY_MODE + "=" + mode);
        }
        LOGGER.log(Level.INFO, "HerdDB version {0}", new Object[]{Version.getVERSION()});
        LOGGER.log(Level.INFO, "Local " + ServerConfiguration.PROPERTY_NODEID + " is {0}", new Object[]{nodeId});
    }

    private NettyChannelAcceptor buildChannelAcceptor() {
        String realHost = serverHostData.getAdditionalData().get(ServerConfiguration.PROPERTY_HOST);
        int realPort = Integer.parseInt(serverHostData.getAdditionalData().get(ServerConfiguration.PROPERTY_PORT));
        LOGGER.log(Level.SEVERE, "Binding network acceptor to {0}:{1} ssl:{2}",
            new Object[]{realHost, realPort, serverHostData.isSsl()});
        NettyChannelAcceptor acceptor = new NettyChannelAcceptor(realHost,
            realPort, serverHostData.isSsl());
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
                Path metadataDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_METADATADIR, ServerConfiguration.PROPERTY_METADATADIR_DEFAULT));
                return new FileMetadataStorageManager(metadataDirectory);
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
                return new FileDataStorageManager(dataDirectory, tmpDirectory, diskswapThreshold);
            default:
                throw new RuntimeException();
        }
    }

    private CommitLogManager buildCommitLogManager() {

        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryCommitLogManager();
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                Path logDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_LOGDIR, ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));
                return new FileCommitLogManager(logDirectory, 64 * 1024 * 1024);
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
                BookkeeperCommitLogManager bkmanager = new BookkeeperCommitLogManager((ZookeeperMetadataStorageManager) this.metadataStorageManager, configuration);
                bkmanager.setAckQuorumSize(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE_DEFAULT));
                bkmanager.setEnsemble(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE, ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE_DEFAULT));
                bkmanager.setWriteQuorumSize(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE_DEFAULT));
                long ledgersRetentionPeriod = configuration.getLong(ServerConfiguration.PROPERTY_BOOKKEEPER_LOGRETENTION_PERIOD, ServerConfiguration.PROPERTY_LOG_RETENTION_PERIOD_DEFAULT);
                bkmanager.setLedgersRetentionPeriod(ledgersRetentionPeriod);
                long checkPointperiod = configuration.getLong(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD_DEFAULT);

                if (checkPointperiod > 0 && ledgersRetentionPeriod > 0) {
                    long limit = ledgersRetentionPeriod / 2;
                    if (checkPointperiod > limit) {
                        throw new RuntimeException(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD + "=" + checkPointperiod
                            + " must be less then " + ServerConfiguration.PROPERTY_BOOKKEEPER_LOGRETENTION_PERIOD + "/2=" + limit);
                    }
                }

                return bkmanager;
            default:
                throw new RuntimeException();
        }
    }

    public void start() throws Exception {
        boolean startBookie = configuration.getBoolean(ServerConfiguration.PROPERTY_BOOKKEEPER_START,
            ServerConfiguration.PROPERTY_BOOKKEEPER_START_DEFAULT);
        if (startBookie && embeddedBookie != null) {
            this.embeddedBookie.start();
        }
        this.manager.start();
        this.networkServer.start();
    }

    public void waitForStandaloneBoot() throws Exception {
        waitForTableSpaceBoot(TableSpace.DEFAULT, true);
    }

    public void waitForTableSpaceBoot(String tableSpace, boolean leader) throws Exception {
        waitForTableSpaceBoot(tableSpace, 10000, leader);
    }

    public void waitForTableSpaceBoot(String tableSpace, int timeout, boolean leader) throws Exception {
        if (!this.manager.waitForTablespace(tableSpace, timeout, leader)) {
            throw new Exception("TableSpace " + tableSpace + " not started within " + timeout + " ms");
        }
    }

    public void waitForBootOfLocalTablespaces(int timeout) throws Exception {
        this.manager.waitForBootOfLocalTablespaces(timeout);
    }

    @Override
    public void close() throws Exception {
        try {
            networkServer.close();
        } catch (Throwable error) {
            LOGGER.log(Level.SEVERE, "error while stopping Network Manager" + error, error);
        }
        try {
            manager.close();
        } catch (Throwable error) {
            LOGGER.log(Level.SEVERE, "error while stopping embedded DBManager " + error, error);
        }

        if (embeddedBookie != null) {
            try {
                embeddedBookie.close();
            } catch (Throwable error) {
                LOGGER.log(Level.SEVERE, "error while stopping embedded bookie " + error, error);
            }
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
