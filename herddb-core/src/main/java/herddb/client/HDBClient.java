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

import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerHostData;
import herddb.network.netty.NettyConnector;
import herddb.server.StaticClientSideMetadataProvider;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HerdDB Client
 *
 * @author enrico.olivelli
 */
public class HDBClient implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(HDBClient.class.getName());

    private final ClientConfiguration configuration;
    private final Map<Long, HDBConnection> connections = new ConcurrentHashMap<>();
    private ClientSideMetadataProvider clientSideMetadataProvider;
    private ExecutorService thredpool;
    private NioEventLoopGroup networkGroup;
    private DefaultEventLoopGroup localEventsGroup;

    public HDBClient(ClientConfiguration configuration) {
        this.configuration = configuration;
        init();
    }

    private void init() {
        LOG.log(Level.SEVERE, "init {0}", this);
        this.thredpool = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "hdb-client");
                t.setDaemon(true);
                return t;
            }
        });
        this.networkGroup = new NioEventLoopGroup(0, thredpool);
        this.localEventsGroup = new DefaultEventLoopGroup();
        String mode = configuration.getString(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_LOCAL);
        switch (mode) {
            case ClientConfiguration.PROPERTY_MODE_LOCAL:
                break;
            case ClientConfiguration.PROPERTY_MODE_STANDALONE:
                this.clientSideMetadataProvider = new StaticClientSideMetadataProvider(
                    configuration.getString(ClientConfiguration.PROPERTY_SERVER_ADDRESS, ClientConfiguration.PROPERTY_SERVER_ADDRESS_DEFAULT),
                    configuration.getInt(ClientConfiguration.PROPERTY_SERVER_PORT, ClientConfiguration.PROPERTY_SERVER_PORT_DEFAULT),
                    configuration.getBoolean(ClientConfiguration.PROPERTY_SERVER_SSL, ClientConfiguration.PROPERTY_SERVER_SSL_DEFAULT)
                );
                break;
            case ClientConfiguration.PROPERTY_MODE_CLUSTER:
                this.clientSideMetadataProvider = new ZookeeperClientSideMetadataProvider(
                    configuration.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT),
                    configuration.getInt(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT),
                    configuration.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT)
                );
                break;
            default:
                throw new IllegalStateException(mode);
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
        LOG.log(Level.SEVERE, "close {0}", this);
        List<HDBConnection> connectionsAtClose = new ArrayList<>(this.connections.values());
        for (HDBConnection connection : connectionsAtClose) {
            connection.close();
        }
        if (networkGroup != null) {
            networkGroup.shutdownGracefully();
        }
        if (localEventsGroup != null) {
            localEventsGroup.shutdownGracefully();
        }
        if (thredpool != null) {
            thredpool.shutdown();
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

    Channel createChannelTo(ServerHostData server, ChannelEventListener eventReceiver) throws IOException {
        int timeout = configuration.getInt(ClientConfiguration.PROPERTY_TIMEOUT, ClientConfiguration.PROPERTY_TIMEOUT_DEFAULT);
        return NettyConnector.connect(server.getHost(), server.getPort(), server.isSsl(), timeout, timeout, eventReceiver, thredpool, networkGroup, localEventsGroup);
    }

}
