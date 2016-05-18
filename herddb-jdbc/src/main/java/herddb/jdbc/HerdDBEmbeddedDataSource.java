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
package herddb.jdbc;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.LoopbackClientSideMetadataProvider;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.nio.file.Paths;
import java.security.AuthProvider;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bootstrap for Embedded server
 *
 * @author enrico.olivelli
 */
public class HerdDBEmbeddedDataSource extends HerdDBDataSource {

    private static final Logger LOGGER = Logger.getLogger(HerdDBEmbeddedDataSource.class.getName());

    private ClientConfiguration clientConfiguration;
    private final Properties properties = new Properties();
    private ServerConfiguration serverConfiguration;
    private Server server;

    public HerdDBEmbeddedDataSource() {
    }

    public Server getServer() {
        return server;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    protected synchronized void ensureConnection() throws SQLException {
        if (serverConfiguration == null) {
            LOGGER.log(Level.SEVERE, "Booting Embedded HerdDB");
            serverConfiguration = new ServerConfiguration(properties);
        }
        if (server == null) {
            server = new Server(serverConfiguration);
            try {
                server.start();
                server.waitForStandaloneBoot();
            } catch (Exception ex) {
                throw new SQLException("Cannot boot embedded server " + ex, ex);
            }
        }
        if (clientConfiguration == null) {
            clientConfiguration = new ClientConfiguration(properties);
        }
        if (client == null) {
            client = new HDBClient(clientConfiguration);
            client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));
        }
        super.ensureConnection();
    }

    @Override
    public synchronized void close() {
        super.close();
        if (server != null) {
            try {
                server.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error during server shutdown:" + err, err);
            }
            server = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
