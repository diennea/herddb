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
import herddb.server.StaticClientSideMetadataProvider;
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
public class HerdDBEmbeddedDataSource extends AbstractHerdDBDataSource {

    private static final Logger LOGGER = Logger.getLogger(HerdDBEmbeddedDataSource.class.getName());

    private Server server;

    public HerdDBEmbeddedDataSource() {
    }

    public Server getServer() {
        return server;
    }

    @Override
    protected synchronized void ensureClient() throws SQLException {

        if (server == null) {
            ServerConfiguration serverConfiguration = new ServerConfiguration(properties);
            LOGGER.log(Level.SEVERE, "Booting Embedded HerdDB");
            server = new Server(serverConfiguration);
            try {
                server.start();

            } catch (Exception ex) {
                throw new SQLException("Cannot boot embedded server " + ex, ex);
            }
        }

        super.ensureClient();

        if (client.getClientSideMetadataProvider() == null) {
            try {
                // single machine, local mode, boot the 'default' tablespace
                server.waitForStandaloneBoot();
            } catch (Exception ex) {
                throw new SQLException("Cannot boot embedded server " + ex, ex);
            }
            client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
        }

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
