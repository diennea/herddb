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

import herddb.model.TableSpace;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bootstrap for Embedded server
 *
 * @author enrico.olivelli
 */
public class HerdDBEmbeddedDataSource extends BasicHerdDBDataSource {

    private static final Logger LOGGER = Logger.getLogger(HerdDBEmbeddedDataSource.class.getName());

    private Server server;
    private volatile boolean serverInitialized;

    private boolean startServer;
    private String waitForTableSpace = "";
    private int waitForTableSpaceTimeout = 60000;

    public int getWaitForTableSpaceTimeout() {
        return waitForTableSpaceTimeout;
    }

    public void setWaitForTableSpaceTimeout(int waitForTableSpaceTimeout) {
        this.waitForTableSpaceTimeout = waitForTableSpaceTimeout;
    }

    public String getWaitForTableSpace() {
        return waitForTableSpace;
    }

    public void setWaitForTableSpace(String waitForTableSpace) {
        this.waitForTableSpace = waitForTableSpace;
    }

    public boolean isStartServer() {
        return startServer;
    }

    public void setStartServer(boolean startServer) {
        this.startServer = startServer;
    }

    public HerdDBEmbeddedDataSource() {
    }

    public HerdDBEmbeddedDataSource(Properties properties) {
        this.properties.putAll(properties);
    }

    public Server getServer() {
        return server;
    }

    @Override
    protected synchronized void ensureClient() throws SQLException {

        super.ensureClient();

        if (!serverInitialized) {
            ServerConfiguration serverConfiguration = new ServerConfiguration(properties);
            serverConfiguration.readJdbcUrl(url);
            String mode = serverConfiguration.getString(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
            if (ServerConfiguration.PROPERTY_MODE_LOCAL.equals(mode)
                || (ServerConfiguration.PROPERTY_MODE_STANDALONE.equals(mode) && startServer)) {
                LOGGER.log(Level.SEVERE, "Booting Local Embedded HerdDB mode, url:" + url + ", properties:" + serverConfiguration);
                server = new Server(serverConfiguration);
                try {
                    server.start();
                    // single machine, local mode, boot the 'default' tablespace
                    if (waitForTableSpace != null && !waitForTableSpace.isEmpty()) {
                        server.waitForTableSpaceBoot(waitForTableSpace, waitForTableSpaceTimeout, true);
                    }
                    server.waitForBootOfLocalTablespaces(waitForTableSpaceTimeout);
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                } catch (Exception ex) {
                    throw new SQLException("Cannot boot embedded server " + ex, ex);
                }
            }
            if (server != null && waitForTableSpace != null && !waitForTableSpace.isEmpty()) {
                try {
                    LOGGER.log(Level.SEVERE, "Waiting for boot of tablespace " + waitForTableSpace + ". Waiting at max " + waitForTableSpaceTimeout + " ms");
                    if (waitForTableSpace != null && !waitForTableSpace.isEmpty()) {
                        server.waitForTableSpaceBoot(waitForTableSpace, waitForTableSpaceTimeout, true);
                    }
                    server.waitForBootOfLocalTablespaces(waitForTableSpaceTimeout);
                } catch (Exception ex) {
                    throw new SQLException("Cannot wait for tableSpace " + defaultSchema + " to boot: " + ex, ex);
                }
            }
            serverInitialized = true;
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
