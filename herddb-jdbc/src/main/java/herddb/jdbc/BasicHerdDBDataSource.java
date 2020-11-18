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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.client.ClientConfiguration;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.model.TableSpace;
import herddb.utils.QueryParser;
import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HerdDB DataSource
 *
 * @author enrico.olivelli
 */
public class BasicHerdDBDataSource implements javax.sql.DataSource, AutoCloseable, Closeable {

    protected HDBClient client;
    protected final Properties properties = new Properties();
    protected int loginTimeout;
    protected int maxActive = 200;
    private boolean autoClose;
    private final AtomicInteger activeCount = new AtomicInteger();
    private Consumer<BasicHerdDBDataSource> onAutoClose = BasicHerdDBDataSource::close;
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicHerdDBDataSource.class.getName());
    protected String url;
    protected String defaultSchema = TableSpace.DEFAULT;
    private String waitForTableSpace = "";
    private int waitForTableSpaceTimeout = 60000;
    private boolean discoverTableSpaceFromQuery = true;
    private HDBConnection connection;
    private boolean poolConnections = true;
    private ConnectionsPoolRuntime poolRuntime;

    private synchronized HDBConnection getHDBConnection() {
        if (connection == null) {
            HDBClient _client = getClient();
            HDBConnection _connection = _client.openConnection();
            _connection.setDiscoverTablespaceFromSql(false);
            connection = _connection;
        }
        return connection;
    }

    public synchronized int getWaitForTableSpaceTimeout() {
        return waitForTableSpaceTimeout;
    }

    public synchronized void setWaitForTableSpaceTimeout(int waitForTableSpaceTimeout) {
        this.waitForTableSpaceTimeout = waitForTableSpaceTimeout;
    }

    public synchronized String getWaitForTableSpace() {
        return waitForTableSpace;
    }

    public synchronized void setWaitForTableSpace(String waitForTableSpace) {
        this.waitForTableSpace = waitForTableSpace;
    }

    public synchronized boolean isDiscoverTableSpaceFromQuery() {
        return discoverTableSpaceFromQuery;
    }

    public synchronized void setDiscoverTableSpaceFromQuery(boolean discoverTableSpaceFromQuery) {
        this.discoverTableSpaceFromQuery = discoverTableSpaceFromQuery;
    }

    protected BasicHerdDBDataSource() {
    }

    public BasicHerdDBDataSource(HDBClient client) {
        if (client == null) {
            throw new NullPointerException();
        }
        this.client = client;
    }

    public synchronized int getMaxActive() {
        return maxActive;
    }

    public synchronized void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public synchronized String getUsername() {
        if (client != null) {
            return client.getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
        } else {
            return properties.getProperty(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
        }
    }

    public synchronized void setUsername(String username) {
        properties.put(ClientConfiguration.PROPERTY_CLIENT_USERNAME, username);
        if (client != null) {
            client.getConfiguration().set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, username);
        }
    }

    public synchronized String getPassword() {
        if (client != null) {
            return client.getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
        } else {
            return properties.getProperty(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
        }
    }

    public synchronized void setPassword(String password) {
        properties.put(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, password);
        if (client != null) {
            client.getConfiguration().set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, password);
        }
    }

    public synchronized String getUrl() {
        return url;
    }

    @SuppressFBWarnings("IS2_INCONSISTENT_SYNC") // sb is lost by the lambda of forEach and does not see we are still in a synchronized block
    public synchronized void setUrl(String url) {
        this.url = url;
        if (client != null) {
            client.getConfiguration().set(ClientConfiguration.PROPERTY_CLIENT_INITIALIZED, "true");
        }
        QueryParser.parseQueryKeyPairs(url).forEach(pair -> {
            if (pair[0].startsWith("client.") && client != null) {
                client.getConfiguration().set(pair[0], pair[1]);
            } else {
                properties.setProperty(pair[0], pair[1]);
            }
        });
    }

    public synchronized boolean isPoolConnections() {
        return poolConnections;
    }

    public synchronized void setPoolConnections(boolean poolConnections) {
        if (client != null || poolRuntime != null) {
            throw new IllegalStateException("Cannot set poolConections=" + poolConnections + " once bootstrap is completed");
        }
        this.poolConnections = poolConnections;
    }

    public Properties getProperties() {
        return properties;
    }

    protected synchronized void doWaitForTableSpace() throws SQLException {
        if (waitForTableSpaceTimeout > 0 && !waitForTableSpace.isEmpty()) {
            try (HDBConnection con = client.openConnection()) {
                con.waitForTableSpace(waitForTableSpace, waitForTableSpaceTimeout);
            } catch (HDBException | ClientSideMetadataProviderException err) {
                throw new SQLException(err);
            }
        }
    }

    protected synchronized void ensureClient() throws SQLException {
        if (client == null) {
            ClientConfiguration clientConfiguration = new ClientConfiguration(properties);
            Properties propsNoPassword = new Properties();
            propsNoPassword.putAll(properties);
            propsNoPassword.setProperty("password", "---");
            LOGGER.info("Booting HerdDB Client, url: {}" + url + ", properties: {}, clientConfig {}", url, propsNoPassword, clientConfiguration);
            try {
                clientConfiguration.readJdbcUrl(url);
            } catch (RuntimeException err) {
                throw new SQLException(err);
            }
            autoClose = clientConfiguration.getBoolean("autoClose", isAutoClose());
            if (properties.containsKey("discoverTableSpaceFromQuery")) {
                this.discoverTableSpaceFromQuery = clientConfiguration.getBoolean("discoverTableSpaceFromQuery", true);
            }
            properties.stringPropertyNames().stream()
                    .filter(it -> it.startsWith("client."))
                    .forEach(key -> clientConfiguration.set(key, properties.getProperty(key)));
            client = new HDBClient(clientConfiguration);
        }
        if (poolRuntime == null) {
            poolConnections = Boolean.parseBoolean(properties.getProperty("poolConnections", poolConnections + ""));
            if (properties.containsKey("maxActive")) {
                this.maxActive = Integer.parseInt(properties.getProperty("maxActive", maxActive + ""));
            }
            if (!poolConnections) {
                poolRuntime = null;
            } else {
                poolRuntime = new ConnectionsPoolRuntime(this);
            }
        }
    }

    protected synchronized void ensureConnection() throws SQLException {
         ensureClient();
    }

    public synchronized HDBClient getClient() {
        return client;
    }

    public synchronized void setClient(HDBClient client) {
        this.client = client;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    HerdDBConnection makeConnection() throws SQLException {
        return new HerdDBConnection(BasicHerdDBDataSource.this, getHDBConnection(), defaultSchema);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        ensureConnection();
        try {
            Connection res;
            if (!isPoolConnections()) {
                res = makeConnection();
            } else {
                res = getPoolRuntime().borrowObject();
            }
            activeCount.incrementAndGet();
            return res;
        } catch (Exception err) {
            throw new SQLException(err);
        }
    }

    private PrintWriter logWriter;

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return java.util.logging.Logger.getLogger(BasicHerdDBDataSource.class.getName());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public synchronized boolean isAutoClose() {
        return autoClose;
    }

    public synchronized void setAutoClose(boolean autoClose) {
        this.autoClose = autoClose;
    }

    public synchronized void setOnAutoClose(Consumer<BasicHerdDBDataSource> onAutoClose) {
        this.onAutoClose = onAutoClose;
    }

    @Override
    public synchronized void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    void releaseConnection(HerdDBConnection connection) {
        if (isPoolConnections()) {
            getPoolRuntime().returnObject(connection);
        }
        if (activeCount.decrementAndGet() == 0) {
            performAutoClose();
        }
    }

    private synchronized void performAutoClose() {
        if (autoClose) {
            if (onAutoClose != null) {
                onAutoClose.accept(this);
            }
        }
    }

    public synchronized ConnectionsPoolRuntime getPoolRuntime() {
        return poolRuntime;
    }

}
