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
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.model.TableSpace;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * HerdDB DataSource
 *
 * @author enrico.olivelli
 */
public class BasicHerdDBDataSource implements javax.sql.DataSource, AutoCloseable {

    protected HDBClient client;
    protected final Properties properties = new Properties();
    protected int loginTimeout;
    protected int maxActive = 100;

    private static final Logger LOGGER = Logger.getLogger(BasicHerdDBDataSource.class.getName());
    protected String url;
    protected String defaultSchema = TableSpace.DEFAULT;
    private String waitForTableSpace = "";
    private int waitForTableSpaceTimeout = 60000;
    private boolean discoverTableSpaceFromQuery = true;
    private HDBConnection connection;
    private GenericObjectPool<HerdDBConnection> pool;

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

    public synchronized void setUrl(String url) {
        this.url = url;
    }

    public Properties getProperties() {
        return properties;
    }

    protected synchronized void doWaitForTableSpace() throws SQLException {
        if (waitForTableSpaceTimeout > 0 && !waitForTableSpace.isEmpty()) {
            try (HDBConnection con = client.openConnection();) {
                con.waitForTableSpace(waitForTableSpace, waitForTableSpaceTimeout);
            } catch (HDBException err) {
                throw new SQLException(err);
            }
        }
    }

    protected synchronized void ensureClient() throws SQLException {
        if (client == null) {
            ClientConfiguration clientConfiguration = new ClientConfiguration(properties);
            Properties propsNoPassword = new Properties(properties);
            if (propsNoPassword.contains("password")) {
                propsNoPassword.setProperty("password", "-------");
            }
            LOGGER.log(Level.INFO, "Booting HerdDB Client, url:" + url + ", properties:" + propsNoPassword + " clientConfig " + clientConfiguration);
            clientConfiguration.readJdbcUrl(url);
            if (properties.containsKey("discoverTableSpaceFromQuery")) {
                this.discoverTableSpaceFromQuery = clientConfiguration.getBoolean("discoverTableSpaceFromQuery", true);
            }
            client = new HDBClient(clientConfiguration);
        }
        if (pool == null) {
            if (properties.containsKey("maxActive")) {
                this.maxActive = Integer.parseInt(properties.get("maxActive").toString());
            }
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            config.setBlockWhenExhausted(true);
            config.setMaxTotal(maxActive);
            config.setMaxIdle(maxActive);
            config.setMinIdle(maxActive / 2);
            config.setJmxNamePrefix("HerdDBClient");
            pool = new GenericObjectPool<>(new ConnectionsFactory(), config);
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

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        ensureConnection();
        try {
            return pool.borrowObject();
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
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public synchronized void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    void releaseConnection(HerdDBConnection connection) {
        pool.returnObject(connection);
    }

    private class ConnectionsFactory implements PooledObjectFactory<HerdDBConnection> {

        @Override
        public PooledObject<HerdDBConnection> makeObject() throws Exception {
            HerdDBConnection res = new HerdDBConnection(BasicHerdDBDataSource.this, getHDBConnection(), defaultSchema);
            return new DefaultPooledObject<>(res);
        }

        @Override
        public void destroyObject(PooledObject<HerdDBConnection> po) throws Exception {
            po.getObject().close();
        }

        @Override
        public boolean validateObject(PooledObject<HerdDBConnection> po) {
            return true;
        }

        @Override
        public void activateObject(PooledObject<HerdDBConnection> po) throws Exception {
            po.getObject().reset(defaultSchema);
        }

        @Override
        public void passivateObject(PooledObject<HerdDBConnection> po) throws Exception {
        }

    }
}
