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
class AbstractHerdDBDataSource implements javax.sql.DataSource, AutoCloseable {

    protected HDBClient client;
    protected final Properties properties = new Properties();
    protected int loginTimeout;
    protected int maxActive = 100;

    private static final Logger LOGGER = Logger.getLogger(AbstractHerdDBDataSource.class.getName());
    protected String url;
    protected String defaultSchema = TableSpace.DEFAULT;
    private GenericObjectPool<HDBConnection> pool;

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public String getUsername() {
        if (client != null) {
            return client.getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
        } else {
            return properties.getProperty(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
        }
    }

    public void setUsername(String username) {
        properties.put(ClientConfiguration.PROPERTY_CLIENT_USERNAME, username);
        if (client != null) {
            client.getConfiguration().set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, username);
        }
    }

    public String getPassword() {
        if (client != null) {
            return client.getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
        } else {
            return properties.getProperty(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
        }
    }

    public void setPassword(String password) {
        properties.put(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, password);
        if (client != null) {
            client.getConfiguration().set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, password);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getProperties() {
        return properties;
    }

    protected AbstractHerdDBDataSource() {
    }

    public AbstractHerdDBDataSource(HDBClient client) {
        this.client = client;
    }

    void releaseConnection(HDBConnection connection) {
        pool.returnObject(connection);
    }

    private class ConnectionsFactory implements PooledObjectFactory<HDBConnection> {

        @Override
        public PooledObject<HDBConnection> makeObject() throws Exception {
            HDBConnection connection = client.openConnection();
            connection.setDiscoverTablespaceFromSql(false);
            return new DefaultPooledObject<>(connection);
        }

        @Override
        public void destroyObject(PooledObject<HDBConnection> po) throws Exception {
            po.getObject().close();
        }

        @Override
        public boolean validateObject(PooledObject<HDBConnection> po) {
            return true;
        }

        @Override
        public void activateObject(PooledObject<HDBConnection> po) throws Exception {
        }

        @Override
        public void passivateObject(PooledObject<HDBConnection> po) throws Exception {
        }

    }

    protected synchronized void ensureClient() throws SQLException {
        if (client == null) {
            ClientConfiguration clientConfiguration = new ClientConfiguration(properties);
            LOGGER.log(Level.SEVERE, "Booting HerdDB Client, url:" + url + ", properties:" + properties + " clientConfig " + clientConfiguration);
            clientConfiguration.readJdbcUrl(url);
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
            pool = new GenericObjectPool<>(new ConnectionsFactory(), config);
        }
    }

    protected HDBConnection createNewConnection() throws SQLException {
        try {
            return pool.borrowObject();
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    protected synchronized void ensureConnection() throws SQLException {
        ensureClient();

    }

    public HDBClient getClient() {
        return client;
    }

    public void setClient(HDBClient client) {
        this.client = client;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        ensureConnection();
        return new HerdDBConnection(this, createNewConnection(), defaultSchema);
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
}
