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
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * HerdDB DataSource
 *
 * @author enrico.olivelli
 */
class AbstractHerdDBDataSource implements javax.sql.DataSource, AutoCloseable {

    protected HDBClient client;
    protected HDBConnection connection;
    protected final Properties properties = new Properties();
    protected int loginTimeout;

    protected String url;

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

    protected synchronized void ensureClient() throws SQLException {
        if (client == null) {
            ClientConfiguration clientConfiguration = new ClientConfiguration(properties);
            clientConfiguration.readJdbcUrl(url);
            client = new HDBClient(clientConfiguration);
        }
    }

    protected synchronized void ensureConnection() throws SQLException {
        ensureClient();

        if (this.connection == null) {
            this.connection = client.openConnection();
        }
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
        return new HerdDBConnection(connection);
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
        throw new SQLFeatureNotSupportedException();
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
        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
