/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.jdbc;

import static herddb.model.TransactionContext.AUTOTRANSACTION_ID;
import static herddb.model.TransactionContext.NOTRANSACTION_ID;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.jdbc.utils.SQLExceptionUtils;
import herddb.model.TransactionContext;
import herddb.utils.QueryUtils;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JDBC Connection
 *
 * @author enrico.olivelli
 */
public class HerdDBConnection implements java.sql.Connection {

    private final HDBConnection connection;
    private long transactionId;
    private boolean autocommit = true;
    private String tableSpace;
    private final BasicHerdDBDataSource datasource;
    private boolean closed;
    private final ConcurrentHashMap<Long, HerdDBStatement> openStatements = new ConcurrentHashMap<>();

    HerdDBConnection(BasicHerdDBDataSource datasource, HDBConnection connection, String defaultTablespace) throws SQLException {
        if (connection == null) {
            throw new NullPointerException();
        }
        this.connection = connection;
        this.datasource = datasource;
        reset(defaultTablespace);
    }

    final void reset(String defaultTablespace) {
        if (!openStatements.isEmpty()) {
            throw new IllegalStateException("Found open statements " + openStatements);
        }
        this.autocommit = true;
        this.tableSpace = defaultTablespace;
        this.transactionId = 0;
        this.closed = false;
    }

    long ensureTransaction() throws SQLException {
        if (!autocommit && transactionId == NOTRANSACTION_ID) {
            // transaction will be started at first statement execution
            transactionId = TransactionContext.AUTOTRANSACTION_ID;
        }
        return transactionId;
    }

    public String getTableSpace() {
        return tableSpace;
    }

    public HDBConnection getConnection() {
        return connection;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new HerdDBStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new HerdDBPreparedStatement(this, sql, false);
    }

    private PreparedStatement prepareStatement(String sql, boolean returnValues) throws SQLException {
        return new HerdDBPreparedStatement(this, sql, returnValues);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.autocommit == autoCommit) {
            return;
        }
        if (this.autocommit && !autoCommit) {
            this.autocommit = false;
            return;
        }
        if (!this.autocommit && autoCommit) {
            commit();
            this.autocommit = true;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autocommit;
    }

    @Override
    public void commit() throws SQLException {
        if (autocommit) {
            throw new SQLException("connection is in autocommit mode");
        }
        if (transactionId == NOTRANSACTION_ID || transactionId == AUTOTRANSACTION_ID) {
            // no transaction actually started, nothing to commit
            return;
        }
        ensureAllStatementClosed();
        try {
            connection.commitTransaction(tableSpace, transactionId);
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw SQLExceptionUtils.wrapException(err);
        } finally {
            transactionId = NOTRANSACTION_ID;
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (autocommit) {
            throw new SQLException("connection is not in autocommit mode");
        }
        if (transactionId == NOTRANSACTION_ID || transactionId == AUTOTRANSACTION_ID) {
            // no transaction actually started, nothing to commit
            return;
        }
        ensureAllStatementClosed();
        try {
            connection.rollbackTransaction(tableSpace, transactionId);
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw SQLExceptionUtils.wrapException(err);
        } finally {
            transactionId = NOTRANSACTION_ID;
        }
    }

    private void ensureAllStatementClosed() throws SQLException {
        for (HerdDBStatement ps : openStatements.values()) {
            LOGGER.log(Level.FINE, "closing abandoned statement {0}", ps);
            try {
                ps.close();
            } catch (SQLException err) {
                LOGGER.log(Level.SEVERE, "Cannot close an open statement", err);
            }
        }
        openStatements.clear();
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        ensureAllStatementClosed();
        if (transactionId != NOTRANSACTION_ID
                && transactionId != AUTOTRANSACTION_ID) {
            rollback();
        }
        closed = true;
        datasource.releaseConnection(this);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed || connection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new HerdDBDatabaseMetadata(this, tableSpace);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return "default_catalog";
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_READ_COMMITTED) {
            LOGGER.log(Level.SEVERE,
                    "Warning, ignoring setTransactionIsolation {0}, only TRANSACTION_READ_COMMITTED is supported", level);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(HerdDBConnection.class.getName());

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql, autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql, columnIndexes != null && columnIndexes.length > 0);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql, columnNames != null && columnNames.length > 0);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    private Properties clientInfo = new Properties();

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (transactionId > 0 && !Objects.equals(schema, tableSpace)) {
            throw new SQLException(
                    "cannot switch to schema " + schema + ", transaction (ID " + transactionId + ") is already started on tableSpace " + tableSpace);
        }
        this.tableSpace = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return tableSpace;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
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
    public String toString() {
        return "HerdDBConnection{connection=" + connection + ", transactionId=" + transactionId + ", autocommit=" + autocommit + ", tableSpace=" + tableSpace + '}';
    }

    void discoverTableSpace(String sql) throws SQLException {
        setSchema(QueryUtils.discoverTablespace(tableSpace, sql));
    }

    void releaseStatement(HerdDBStatement statement) {
        this.openStatements.remove(statement.getId());
    }

    void bindToTransaction(long transactionId) {
        this.transactionId = transactionId;
    }

    void registerOpenStatement(HerdDBStatement statement) {
       openStatements.put(statement.getId(), statement);
    }

}
