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

import herddb.client.ClientSideMetadataProviderException;
import herddb.client.DMLResult;
import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.client.impl.EmptyScanResultSet;
import herddb.client.impl.SingletonScanResultSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;

/**
 * SQL Statement
 *
 * @author enrico.olivelli
 */
public class HerdDBStatement implements java.sql.Statement {

    protected final HerdDBConnection parent;
    protected int maxRows;
    protected ResultSet lastResultSet;
    protected long lastUpdateCount;
    protected Object lastKey;

    public HerdDBStatement(HerdDBConnection parent) {
        this.parent = parent;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            ScanResultSet scanResult = this.parent.getConnection().executeScan(parent.getTableSpace(), sql, Collections.emptyList(), parent.ensureTransaction(), maxRows);
            return lastResultSet = new HerdDBResultSet(scanResult);
        } catch (ClientSideMetadataProviderException | HDBException | InterruptedException ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return (int) executeLargeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeUpdate(sql);
        // TODO: better return code
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return lastResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) lastUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Connection getConnection() throws SQLException {
        return parent;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (lastKey != null) {
            return new HerdDBResultSet(new SingletonScanResultSet(lastKey));
        } else {
            return new HerdDBResultSet(new EmptyScanResultSet());
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        try {
            DMLResult result = parent.getConnection().executeUpdate(parent.getTableSpace(), sql, parent.ensureTransaction(), Collections.emptyList());
            lastUpdateCount = result.updateCount;
            lastKey = result.key;
            return lastUpdateCount;
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw new SQLException(err);
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery(sql);
            return true;
        } else {
            executeUpdate(sql);
            return false;
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery(sql);
            return true;
        } else {
            executeUpdate(sql);
            return false;
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery(sql);
            return true;
        } else {
            executeUpdate(sql);
            return false;
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
