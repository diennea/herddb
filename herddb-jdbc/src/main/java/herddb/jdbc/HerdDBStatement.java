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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.DMLResult;
import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.client.impl.EmptyScanResultSet;
import herddb.client.impl.SingletonScanResultSet;
import herddb.jdbc.utils.SQLExceptionUtils;
import herddb.model.TransactionContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Statement
 *
 * @author enrico.olivelli
 */
public class HerdDBStatement implements java.sql.Statement {

    protected static final Pattern EXPECTS_RESULTSET = Pattern.compile("[\\s]*(SELECT|EXPLAIN|SHOW).*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Logger LOG = LoggerFactory.getLogger(HerdDBStatement.class.getName());
    private static final AtomicLong IDGENERATOR = new AtomicLong(1);

    protected final HerdDBConnection parent;
    protected int maxRows;
    protected int fetchSize = 1000;
    protected ResultSet lastResultSet;
    protected long lastUpdateCount = -1;
    protected Object lastKey;
    private final Long id = IDGENERATOR.incrementAndGet();
    private final ConcurrentHashMap<Long, HerdDBResultSet> openResultSets = new ConcurrentHashMap<>();
    private boolean closed;

    public HerdDBStatement(HerdDBConnection parent) {
        this.parent = parent;
        this.parent.registerOpenStatement(this);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            parent.discoverTableSpace(sql);
            ScanResultSet scanResult = this.parent.getConnection()
                    .executeScan(parent.getTableSpace(), sql, false, Collections.emptyList(), parent.ensureTransaction(),
                            maxRows, fetchSize, parent.isKeepReadLocks());
            parent.bindToTransaction(scanResult.transactionId);
            return lastResultSet = new HerdDBResultSet(scanResult, this);
        } catch (ClientSideMetadataProviderException | HDBException | InterruptedException ex) {
            throw SQLExceptionUtils.wrapException(ex);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return (int) executeLargeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        for (HerdDBResultSet rs : openResultSets.values()) {
            LOG.debug("closing abandoned result set {}", rs);
            try {
                rs.close();
            } catch (SQLException err) {
                LOG.error("Error while closing open resultset", err);
            }
        }
        openResultSets.clear();
        parent.releaseStatement(this);
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

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

    }

    private int queryTimeout;

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    protected boolean moreResults = false;

    @Override
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    public boolean execute(String sql) throws SQLException {
        if (EXPECTS_RESULTSET.matcher(sql).matches()) {
            executeQuery(sql);
            moreResults = true;
            return true;
        } else {
            executeUpdate(sql);
            moreResults = false;
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        moreResults = false;
        return lastResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) lastUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        lastUpdateCount = -1;
        return moreResults;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD
                && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLFeatureNotSupportedException("setFetchDirection " + direction);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet, only with PreparedStatement");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet, only with PreparedStatement");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet, only with PreparedStatement");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return parent;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (lastKey != null) {
            return new HerdDBResultSet(new SingletonScanResultSet(TransactionContext.NOTRANSACTION_ID, lastKey), this);
        } else {
            return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID), this);
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return (int) executeLargeUpdateImpl(sql, autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return (int) executeLargeUpdateImpl(sql, columnIndexes != null && columnIndexes.length > 0);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return (int) executeLargeUpdateImpl(sql, columnNames != null && columnNames.length > 0);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return executeLargeUpdateImpl(sql, false);
    }

    private long executeLargeUpdateImpl(String sql, boolean returnValues) throws SQLException {
        try {
            parent.discoverTableSpace(sql);
            DMLResult result = parent.getConnection()
                    .executeUpdate(parent.getTableSpace(),
                            sql, parent.ensureTransaction(), returnValues, false, Collections.emptyList());
            parent.bindToTransaction(result.transactionId);
            lastUpdateCount = result.updateCount;
            lastKey = result.key;
            return lastUpdateCount;
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw SQLExceptionUtils.wrapException(err);
        }
    }

    @Override
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery(sql);
            return true;
        } else {
            executeUpdate(sql, autoGeneratedKeys);
            return false;
        }
    }

    @Override
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery(sql);
            return true;
        } else {
            executeUpdate(sql, columnIndexes);
            return false;
        }
    }

    @Override
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery(sql);
            return true;
        } else {
            executeUpdate(sql, columnNames);
            return false;
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    final Long getId() {
        return id;
    }

    void releaseResultSet(HerdDBResultSet rs) {
        openResultSets.remove(rs.getId());
    }

    void registerResultSet(HerdDBResultSet rs) {
        openResultSets.put(rs.getId(), rs);
    }
}
