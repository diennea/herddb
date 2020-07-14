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
import herddb.jdbc.utils.SQLExceptionUtils;
import herddb.utils.FileUtils;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * SQL Statement
 *
 * @author enrico.olivelli
 */
public class HerdDBPreparedStatement extends HerdDBStatement implements PreparedStatementAsync {

    private final String sql;
    private final List<Object> parameters = new ArrayList<>();
    private final List<List<Object>> batch = new ArrayList<>();
    private final boolean returnValues;

    public HerdDBPreparedStatement(HerdDBConnection parent, String sql, boolean returnValues) {
        super(parent);
        this.sql = sql;
        this.returnValues = returnValues;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            parent.discoverTableSpace(sql);
            ScanResultSet scanResult = this.parent.getConnection()
                    .executeScan(parent.getTableSpace(), sql, true, parameters, parent.ensureTransaction(), maxRows,
                            fetchSize);
            this.parent.bindToTransaction(scanResult.transactionId);
            return lastResultSet = new HerdDBResultSet(scanResult, this);
        } catch (ClientSideMetadataProviderException | HDBException | InterruptedException ex) {
            throw SQLExceptionUtils.wrapException(ex);
        }
    }

    private void ensureParameterPos(int index) {
        while (parameters.size() < index) {
            parameters.add(null);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setInt(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        // TODO: this is an approximation
        setDouble(parameterIndex, x.doubleValue());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            // this is intentionally converted to java.sql.Timestamp
            setTimestamp(parameterIndex, new java.sql.Timestamp(x.getTime()));
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            // this is intentionally converted to java.sql.Timestamp
            setTimestamp(parameterIndex, new java.sql.Timestamp(x.getTime()));
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        // in ASCII length in bytes == length in chars
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.US_ASCII), length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.UTF_8), length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    public boolean execute() throws SQLException {
        if (EXPECTS_RESULTSET.matcher(sql).matches()) {
            executeQuery();
            moreResults = true;
            return true;
        } else {
            executeLargeUpdate();
            moreResults = false;
            return false;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        batch.add(new ArrayList<>(parameters));
    }

    @Override
    public int[] executeBatch() throws SQLException {
        try {
            int[] results = new int[batch.size()];
            int i = 0;

            lastUpdateCount = 0;
            parent.discoverTableSpace(sql);
            List<DMLResult> dmlresults = parent.getConnection().executeUpdates(
                    parent.getTableSpace(), sql,
                    parent.ensureTransaction(), false, true, this.batch);

            for (DMLResult dmlresult : dmlresults) {
                results[i++] = (int) dmlresult.updateCount;
                parent.bindToTransaction(dmlresult.transactionId);
                lastUpdateCount += dmlresult.updateCount;
                lastKey = dmlresult.key;
            }
            return results;
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw SQLExceptionUtils.wrapException(err);
        } finally {
            batch.clear();
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        batch.clear();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (long) length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException("setRef is not supported yet");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setBinaryStream(parameterIndex, x.getBinaryStream());
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setCharacterStream(parameterIndex, x.getCharacterStream());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException("setArray is not supported yet");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Not supported yet.");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            // this is intentionally converted to java.sql.Timestamp
            setTimestamp(parameterIndex, new java.sql.Timestamp(x.getTime()), cal);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            // this is intentionally converted to java.sql.Timestamp
            setTimestamp(parameterIndex, new java.sql.Timestamp(x.getTime()), cal);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex, x.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new ParameterMetaData() {
            @Override
            public int getParameterCount() throws SQLException {
                return (int) sql.chars().filter(ch -> ch == '?').count();

            }

            @Override
            public int isNullable(int param) throws SQLException {
                return ParameterMetaData.parameterNullableUnknown;
            }

            @Override
            public boolean isSigned(int param) throws SQLException {
                // unknown at client side
                return true;
            }

            @Override
            public int getPrecision(int param) throws SQLException {
                // unknown at client side
                return 0;
            }

            @Override
            public int getScale(int param) throws SQLException {
                // unknown at client side
                return 0;
            }

            @Override
            public int getParameterType(int param) throws SQLException {
                // unknown at client side
                return java.sql.Types.OTHER;
            }

            @Override
            public String getParameterTypeName(int param) throws SQLException {
                // unknown at client side
                return "object";
            }

            @Override
            public String getParameterClassName(int param) throws SQLException {
                // unknown at client side
                return Object.class.getName();
            }

            @Override
            public int getParameterMode(int param) throws SQLException {
                return ParameterMetaData.parameterModeIn;
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return (T) this;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return false;
            }
        };
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException("setRowId not supported yet.");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setNCharacterStream(parameterIndex, value.getCharacterStream());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setNCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setNCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        setNCharacterStream(parameterIndex, xmlObject.getCharacterStream());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        // in ASCII length in bytes == length in chars
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.US_ASCII), length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (length >= Integer.MAX_VALUE) {
            throw new SQLException("Cannot send a byte[] larger than " + Integer.MAX_VALUE);
        }
        try {
            VisibleByteArrayOutputStream out = new VisibleByteArrayOutputStream((int) length);
            // one day we will be able to use InputStream#transferTo
            long writtenCount = FileUtils.copyStreams(x, out, length);
            if (writtenCount != length) {
                throw new SQLException("The supplied inputstream returned only " + writtenCount + " bytes, exptected " + length);
            }
            // toByteArrayNoCopy won't create an additional copy if not needed
            setBytes(parameterIndex, out.toByteArrayNoCopy());
        } catch (IOException ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        if (length >= Integer.MAX_VALUE) {
            throw new SQLException("Cannot handle a value larger that " + Integer.MAX_VALUE + " chars");
        }
        try {
            CharBuffer buffer = CharBuffer.allocate((int) length);
            int readCount = reader.read(buffer);
            if (readCount != length) {
                throw new IOException("short read from " + reader + ", read " + readCount + " characters, expected " + length);
            }
            buffer.flip();
            setString(parameterIndex, buffer.toString());
        } catch (IOException iOException) {
            throw new SQLException(iOException);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setCharacterStream(parameterIndex, new InputStreamReader(x, StandardCharsets.US_ASCII));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            VisibleByteArrayOutputStream out = new VisibleByteArrayOutputStream();
            // one day we will be able to use InputStream#transferTo
            FileUtils.copyStreams(x, out);
            setBytes(parameterIndex, out.toByteArray());
        } catch (IOException ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            // one day we will be able to use Reader#transferTo
            char[] arr = new char[8 * 1024];
            StringBuilder buffer = new StringBuilder();
            int numCharsRead;
            while ((numCharsRead = reader.read(arr, 0, arr.length)) != -1) {
                buffer.append(arr, 0, numCharsRead);
            }
            setString(parameterIndex, buffer.toString());
        } catch (IOException iOException) {
            throw new SQLException(iOException);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) executeLargeUpdate();
    }

    @Override
    public void close() throws SQLException {
        parameters.clear();
        super.close();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return doExecuteLargeUpdateWithParameters(parameters, returnValues);
    }

    private long doExecuteLargeUpdateWithParameters(List<Object> actualParameters, boolean returnValues) throws SQLException {
        try {
            parent.discoverTableSpace(sql);
            DMLResult result = parent.getConnection().executeUpdate(parent.getTableSpace(),
                    sql, parent.ensureTransaction(), returnValues, true, actualParameters);
            parent.bindToTransaction(result.transactionId);
            lastUpdateCount = result.updateCount;
            lastKey = result.key;
            return lastUpdateCount;
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw SQLExceptionUtils.wrapException(err);
        }
    }

    @Override
    public CompletableFuture<Long> executeLargeUpdateAsync() {
        return doExecuteLargeUpdateWithParametersAsync(parameters, returnValues);
    }

    @Override
    public CompletableFuture<Integer> executeUpdateAsync() {
        return doExecuteLargeUpdateWithParametersAsync(parameters, returnValues)
                .thenApply(Number::intValue);
    }

    private CompletableFuture<Long> doExecuteLargeUpdateWithParametersAsync(List<Object> actualParameters,
                                                                            boolean returnValues) {
        CompletableFuture<Long> res = new CompletableFuture<>();

        lastUpdateCount = 0;
        long tx;
        try {
            parent.discoverTableSpace(sql);
            tx = parent.ensureTransaction();
        } catch (SQLException err) {
            res.completeExceptionally(err);
            return res;
        }
        parent.getConnection()
                .executeUpdateAsync(parent.getTableSpace(),
                        sql, tx, returnValues, true, actualParameters)
                .whenComplete((dmlres, error) -> {
                    if (error != null) {
                        res.completeExceptionally(SQLExceptionUtils.wrapException(error));
                        return;
                    }
                    parent.bindToTransaction(dmlres.transactionId);
                    lastUpdateCount = dmlres.updateCount;
                    lastKey = dmlres.key;
                    res.complete(dmlres.updateCount);
                });
        return res;

    }

    @Override
    public <T> T unwrap(Class<T> clazz) throws SQLException {
        if (clazz.isAssignableFrom(PreparedStatementAsync.class)) {
            return (T) this;
        }
        return super.unwrap(clazz);
    }

    @Override
    public CompletableFuture<int[]> executeBatchAsync() {
        CompletableFuture<int[]> res = new CompletableFuture<>();

        lastUpdateCount = 0;
        long tx;
        try {
            parent.discoverTableSpace(sql);
            tx = parent.ensureTransaction();
        } catch (SQLException err) {
            res.completeExceptionally(err);
            return res;
        }

        parent.getConnection().executeUpdatesAsync(
                parent.getTableSpace(), sql,
                tx, false, true, this.batch)
                .whenComplete((dmsresults, error) -> {
                    if (error != null) {
                        res.completeExceptionally(SQLExceptionUtils.wrapException(error));
                    } else {
                        int[] results = new int[batch.size()];
                        int i = 0;
                        for (DMLResult dmlresult : dmsresults) {
                            results[i++] = (int) dmlresult.updateCount;
                            parent.bindToTransaction(dmlresult.transactionId);
                            lastUpdateCount += dmlresult.updateCount;
                            lastKey = dmlresult.key;
                        }
                        res.complete(results);
                    }
                    batch.clear();
                });

        return res;
    }

    public boolean isPoolable() throws SQLException {
        return true;
    }

}
