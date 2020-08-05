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

import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.client.ScanResultSetMetadata;
import herddb.jdbc.utils.SQLExceptionUtils;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ResultSet implementation
 *
 * @author enrico.olivelli
 */
public final class HerdDBResultSet implements ResultSet {

    private static final AtomicLong IDGENERATOR = new AtomicLong();
    private final Long id = IDGENERATOR.incrementAndGet();
    private final ScanResultSet scanResult;
    private DataAccessor actualValue;
    private Object lastValue;
    private final HerdDBStatement parent;
    private final ScanResultSetMetadata metadata;
    private boolean closed;

    HerdDBResultSet(ScanResultSet scanResult) {
        this(scanResult, null);
    }

    HerdDBResultSet(ScanResultSet scanResult, HerdDBStatement parent) {
        this.scanResult = scanResult;
        this.metadata = scanResult.getMetadata();
        this.parent = parent;
        if (parent != null) {
            this.parent.registerResultSet(this);
        }
    }

    @Override
    public boolean next() throws SQLException {
        try {
            if (scanResult.hasNext()) {
                actualValue = scanResult.next();
                return true;
            } else {
                actualValue = null;
                lastValue = null;
                return false;
            }
        } catch (HDBException ex) {
            throw SQLExceptionUtils.wrapException(ex);
        }
    }

    @Override
    public void close() throws SQLException {
        scanResult.close();
        if (parent != null) {
            parent.releaseResultSet(this);
        }
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    private int resolveColumnIndexByName(String columnName) throws SQLException {
        // returns -1 if column not found (non case sensitive)
        return metadata.getColumnPosition(columnName);
    }

    @Override
    public String getString(String columnIndex) throws SQLException {
        return getString(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public boolean getBoolean(String columnIndex) throws SQLException {
        return getBoolean(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public byte getByte(String columnIndex) throws SQLException {
        return getByte(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public short getShort(String columnIndex) throws SQLException {
        return getShort(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public int getInt(String columnIndex) throws SQLException {
        return getInt(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public long getLong(String columnIndex) throws SQLException {
        return getLong(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public float getFloat(String columnIndex) throws SQLException {
        return getFloat(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public double getDouble(String columnIndex) throws SQLException {
        return getDouble(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnIndex, int scale) throws SQLException {
        return getBigDecimal(resolveColumnIndexByName(columnIndex), scale);
    }

    @Override
    public byte[] getBytes(String columnIndex) throws SQLException {
        return getBytes(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public Date getDate(String columnIndex) throws SQLException {
        return getDate(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public Time getTime(String columnIndex) throws SQLException {
        return getTime(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(String columnIndex) throws SQLException {
        return getTimestamp(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(String columnIndex) throws SQLException {
        return getAsciiStream(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public InputStream getUnicodeStream(String columnIndex) throws SQLException {
        return getUnicodeStream(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(String columnIndex) throws SQLException {
        return getBinaryStream(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public String getString(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            return lastValue.toString();
        } else {
            return null;
        }
    }

    private void fillLastValue(int columnLabel) {
        if (columnLabel <= 0) {
            lastValue = null;
        } else {
            lastValue = actualValue.get(columnLabel - 1);
        }
    }

    private void ensureNextCalled() throws SQLException {
        if (actualValue == null) {
            throw new SQLException("please call next()");
        }
    }

    private boolean wasNull;

    @Override
    public boolean getBoolean(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof Number) {
                return ((Number) lastValue).intValue() == 1;
            } else {
                return Boolean.parseBoolean(lastValue.toString());
            }
        } else {
            wasNull = true;
            return false;
        }
    }

    @Override
    public byte getByte(int columnLabel) throws SQLException {
        return (byte) getInt(columnLabel);
    }

    @Override
    public short getShort(int columnLabel) throws SQLException {
        return (short) getInt(columnLabel);
    }

    @Override
    public int getInt(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof Integer) {
                return ((Integer) lastValue);
            }
            if (lastValue instanceof Number) {
                return ((Number) lastValue).intValue();
            }
            if (lastValue instanceof Boolean) {
                return ((Boolean) lastValue) ? 1 : 0;
            }
            try {
                return Integer.parseInt(lastValue.toString());
            } catch (NumberFormatException err) {
                throw buildPrettyPrintConvertionError(columnLabel, "getInt", err);
            }
        } else {
            wasNull = true;
            return 0;
        }
    }

    private SQLException buildPrettyPrintConvertionError(int columnLabel, String method, NumberFormatException err) throws SQLException {
        throw  new SQLException(
                "Value '" + lastValue + "' (" + lastValue.getClass() + ") cannot be converted to an integer value, call was " + method + "(" + columnLabel + "), column names: " + Arrays.asList(
                this.metadata.getColumnNames()), err);
    }

    @Override
    public long getLong(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof Long) {
                return (Long) lastValue;
            }
            if (lastValue instanceof Number) {
                return ((Number) lastValue).longValue();
            }
            try {
                return Long.parseLong(lastValue.toString());
            } catch (NumberFormatException err) {
                throw buildPrettyPrintConvertionError(columnLabel, "getLong", err);
            }
        } else {
            wasNull = true;
            return 0;
        }
    }

    @Override
    public float getFloat(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof Number) {
                return ((Number) lastValue).floatValue();
            }
            return Float.parseFloat(lastValue.toString());
        } else {
            wasNull = true;
            return 0;
        }
    }

    @Override
    public double getDouble(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof Number) {
                return ((Number) lastValue).doubleValue();
            }
            return Double.parseDouble(lastValue.toString());
        } else {
            wasNull = true;
            return 0;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        // we are not taking scale into consideration,
        // this method has been deprecated since Java 1.2 in favour
        // of the method without scale
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof byte[]) {
                return ((byte[]) lastValue);
            } else {
                throw new SQLException("Value " + lastValue + " (" + lastValue.getClass() + ") cannot be interpreted as a byte[]");
            }
        } else {
            wasNull = true;
            return null;
        }
    }

    @Override
    public Date getDate(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof java.sql.Date) {
                return (java.sql.Date) lastValue;
            }
            if (lastValue instanceof java.util.Date) {
                return new java.sql.Date(((java.util.Date) lastValue).getTime());
            }
            try {
                return new java.sql.Date(Long.parseLong(lastValue.toString()));
            } catch (NumberFormatException err) {
                throw buildPrettyPrintConvertionError(columnLabel, "getDate", err);
            }
        } else {
            wasNull = true;
            return null;
        }
    }

    @Override
    public Time getTime(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof java.sql.Time) {
                return (java.sql.Time) lastValue;
            }
            if (lastValue instanceof java.util.Date) { // usually it will be a java.sql.Timestamp
                return new java.sql.Time(((java.util.Date) lastValue).getTime());
            }
            try {
                return new java.sql.Time(Long.parseLong(lastValue.toString()));
            } catch (NumberFormatException err) {
                throw buildPrettyPrintConvertionError(columnLabel, "getTimestamp", err);
            }
        } else {
            wasNull = true;
            return null;
        }
    }

    @Override
    public Timestamp getTimestamp(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof java.sql.Timestamp) {
                return (java.sql.Timestamp) lastValue;
            }
            if (lastValue instanceof java.util.Date) {
                return new java.sql.Timestamp(((java.util.Date) lastValue).getTime());
            }
            try {
                return new java.sql.Timestamp(Long.parseLong(lastValue.toString()));
            } catch (NumberFormatException err) {
                throw buildPrettyPrintConvertionError(columnLabel, "getTimestamp", err);
            }
        } else {
            wasNull = true;
            return null;
        }
    }

    @Override
    public InputStream getAsciiStream(int columnLabel) throws SQLException {
        ClobImpl lob = getClob(columnLabel);
        if (lob == null) {
            return null;
        }
        return lob.getAsciiStream();
    }

    @Override
    public InputStream getUnicodeStream(int columnLabel) throws SQLException {
        ClobImpl lob = getClob(columnLabel);
        if (lob == null) {
            return null;
        }
        return lob.getUnicodeStream();
    }

    @Override
    public InputStream getBinaryStream(int columnLabel) throws SQLException {
        byte[] bytes = getBytes(columnLabel);
        if (bytes != null) {
            return new SimpleByteArrayInputStream(bytes);
        } else {
            return null;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return this.scanResult.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ResultSetMetaData() {
            @Override
            public int getColumnCount() throws SQLException {
                return metadata.getColumnNames().length;
            }

            @Override
            public boolean isAutoIncrement(int column) throws SQLException {
                return false;
            }

            @Override
            public boolean isCaseSensitive(int column) throws SQLException {
                return true;
            }

            @Override
            public boolean isSearchable(int column) throws SQLException {
                return true;
            }

            @Override
            public boolean isCurrency(int column) throws SQLException {
                return false;
            }

            @Override
            public int isNullable(int column) throws SQLException {
                return columnNullable;
            }

            @Override
            public boolean isSigned(int column) throws SQLException {
                return true;
            }

            @Override
            public int getColumnDisplaySize(int column) throws SQLException {
                return 10;
            }

            @Override
            public String getColumnLabel(int column) throws SQLException {
                try {
                    return metadata.getColumnNames()[column - 1];
                } catch (final ArrayIndexOutOfBoundsException err) {
                    throw new SQLException("not such index " + column, err);
                }
            }

            @Override
            public String getColumnName(int column) throws SQLException {
                try {
                    return metadata.getColumnNames()[column - 1];
                } catch (final ArrayIndexOutOfBoundsException err) {
                    throw new SQLException("not such index " + column, err);
                }
            }

            @Override
            public String getSchemaName(int column) throws SQLException {
                return null;
            }

            @Override
            public int getPrecision(int column) throws SQLException {
                return 0;
            }

            @Override
            public int getScale(int column) throws SQLException {
                return 0;
            }

            @Override
            public String getTableName(int column) throws SQLException {
                return null;
            }

            @Override
            public String getCatalogName(int column) throws SQLException {
                return null;
            }

            @Override
            public int getColumnType(int column) throws SQLException {
                return Types.OTHER;
            }

            @Override
            public String getColumnTypeName(int column) throws SQLException {
                return null;
            }

            @Override
            public boolean isReadOnly(int column) throws SQLException {
                return true;
            }

            @Override
            public boolean isWritable(int column) throws SQLException {
                return false;
            }

            @Override
            public boolean isDefinitelyWritable(int column) throws SQLException {
                return false;
            }

            @Override
            public String getColumnClassName(int column) throws SQLException {
                return java.lang.Object.class.getName();
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return (T) this;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return iface.isInstance(this);
            }

        };

    }

    @Override
    public Object getObject(String columnIndex) throws SQLException {
        return getObject(resolveColumnIndexByName(columnIndex));
    }

    @Override
    public Object getObject(int columnLabel) throws SQLException {
        ensureNextCalled();
        fillLastValue(columnLabel);
        if (lastValue != null) {
            wasNull = false;
            if (lastValue instanceof RawString) {
                return lastValue.toString();
            }
            return lastValue;
        } else {
            wasNull = true;
            return null;
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int index = metadata.getColumnPosition(columnLabel);
        if (index <= 0) {
            throw new SQLException("no such column " + columnLabel + ", only " + Arrays.toString(metadata.getColumnNames()));
        }
        return index;
    }

    @Override
    public Reader getCharacterStream(String columnIndex) throws SQLException {
        Clob lob =  getClob(columnIndex);
        if (lob == null) {
            return null;
        }
        return lob.getCharacterStream();
    }

    @Override
    public Reader getCharacterStream(int columnLabel) throws SQLException {
        Clob lob =  getClob(columnLabel);
        if (lob == null) {
            return null;
        }
        return lob.getCharacterStream();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        double val = getDouble(columnIndex);
        if (wasNull()) {
            return null;
        }
        return BigDecimal.valueOf(val);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(resolveColumnIndexByName(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        parent.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return parent.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return parent;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        final byte[] bytes = getBytes(columnIndex);
        if (bytes == null) {
            return null;
        }
        return new BlobImpl(bytes);
    }

    @Override
    public ClobImpl getClob(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        return new ClobImpl(string);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(resolveColumnIndexByName(columnLabel));
    }

    @Override
    public ClobImpl getClob(String columnLabel) throws SQLException {
        return getClob(resolveColumnIndexByName(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String res = getString(columnIndex);
        if (res == null) {
            return null;
        }
        try {
            return new URL(res);
        } catch (MalformedURLException err) {
            throw new SQLException(err);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(resolveColumnIndexByName(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        return new NClob() {
            @Override
            public long length() throws SQLException {
                return string.length();
            }

            @Override
            public String getSubString(long pos, int length) throws SQLException {
                return string.substring((int) pos, (int) length);
            }

            @Override
            public Reader getCharacterStream() throws SQLException {
                return new StringReader(string);
            }

            @Override
            public InputStream getAsciiStream() throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public long position(String searchstr, long start) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public long position(Clob searchstr, long start) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public int setString(long pos, String str) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public int setString(long pos, String str, int offset, int len) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public OutputStream setAsciiStream(long pos) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public Writer setCharacterStream(long pos) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public void truncate(long len) throws SQLException {
                throw new SQLFeatureNotSupportedException("Not supported yet.");
            }

            @Override
            public void free() throws SQLException {
            }

            @Override
            public Reader getCharacterStream(long pos, long length) throws SQLException {
                return new StringReader(this.getSubString(0, (int) length));
            }
        };
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(resolveColumnIndexByName(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not supported yet.");
    }

    @Override
    public <T> T getObject(String columnIndex, Class<T> type) throws SQLException {
        return getObject(resolveColumnIndexByName(columnIndex), type);
    }

    @Override
    public <T> T getObject(int columnLabel, Class<T> type) throws SQLException {
        return (T) getObject(columnLabel);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    Long getId() {
        return id;
    }

    private static class ClobImpl implements Clob {

        private final String string;

        public ClobImpl(String string) {
            this.string = string;
        }

        @Override
        public long length() throws SQLException {
            return string.length();
        }

        @Override
        public String getSubString(long pos, int length) throws SQLException {
            return string.substring((int) pos, (int) length);
        }

        @Override
        public Reader getCharacterStream() throws SQLException {
            return new StringReader(string);
        }

        public InputStream getUnicodeStream() throws SQLException {
            return new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public InputStream getAsciiStream() throws SQLException {
            return new ByteArrayInputStream(string.getBytes(StandardCharsets.US_ASCII));
        }

        @Override
        public long position(String searchstr, long start) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public long position(Clob searchstr, long start) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public int setString(long pos, String str) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public int setString(long pos, String str, int offset, int len) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public OutputStream setAsciiStream(long pos) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public Writer setCharacterStream(long pos) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public void truncate(long len) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public void free() throws SQLException {
        }

        @Override
        public Reader getCharacterStream(long pos, long length) throws SQLException {
            return new StringReader(this.getSubString(0, (int) length));
        }
    }

    private static class BlobImpl implements Blob {

        private final byte[] bytes;

        public BlobImpl(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long length() throws SQLException {
            return bytes.length;
        }

        @Override
        public byte[] getBytes(long pos, int length) throws SQLException {
            if (pos < 0 || pos + length > bytes.length || pos >= Integer.MAX_VALUE) {
                throw new SQLException("invalid parameters");
            }
            return Arrays.copyOfRange(bytes, (int) pos, length);
        }

        @Override
        public InputStream getBinaryStream() throws SQLException {
            return new SimpleByteArrayInputStream(bytes);
        }

        @Override
        public long position(byte[] pattern, long start) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public long position(Blob pattern, long start) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public int setBytes(long pos, byte[] bytes) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public OutputStream setBinaryStream(long pos) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public void truncate(long len) throws SQLException {
            throw new SQLFeatureNotSupportedException("Not supported yet.");
        }

        @Override
        public void free() throws SQLException {
        }

        @Override
        public InputStream getBinaryStream(long pos, long length) throws SQLException {
            if (pos >= Integer.MAX_VALUE || length >= Integer.MAX_VALUE || pos + length > bytes.length) {
                throw new SQLException("Invalid paramters");
            }
            return new ByteArrayInputStream(bytes, (int) pos, (int) length);
        }
    }
}
