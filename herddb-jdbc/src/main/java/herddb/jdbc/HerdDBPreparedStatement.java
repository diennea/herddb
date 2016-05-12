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
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * SQL Statement
 *
 * @author enrico.olivelli
 */
public class HerdDBPreparedStatement extends HerdDBStatement implements PreparedStatement {

    private final String sql;
    private final List<Object> parameters = new ArrayList<>();

    public HerdDBPreparedStatement(HerdDBConnection parent, String sql) {
        super(parent);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            ScanResultSet scanResult = this.parent.getConnection().executeScan(parent.getTableSpace(), sql, parameters, maxRows);
            return lastResultSet = new HerdDBResultSet(scanResult);
        } catch (ClientSideMetadataProviderException | HDBException | InterruptedException ex) {
            throw new SQLException(ex);
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
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
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
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    public boolean execute() throws SQLException {
        if (sql.toLowerCase().contains("select")) {
            executeQuery();
            return true;
        } else {
            executeUpdate();
            return false;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isSigned(int param) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getPrecision(int param) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getScale(int param) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getParameterType(int param) throws SQLException {
                return java.sql.Types.OTHER;
            }

            @Override
            public String getParameterTypeName(int param) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public String getParameterClassName(int param) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getParameterMode(int param) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureParameterPos(parameterIndex);
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int) executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            ScanResultSet scanResult = this.parent.getConnection().executeScan(parent.getTableSpace(), sql, Collections.emptyList(), maxRows);
            return new HerdDBResultSet(scanResult);
        } catch (ClientSideMetadataProviderException | HDBException | InterruptedException ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public void close() throws SQLException {
        parameters.clear();
    }

    
    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            DMLResult result = parent.getConnection().executeUpdate(parent.getTableSpace(), sql, parent.ensureTransaction(), parameters);
            lastUpdateCount = result.updateCount;
            lastKey = result.key;
            return lastUpdateCount;
        } catch (ClientSideMetadataProviderException | HDBException err) {
            throw new SQLException(err);
        }
    }

}
