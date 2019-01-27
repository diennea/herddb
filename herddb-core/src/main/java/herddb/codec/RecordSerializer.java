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
package herddb.codec;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import herddb.model.ColumnsList;
import herddb.model.StatementExecutionException;
import herddb.utils.AbstractDataAccessor;
import herddb.utils.ByteArrayCursor;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import herddb.utils.SQLRecordPredicateFunctions;
import herddb.utils.SingleEntryMap;
import herddb.utils.SystemProperties;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Record conversion to byte[]
 *
 * @author enrico.olivelli
 */
public final class RecordSerializer {

    private static final int INITIAL_BUFFER_SIZE = SystemProperties.getIntSystemProperty("herddb.serializer.initbufsize", 4 * 1024);

    public static Object deserialize(Bytes data, int type) {
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return data.to_array();
            case ColumnTypes.INTEGER:
                return data.to_int();
            case ColumnTypes.LONG:
                return data.to_long();
            case ColumnTypes.STRING:
                return data.to_RawString();
            case ColumnTypes.TIMESTAMP:
                return data.to_timestamp();
            case ColumnTypes.NULL:
                return null;
            case ColumnTypes.BOOLEAN:
                return data.to_boolean();
            case ColumnTypes.DOUBLE:
                return data.to_double();
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }
    
    public static Object deserialize(byte[] data, int type) {
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return data;
            case ColumnTypes.INTEGER:
                return Bytes.toInt(data, 0);
            case ColumnTypes.LONG:
                return Bytes.toLong(data, 0);
            case ColumnTypes.STRING:
                return Bytes.to_rawstring(data);
            case ColumnTypes.TIMESTAMP:
                return Bytes.toTimestamp(data, 0);
            case ColumnTypes.NULL:
                return null;
            case ColumnTypes.BOOLEAN:
                return Bytes.toBoolean(data, 0);
            case ColumnTypes.DOUBLE:
                return Bytes.toDouble(data, 0);
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    public static int deserializeCompare(byte[] data, int type, Object cvalue) {
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return SQLRecordPredicateFunctions.compare(data, cvalue);
            case ColumnTypes.INTEGER:
                if (cvalue instanceof Integer) {
                    return Bytes.compareInt(data, 0, (int) cvalue);
                } else if (cvalue instanceof Long) {
                    return Bytes.compareInt(data, 0, (long) cvalue);
                }
                return SQLRecordPredicateFunctions.compare(Bytes.toInt(data, 0), cvalue);
            case ColumnTypes.LONG:
                if (cvalue instanceof Integer) {
                    return Bytes.compareLong(data, 0, (int) cvalue);
                } else if (cvalue instanceof Long) {
                    return Bytes.compareLong(data, 0, (long) cvalue);
                }
                return SQLRecordPredicateFunctions.compare(Bytes.toLong(data, 0), cvalue);
            case ColumnTypes.STRING:
                if (cvalue instanceof RawString) {
                    return RawString.compareRaw(data, 0, data.length, ((RawString) cvalue));
                } else if (cvalue instanceof String) {
                    return RawString.compareRaw(data, 0, data.length, ((String) cvalue));
                }
                return SQLRecordPredicateFunctions.compare(Bytes.to_rawstring(data), cvalue);
            case ColumnTypes.TIMESTAMP:
                return SQLRecordPredicateFunctions.compare(Bytes.toTimestamp(data, 0), cvalue);
            case ColumnTypes.NULL:
                return SQLRecordPredicateFunctions.compareNullTo(cvalue);
            case ColumnTypes.BOOLEAN:
                return SQLRecordPredicateFunctions.compare(Bytes.toBoolean(data, 0), cvalue);
            case ColumnTypes.DOUBLE:
                return SQLRecordPredicateFunctions.compare(Bytes.toDouble(data, 0), cvalue);
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    public static Object deserializeTypeAndValue(ByteArrayCursor dii) throws IOException {
        int type = dii.readVInt();
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return dii.readArray();
            case ColumnTypes.INTEGER:
                return dii.readInt();
            case ColumnTypes.LONG:
                return dii.readLong();
            case ColumnTypes.STRING:
                return dii.readRawStringNoCopy();
            case ColumnTypes.TIMESTAMP:
                return new java.sql.Timestamp(dii.readLong());
            case ColumnTypes.NULL:
                return null;
            case ColumnTypes.BOOLEAN:
                return dii.readBoolean();
            case ColumnTypes.DOUBLE:
                return dii.readDouble();
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    public static int compareDeserializeTypeAndValue(ByteArrayCursor dii, Object cvalue) throws IOException {
        int type = dii.readVInt();
        switch (type) {
            case ColumnTypes.BYTEARRAY: {
                byte[] datum = dii.readArray();
                return SQLRecordPredicateFunctions.compare(datum, cvalue);
            }
            case ColumnTypes.INTEGER:
                return SQLRecordPredicateFunctions.compare(dii.readInt(), cvalue);
            case ColumnTypes.LONG:
                return SQLRecordPredicateFunctions.compare(dii.readLong(), cvalue);
            case ColumnTypes.STRING:
                int len = dii.readArrayLen();
                if (cvalue instanceof RawString) {
                    RawString _cvalue = (RawString) cvalue;
                    return RawString.compareRaw(dii.getArray(), dii.getPosition(), len, _cvalue);
                } else if (cvalue instanceof String) {
                    String _cvalue = (String) cvalue;
                    return RawString.compareRaw(dii.getArray(), dii.getPosition(), len, _cvalue);
                } else {
                    RawString value = dii.readRawStringNoCopy();
                    return SQLRecordPredicateFunctions.compare(value, cvalue);
                }

            case ColumnTypes.TIMESTAMP:
                return SQLRecordPredicateFunctions.compare(new java.sql.Timestamp(dii.readLong()), cvalue);
            case ColumnTypes.NULL:
                return SQLRecordPredicateFunctions.compareNullTo(cvalue);
            case ColumnTypes.BOOLEAN:
                return SQLRecordPredicateFunctions.compare(dii.readBoolean(), cvalue);
            case ColumnTypes.DOUBLE:
                return SQLRecordPredicateFunctions.compare(dii.readDouble(), cvalue);
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    public static void skipTypeAndValue(ByteArrayCursor dii) throws IOException {
        int type = dii.readVInt();
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                dii.skipArray();
                break;
            case ColumnTypes.INTEGER:
                dii.skipInt();
                break;
            case ColumnTypes.LONG:
                dii.skipLong();
                break;
            case ColumnTypes.STRING:
                dii.skipArray();
                break;
            case ColumnTypes.TIMESTAMP:
                dii.skipLong();
                break;
            case ColumnTypes.NULL:
                break;
            case ColumnTypes.BOOLEAN:
                dii.skipBoolean();
                break;
            case ColumnTypes.DOUBLE:
                dii.skipDouble();
                break;
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    public static byte[] serialize(Object v, int type) {
        if (v == null) {
            return null;
        }
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return (byte[]) v;
            case ColumnTypes.INTEGER:
                if (v instanceof Integer) {
                    return Bytes.intToByteArray((Integer) v);
                } else if (v instanceof Number) {
                    return Bytes.intToByteArray(((Number) v).intValue());
                } else {
                    return Bytes.intToByteArray(Integer.parseInt(v.toString()));
                }
            case ColumnTypes.LONG:
                if (v instanceof Long) {
                    return Bytes.longToByteArray((Long) v);
                } else if (v instanceof Number) {
                    return Bytes.longToByteArray(((Number) v).longValue());
                } else {
                    return Bytes.longToByteArray(Long.parseLong(v.toString()));
                }
            case ColumnTypes.STRING:
                if (v instanceof RawString) {
                    RawString rs = (RawString) v;
                    // this will potentially make a copy
                    return rs.toByteArray();
                } else {
                    return Bytes.string_to_array(v.toString());
                }
            case ColumnTypes.BOOLEAN:
                if (v instanceof Boolean) {
                    return Bytes.booleanToByteArray((Boolean) v);
                } else {
                    return Bytes.booleanToByteArray(Boolean.parseBoolean(v.toString()));
                }
            case ColumnTypes.DOUBLE:
                if (v instanceof Double) {
                    return Bytes.doubleToByteArray((Double) v);
                } else if (v instanceof Long) {
                    return Bytes.doubleToByteArray((Long) v);
                } else if (v instanceof Number) {
                    return Bytes.doubleToByteArray(((Number) v).longValue());
                } else {
                    return Bytes.doubleToByteArray(Double.parseDouble(v.toString()));
                }
            case ColumnTypes.TIMESTAMP:
                if (v instanceof Long) {
                    return Bytes.timestampToByteArray(new java.sql.Timestamp(((Long) v)));
                }
                if (!(v instanceof java.sql.Timestamp)) {
                    throw new IllegalArgumentException("bad value type for column " + type + ": required java.sql.Timestamp, but was " + v.getClass() + ", toString of value is " + v);
                }
                return Bytes.timestampToByteArray((java.sql.Timestamp) v);
            default:
                throw new IllegalArgumentException("bad column type " + type);

        }
    }

    public static void serializeTo(Object v, int type, ExtendedDataOutputStream out) throws IOException {
        if (v == null) {
            out.writeNullArray();
            return;
        }
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                out.writeArray((byte[]) v);
                return;
            case ColumnTypes.INTEGER:
                if (v instanceof Integer) {
                    out.writeArray(Bytes.intToByteArray((Integer) v));
                } else if (v instanceof Number) {
                    out.writeArray(Bytes.intToByteArray(((Number) v).intValue()));
                } else {
                    out.writeArray(Bytes.intToByteArray(Integer.parseInt(v.toString())));
                }
                return;
            case ColumnTypes.LONG:
                if (v instanceof Long) {
                    out.writeArray(Bytes.longToByteArray((Long) v));
                } else if (v instanceof Number) {
                    out.writeArray(Bytes.longToByteArray(((Number) v).longValue()));
                } else {
                    out.writeArray(Bytes.longToByteArray(Long.parseLong(v.toString())));
                }
                return;
            case ColumnTypes.STRING:
                if (v instanceof RawString) {
                    RawString rs = (RawString) v;
                    out.writeArray(rs.getData(), rs.getOffset(), rs.getLength());
                } else {
                    out.writeArray(Bytes.string_to_array(v.toString()));
                }
                return;
            case ColumnTypes.BOOLEAN:
                if (v instanceof Boolean) {
                    out.writeArray(Bytes.booleanToByteArray((Boolean) v));
                } else {
                    out.writeArray(Bytes.booleanToByteArray(Boolean.parseBoolean(v.toString())));
                }
                return;
            case ColumnTypes.DOUBLE:
                if (v instanceof Double) {
                    out.writeArray(Bytes.doubleToByteArray((Double) v));
                } else if (v instanceof Long) {
                    out.writeArray(Bytes.doubleToByteArray((Long) v));
                } else if (v instanceof Number) {
                    out.writeArray(Bytes.doubleToByteArray(((Number) v).longValue()));
                } else {
                    out.writeArray(Bytes.doubleToByteArray(Double.parseDouble(v.toString())));
                }
                return;
            case ColumnTypes.TIMESTAMP:
                if (v instanceof Long) {
                    out.writeArray(Bytes.timestampToByteArray(new java.sql.Timestamp(((Long) v))));
                    return;
                }
                if (!(v instanceof java.sql.Timestamp)) {
                    throw new IllegalArgumentException("bad value type for column " + type + ": required java.sql.Timestamp, but was " + v.getClass() + ", toString of value is " + v);
                }
                out.writeArray(Bytes.timestampToByteArray((java.sql.Timestamp) v));
                return;
            default:
                throw new IllegalArgumentException("bad column type " + type);

        }
    }

    /**
     * Same as {@link #serialize(java.lang.Object, int) } but without objects
     * allocations
     *
     * @param v
     * @param type
     */
    public static void validate(Object v, int type) {
        if (v == null) {
            return;
        }
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                if (!(v instanceof byte[])) {
                    throw new IllegalArgumentException();
                }
                return;
            case ColumnTypes.INTEGER:
                if (v instanceof Number) {
                    return;
                }
                Integer.parseInt(v.toString());
                return;
            case ColumnTypes.LONG:
                if (v instanceof Number) {
                    return;
                }
                Long.parseLong(v.toString());
                return;
            case ColumnTypes.STRING:
                return;
            case ColumnTypes.BOOLEAN:
                return;
            case ColumnTypes.DOUBLE:
                if (v instanceof Number) {
                    return;
                }
                Double.parseDouble(v.toString());
                return;
            case ColumnTypes.TIMESTAMP:
                if (v instanceof Long || v instanceof java.sql.Timestamp) {
                    return;
                }
                throw new IllegalArgumentException("bad value type for column " + type + ": required java.sql.Timestamp, but was " + v.getClass() + ", toString of value is " + v);
            default:
                throw new IllegalArgumentException("bad column type " + type);

        }
    }

    public static void serializeTypeAndValue(Object v, int type, ExtendedDataOutputStream oo) throws IOException {
        if (v == null) {
            return;
        }
        oo.writeVInt(type);
        serializeValue(v, type, oo);
    }

    public static void serializeValue(Object v, int type, ExtendedDataOutputStream oo) throws IOException {
        if (v == null) {
            throw new IOException("You cannot serialize a null value");
        }
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                oo.writeArray((byte[]) v);
                break;
            case ColumnTypes.INTEGER:
                if (v instanceof Integer) {
                    oo.writeInt((Integer) v);
                } else if (v instanceof Number) {
                    oo.writeInt(((Number) v).intValue());
                } else {
                    oo.writeInt(Integer.parseInt(v.toString()));
                }
                break;
            case ColumnTypes.LONG:
                if (v instanceof Integer) {
                    oo.writeLong((Integer) v);
                } else if (v instanceof Number) {
                    oo.writeLong(((Number) v).longValue());
                } else {
                    oo.writeLong(Long.parseLong(v.toString()));
                }
                break;
            case ColumnTypes.STRING:
                if (v instanceof RawString) {
                    RawString rs = (RawString) v;
                    oo.writeArray(rs.getData(), rs.getOffset(), rs.getLength());
                } else {
                    oo.writeArray(Bytes.string_to_array(v.toString()));
                }
                break;
            case ColumnTypes.TIMESTAMP:
                if (!(v instanceof java.sql.Timestamp)) {
                    throw new IllegalArgumentException("bad value type for column " + type + ": required java.sql.Timestamp, but was " + v.getClass() + ", toString of value is " + v);
                }
                oo.writeLong(((java.sql.Timestamp) v).getTime());
                break;
            case ColumnTypes.BOOLEAN:
                if (v instanceof Boolean) {
                    oo.writeBoolean((Boolean) v);
                } else {
                    oo.writeBoolean(Boolean.parseBoolean(v.toString()));
                }
                break;
            case ColumnTypes.DOUBLE:
                if (v instanceof Integer) {
                    oo.writeDouble((Integer) v);
                } else if (v instanceof Number) {
                    oo.writeDouble(((Number) v).doubleValue());
                } else {
                    oo.writeDouble(Double.parseDouble(v.toString()));
                }
                break;
            default:
                throw new IllegalArgumentException("bad column type " + type);

        }
    }

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(UTC);

    public static final DateTimeFormatter getUTCTimestampFormatter() {
        return TIMESTAMP_FORMATTER;
    }

    public static Object convert(int type, Object value) throws StatementExecutionException {
        switch (type) {
            case ColumnTypes.TIMESTAMP:
                if ((value instanceof java.sql.Timestamp)) {
                    return value;
                } else if (value instanceof RawString
                        || value instanceof String) {
                    try {

                        ZonedDateTime dateTime = ZonedDateTime.parse(value.toString(), TIMESTAMP_FORMATTER);
                        Instant toInstant = dateTime.toInstant();
                        long millis = (toInstant.toEpochMilli());
                        Timestamp timestamp = new java.sql.Timestamp(millis);
                        if (timestamp.getTime() != millis) {
                            throw new StatementExecutionException("Unparsable timestamp " + value + " would been converted as java.sql.Timestamp to " + new java.sql.Timestamp(millis));
                        }
                        return timestamp;
                    } catch (DateTimeParseException err) {
                        throw new StatementExecutionException("Unparsable timestamp " + value, err);
                    }
                }
            case ColumnTypes.BYTEARRAY:
                if (value instanceof RawString) {
                    // TODO: apply a real conversion from MySQL dump format
                    return ((RawString) value).toByteArray();
                }
                return value;
            default:
                return value;
        }
    }

    public static DataAccessor buildRawDataAccessor(Record record, Table table) {
        return new DataAccessorForFullRecord(table, record);
    }

    public static DataAccessor buildRawDataAccessorForPrimaryKey(Bytes key, Table table) {
        return new DataAccessorForPrimaryKey(table, key);

    }

    static Object accessRawDataFromValue(String property, Bytes value, Table table) throws IOException {
        if (table.getColumn(property) == null) {
            throw new herddb.utils.IllegalDataAccessException("table " + table.tablespace + "." + table.name + " does not define column " + property);
        }
        try (ByteArrayCursor din = value.newCursor();) {
            while (!din.isEof()) {
                int serialPosition;
                serialPosition = din.readVIntNoEOFException();
                if (din.isEof()) {
                    return null;
                }
                Column col = table.getColumnBySerialPosition(serialPosition);
                if (col != null && col.name.equals(property)) {
                    return deserializeTypeAndValue(din);
                } else {
                    // we have to deserialize always the value, even the column is no more present
                    skipTypeAndValue(din);
                }
            }
            return null;
        }
    }

    static Object accessRawDataFromValue(int index, Bytes value, Table table) throws IOException {
        Column column = table.getColumn(index);
        try (ByteArrayCursor din = value.newCursor();) {
            while (!din.isEof()) {
                int serialPosition;
                serialPosition = din.readVIntNoEOFException();
                if (din.isEof()) {
                    return null;
                }
                Column col = table.getColumnBySerialPosition(serialPosition);
                if (col != null && col.serialPosition == column.serialPosition) {
                    return deserializeTypeAndValue(din);
                } else {
                    // we have to deserialize always the value, even the column is no more present
                    skipTypeAndValue(din);
                }
            }
            return null;
        }
    }

    static int compareRawDataFromValue(int index, Bytes value, Table table, Object cvalue) throws IOException {
        Column column = table.getColumn(index);
        try (ByteArrayCursor din = value.newCursor()) {
            while (!din.isEof()) {
                int serialPosition;
                serialPosition = din.readVIntNoEOFException();
                if (din.isEof()) {
                    return SQLRecordPredicateFunctions.compareNullTo(cvalue);
                }
                Column col = table.getColumnBySerialPosition(serialPosition);
                if (col != null && col.serialPosition == column.serialPosition) {
                    return compareDeserializeTypeAndValue(din, cvalue);
                } else {
                    // we have to deserialize always the value, even the column is no more present
                    skipTypeAndValue(din);
                }
            }
        }
        return SQLRecordPredicateFunctions.compareNullTo(cvalue);
    }

    static Object accessRawDataFromPrimaryKey(String property, Bytes key, Table table) throws IOException {
        if (table.primaryKey.length == 1) {
            return deserialize(key, table.getColumn(property).type);
        } else {
            try (ByteArrayCursor din = key.newCursor();) {
                for (String primaryKeyColumn : table.primaryKey) {
                    byte[] value = din.readArray();
                    if (primaryKeyColumn.equals(property)) {
                        return deserialize(value, table.getColumn(primaryKeyColumn).type);
                    }
                }
            }
            throw new IOException("property " + property + " not found in PK: " + Arrays.toString(table.primaryKey));
        }
    }

    static Object accessRawDataFromPrimaryKey(int index, Bytes key, Table table) throws IOException {
        Column column = table.getColumn(index);
        if (table.primaryKey.length == 1) {
            return deserialize(key, column.type);
        } else {
            final String cname = column.name;
            try (ByteArrayCursor din = key.newCursor();) {
                for (String primaryKeyColumn : table.primaryKey) {
                    byte[] value = din.readArray();
                    if (primaryKeyColumn.equals(cname)) {
                        return deserialize(value, table.getColumn(primaryKeyColumn).type);
                    }
                }
            }
            throw new IOException("position #" + index + " not found in PK: " + Arrays.toString(table.primaryKey));
        }
    }

    static int compareRawDataFromPrimaryKey(int index, Bytes key, Table table, Object cvalue) throws IOException {
        Column column = table.getColumn(index);
        if (table.primaryKey.length == 1) {
            return deserializeCompare(key.data, column.type, cvalue);
        } else {
            final String cname = column.name;
            try (ByteArrayCursor din = key.newCursor()) {
                for (String primaryKeyColumn : table.primaryKey) {
                    byte[] value = din.readArray();
                    if (primaryKeyColumn.equals(cname)) {
                        return deserializeCompare(value, table.getColumn(primaryKeyColumn).type, cvalue);
                    }
                }
            }
            throw new IOException("position #" + index + " not found in PK: " + Arrays.toString(table.primaryKey));
        }
    }

    private RecordSerializer() {
    }

    public static Record makeRecord(Table table, Object... values) {
        Map<String, Object> record = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            Object name = values[i++];
            Object value = values[i];
            name = table.getColumn((String) name).name;
            if (value instanceof String) {
                value = RawString.of((String) value);
            }
            record.put((String) name, value);
        }
        return toRecord(record, table);
    }

    public static Bytes serializePrimaryKey(Map<String, Object> record, ColumnsList table, String[] columns) {
        return new Bytes(serializePrimaryKeyRaw(record, table, columns));
    }

    public static byte[] serializePrimaryKeyRaw(Map<String, Object> record, ColumnsList table, String[] columns) {
        String[] primaryKey = table.getPrimaryKey();
        if (primaryKey.length == 1) {
            String pkColumn = primaryKey[0];
            if (columns.length != 1 && !columns[0].equals(pkColumn)) {
                throw new IllegalArgumentException("SQLTranslator error, " + Arrays.toString(columns) + " != " + Arrays.asList(pkColumn));
            }
            Column c = table.getColumn(pkColumn);
            Object v = record.get(c.name);
            if (v == null) {
                throw new IllegalArgumentException("key field " + pkColumn + " cannot be null. Record data: " + record);
            }
            return serialize(v, c.type);
        } else {
            ByteArrayOutputStream key = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
            // beware that we can serialize even only a part of the PK, for instance of a prefix index scan            
            try (ExtendedDataOutputStream doo_key = new ExtendedDataOutputStream(key);) {
                int i = 0;
                for (String pkColumn : columns) {
                    if (!pkColumn.equals(primaryKey[i])) {
                        throw new IllegalArgumentException("SQLTranslator error, " + Arrays.toString(columns) + " != " + Arrays.asList(primaryKey));
                    }
                    Column c = table.getColumn(pkColumn);
                    Object v = record.get(c.name);
                    if (v == null) {
                        throw new IllegalArgumentException("key field " + pkColumn + " cannot be null. Record data: " + record);
                    }
                    serializeTo(v, c.type, doo_key);
                    i++;
                }
            } catch (IOException err) {
                throw new RuntimeException(err);
            }
            return key.toByteArray();
        }
    }

    public static Bytes serializePrimaryKey(DataAccessor record, ColumnsList table, String[] columns) {
        String[] primaryKey = table.getPrimaryKey();
        if (primaryKey.length == 1) {
            String pkColumn = primaryKey[0];
            if (columns.length != 1 && !columns[0].equals(pkColumn)) {
                throw new IllegalArgumentException("SQLTranslator error, " + Arrays.toString(columns) + " != " + Arrays.asList(pkColumn));
            }
            Column c = table.getColumn(pkColumn);
            Object v = record.get(c.name);
            if (v == null) {
                throw new IllegalArgumentException("key field " + pkColumn + " cannot be null. Record data: " + record);
            }
            byte[] fieldValue = serialize(v, c.type);
            return new Bytes(fieldValue);
        } else {
            ByteArrayOutputStream key = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
            // beware that we can serialize even only a part of the PK, for instance of a prefix index scan
            try (ExtendedDataOutputStream doo_key = new ExtendedDataOutputStream(key);) {
                int i = 0;
                for (String pkColumn : columns) {
                    if (!pkColumn.equals(primaryKey[i])) {
                        throw new IllegalArgumentException("SQLTranslator error, " + Arrays.toString(columns) + " != " + Arrays.asList(primaryKey));
                    }
                    Column c = table.getColumn(pkColumn);
                    Object v = record.get(c.name);
                    if (v == null) {
                        throw new IllegalArgumentException("key field " + pkColumn + " cannot be null. Record data: " + record);
                    }
                    serializeTo(v, c.type, doo_key);
                    i++;
                }
            } catch (IOException err) {
                throw new RuntimeException(err);
            }
            return new Bytes(key.toByteArray());
        }
    }

    /**
     * Like {@link #serializePrimaryKey(herddb.utils.DataAccessor, herddb.model.ColumnsList, java.lang.String[])
     * } but without return a value and/or creating temporary byte[]
     *
     * @param record
     * @param table
     * @param columns
     */
    public static void validatePrimaryKey(DataAccessor record, ColumnsList table, String[] columns) {
        String[] primaryKey = table.getPrimaryKey();
        if (primaryKey.length == 1) {
            String pkColumn = primaryKey[0];
            if (columns.length != 1 && !columns[0].equals(pkColumn)) {
                throw new IllegalArgumentException("SQLTranslator error, " + Arrays.toString(columns) + " != " + Arrays.asList(pkColumn));
            }
            Column c = table.getColumn(pkColumn);
            Object v = record.get(c.name);
            if (v == null) {
                throw new IllegalArgumentException("key field " + pkColumn + " cannot be null. Record data: " + record);
            }
            validate(v, c.type);
        } else {
            // beware that we can serialize even only a part of the PK, for instance of a prefix index scan
            int i = 0;
            for (String pkColumn : columns) {
                if (!pkColumn.equals(primaryKey[i])) {
                    throw new IllegalArgumentException("SQLTranslator error, " + Arrays.toString(columns) + " != " + Arrays.asList(primaryKey));
                }
                Column c = table.getColumn(pkColumn);
                Object v = record.get(c.name);
                if (v == null) {
                    throw new IllegalArgumentException("key field " + pkColumn + " cannot be null. Record data: " + record);
                }
                validate(v, c.type);
                i++;
            }
        }
    }

    public static Object deserializePrimaryKey(Bytes key, Table table) {

        if (table.primaryKey.length == 1) {
            return deserializeSingleColumnPrimaryKey(key, table);
        } else {
            Map<String, Object> result = new HashMap<>();
            deserializeMultiColumnPrimaryKey(key, table, result);
            return result;
        }
    }

    public static Map<String, Object> deserializePrimaryKeyAsMap(Bytes key, Table table) {
        if (key.deserialized != null) {
            return (Map<String, Object>) key.deserialized;
        }
        Map<String, Object> result;
        if (table.primaryKey.length == 1) {
            Object value = deserializeSingleColumnPrimaryKey(key, table);
            // value will not be null
            result = new SingleEntryMap(table.primaryKey[0], value);
        } else {
            result = new HashMap<>();
            deserializeMultiColumnPrimaryKey(key, table, result);
        }
        key.deserialized = result;
        return result;
    }

    public static Bytes serializeValue(Map<String, Object> record, Table table) {
        return new Bytes(serializeValueRaw(record, table, 0));
    }

    public static byte[] serializeValueRaw(Map<String, Object> record, Table table, int expectedSize) {
        ByteArrayOutputStream value = new ByteArrayOutputStream(expectedSize <= 0 ? INITIAL_BUFFER_SIZE : expectedSize);
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(value);) {
            for (Column c : table.columns) {
                Object v = record.get(c.name);
                if (v != null && !table.isPrimaryKeyColumn(c.name)) {
                    doo.writeVInt(c.serialPosition);
                    serializeTypeAndValue(v, c.type, doo);
                }
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }

        return value.toByteArray();
    }

    public static byte[] buildRecord(int expectedSize, Table table,
            Function<String, Object> evaluator) {
        ByteArrayOutputStream value = new ByteArrayOutputStream(expectedSize <= 0 ? INITIAL_BUFFER_SIZE : expectedSize);
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(value);) {
            for (Column c : table.columns) {
                if (!table.isPrimaryKeyColumn(c.name)) {
                    Object v = evaluator.apply(c.name);
                    if (v != null) {
                        doo.writeVInt(c.serialPosition);
                        serializeTypeAndValue(v, c.type, doo);
                    }
                }
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }

        return value.toByteArray();
    }

    public static Record toRecord(Map<String, Object> record, Table table) {
        return new Record(serializePrimaryKey(record, table, table.primaryKey),
                serializeValue(record, table), record);
    }

    private static Object deserializeSingleColumnPrimaryKey(Bytes data, Table table) {
        String primaryKeyColumn = table.primaryKey[0];
        return deserialize(data, table.getColumn(primaryKeyColumn).type);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
    public static Map<String, Object> toBean(Record record, Table table) {
        try {
            ImmutableMap.Builder<String, Object> res = new ImmutableMap.Builder<>();

            if (table.primaryKey.length == 1) {
                Object key = deserializeSingleColumnPrimaryKey(record.key, table);
                res.put(table.primaryKey[0], key);
            } else {
                deserializeMultiColumnPrimaryKey(record.key, table, res);
            }

            if (record.value != null && record.value.getLength() > 0) {
                try (ByteArrayCursor din = record.value.newCursor();) {
                    while (true) {
                        int serialPosition;
                        serialPosition = din.readVIntNoEOFException();
                        if (din.isEof()) {
                            break;
                        }
                        Column col = table.getColumnBySerialPosition(serialPosition);

                        // we have to deserialize or skip always the value, even the column is no more present
                        if (col != null) {
                            Object v = deserializeTypeAndValue(din);
                            res.put(col.name, v);
                        } else {
                            skipTypeAndValue(din);
                        }
                    }
                }
            }
            return res.build();
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    private static void deserializeMultiColumnPrimaryKey(Bytes data, Table table, Map<String, Object> res) {
        try (ByteArrayCursor din = data.newCursor()) {
            for (String primaryKeyColumn : table.primaryKey) {
                byte[] value = din.readArray();
                res.put(primaryKeyColumn, deserialize(value, table.getColumn(primaryKeyColumn).type));
            }
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
    private static void deserializeMultiColumnPrimaryKey(Bytes data, Table table, ImmutableMap.Builder<String, Object> res) {
        try (ByteArrayCursor din = data.newCursor()) {
            for (String primaryKeyColumn : table.primaryKey) {
                byte[] value = din.readArray();
                res.put(primaryKeyColumn, deserialize(value, table.getColumn(primaryKeyColumn).type));
            }
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    private static class DataAccessorForPrimaryKey extends AbstractDataAccessor {

        private final Table table;
        private final Bytes key;

        public DataAccessorForPrimaryKey(Table table, Bytes key) {
            this.table = table;
            this.key = key;
        }

        @Override
        public Object get(String property) {
            try {
                if (table.isPrimaryKeyColumn(property)) {
                    return accessRawDataFromPrimaryKey(property, key, table);
                } else {
                    return null;
                }
            } catch (IOException err) {
                throw new IllegalStateException("bad data:" + err, err);
            }
        }

        @Override
        public void forEach(BiConsumer<String, Object> consumer) {
            if (table.primaryKey.length == 1) {
                String pkField = table.primaryKey[0];
                Object value = deserialize(key, table.getColumn(pkField).type);
                consumer.accept(pkField, value);
            } else {
                try (ByteArrayCursor din = key.newCursor()) {
                    for (String primaryKeyColumn : table.primaryKey) {
                        byte[] value = din.readArray();
                        Object theValue = deserialize(value, table.getColumn(primaryKeyColumn).type);
                        consumer.accept(primaryKeyColumn, theValue);
                    }
                } catch (IOException err) {
                    throw new IllegalStateException("bad data:" + err, err);
                }
            }
        }

        @Override
        public String[] getFieldNames() {
            return table.primaryKey;
        }

        @Override
        public Map<String, Object> toMap() {
            return deserializePrimaryKeyAsMap(key, table);
        }
    }
}
