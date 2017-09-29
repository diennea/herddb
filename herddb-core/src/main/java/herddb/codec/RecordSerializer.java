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
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import herddb.model.ColumnsList;
import herddb.model.StatementExecutionException;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.SingleEntryMap;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.function.BiConsumer;

/**
 * Record conversion to byte[]
 *
 * @author enrico.olivelli
 */
public final class RecordSerializer {

    public static Object deserialize(byte[] data, int type) {
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return data;
            case ColumnTypes.INTEGER:
                return Bytes.toInt(data, 0, 4);
            case ColumnTypes.LONG:
                return Bytes.toLong(data, 0, 8);
            case ColumnTypes.STRING:
                return Bytes.to_rawstring(data);
            case ColumnTypes.TIMESTAMP:
                return Bytes.toTimestamp(data, 0, 8);
            case ColumnTypes.NULL:
                return null;
            case ColumnTypes.BOOLEAN:
                return Bytes.toBoolean(data, 0, 1);
            case ColumnTypes.DOUBLE:
                return Bytes.toDouble(data, 0, 8);
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    public static Object deserializeTypeAndValue(ExtendedDataInputStream dii) throws IOException {
        int type = dii.readVInt();
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return dii.readArray();
            case ColumnTypes.INTEGER:
                return dii.readInt();
            case ColumnTypes.LONG:
                return dii.readLong();
            case ColumnTypes.STRING:
                return new RawString(dii.readArray());
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

    public static void skipTypeAndValue(ExtendedDataInputStream dii) throws IOException {
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
                    return Bytes.from_int((Integer) v).data;
                } else if (v instanceof Number) {
                    return Bytes.from_int(((Number) v).intValue()).data;
                } else {
                    return Bytes.from_int(Integer.parseInt(v.toString())).data;
                }
            case ColumnTypes.LONG:
                if (v instanceof Long) {
                    return Bytes.from_long((Long) v).data;
                } else if (v instanceof Number) {
                    return Bytes.from_long(((Number) v).longValue()).data;
                } else {
                    return Bytes.from_long(Long.parseLong(v.toString())).data;
                }
            case ColumnTypes.STRING:
                if (v instanceof RawString) {
                    RawString rs = (RawString) v;
                    return rs.data;
                } else {
                    return Bytes.string_to_array(v.toString());
                }
            case ColumnTypes.BOOLEAN:
                if (v instanceof Boolean) {
                    return Bytes.from_boolean((Boolean) v).data;
                } else {
                    return Bytes.from_boolean(Boolean.parseBoolean(v.toString())).data;
                }
            case ColumnTypes.DOUBLE:
                if (v instanceof Double) {
                    return Bytes.from_double((Double) v).data;
                } else if (v instanceof Long) {
                    return Bytes.from_double((Long) v).data;
                } else if (v instanceof Number) {
                    return Bytes.from_double(((Number) v).longValue()).data;
                } else {
                    return Bytes.from_double(Double.parseDouble(v.toString())).data;
                }
            case ColumnTypes.TIMESTAMP:
                if (!(v instanceof java.sql.Timestamp)) {
                    throw new IllegalArgumentException("bad value type for column " + type + ": required java.sql.Timestamp, but was " + v.getClass() + ", toString of value is " + v);
                }
                return Bytes.from_timestamp((java.sql.Timestamp) v).data;
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
                    oo.writeArray(rs.data);
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
                    return ((RawString) value).data;
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
        SimpleByteArrayInputStream s = new SimpleByteArrayInputStream(value.data);
        ExtendedDataInputStream din = new ExtendedDataInputStream(s);
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

    static Object accessRawDataFromPrimaryKey(String property, Bytes key, Table table) throws IOException {
        if (table.primaryKey.length == 1) {
            return deserialize(key.data, table.getColumn(property).type);
        } else {
            try (SimpleByteArrayInputStream key_in = new SimpleByteArrayInputStream(key.data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(key_in)) {
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
        ByteArrayOutputStream key = new ByteArrayOutputStream();
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
                    byte[] fieldValue = serialize(v, c.type);
                    doo_key.writeArray(fieldValue);
                    i++;
                }
            } catch (IOException err) {
                throw new RuntimeException(err);
            }
            return new Bytes(key.toByteArray());
        }
    }

    public static Bytes serializePrimaryKey(DataAccessor record, ColumnsList table, String[] columns) {
        ByteArrayOutputStream key = new ByteArrayOutputStream();
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
                    byte[] fieldValue = serialize(v, c.type);
                    doo_key.writeArray(fieldValue);
                    i++;
                }
            } catch (IOException err) {
                throw new RuntimeException(err);
            }
            return new Bytes(key.toByteArray());
        }
    }

    public static Object deserializePrimaryKey(byte[] key, Table table) {

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
            Object value = deserializeSingleColumnPrimaryKey(key.data, table);
            // value will not be null
            result = new SingleEntryMap(table.primaryKey[0], value);
        } else {
            result = new HashMap<>();
            deserializeMultiColumnPrimaryKey(key.data, table, result);
        }
        key.deserialized = result;
        return result;
    }

    public static Bytes serializeValue(Map<String, Object> record, Table table) {
        ByteArrayOutputStream value = new ByteArrayOutputStream();
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

        return new Bytes(value.toByteArray());
    }

    public static Record toRecord(Map<String, Object> record, Table table) {
        return new Record(serializePrimaryKey(record, table, table.primaryKey),
            serializeValue(record, table), record);
    }

    private static Object deserializeSingleColumnPrimaryKey(byte[] data, Table table) {
        String primaryKeyColumn = table.primaryKey[0];
        return deserialize(data, table.getColumn(primaryKeyColumn).type);
    }

    public static Map<String, Object> toBean(Record record, Table table) {
        try {
            ImmutableMap.Builder<String, Object> res = new ImmutableMap.Builder<>();

            if (table.primaryKey.length == 1) {
                Object key = deserializeSingleColumnPrimaryKey(record.key.data, table);
                res.put(table.primaryKey[0], key);
            } else {
                deserializeMultiColumnPrimaryKey(record.key.data, table, res);
            }

            if (record.value != null && record.value.data.length > 0) {
                SimpleByteArrayInputStream s = new SimpleByteArrayInputStream(record.value.data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(s);
                while (true) {
                    int serialPosition;
                    serialPosition = din.readVIntNoEOFException();
                    if (din.isEof()) {
                        break;
                    }
                    // we have to deserialize always the value, even the column is no more present
                    Object v = deserializeTypeAndValue(din);
                    Column col = table.getColumnBySerialPosition(serialPosition);
                    if (col != null) {
                        res.put(col.name, v);
                    }
                }
            }
            return res.build();
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    private static void deserializeMultiColumnPrimaryKey(byte[] data, Table table, Map<String, Object> res) {
        try (SimpleByteArrayInputStream key_in = new SimpleByteArrayInputStream(data);
            ExtendedDataInputStream din = new ExtendedDataInputStream(key_in)) {
            for (String primaryKeyColumn : table.primaryKey) {
                byte[] value = din.readArray();
                res.put(primaryKeyColumn, deserialize(value, table.getColumn(primaryKeyColumn).type));
            }
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    private static void deserializeMultiColumnPrimaryKey(byte[] data, Table table, ImmutableMap.Builder<String, Object> res) {
        try (ByteArrayInputStream key_in = new ByteArrayInputStream(data);
            ExtendedDataInputStream din = new ExtendedDataInputStream(key_in)) {
            for (String primaryKeyColumn : table.primaryKey) {
                byte[] value = din.readArray();
                res.put(primaryKeyColumn, deserialize(value, table.getColumn(primaryKeyColumn).type));
            }
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    private static class DataAccessorForPrimaryKey implements DataAccessor {

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
                Object value = deserialize(key.data, table.getColumn(pkField).type);
                consumer.accept(pkField, value);
            } else {
                try (SimpleByteArrayInputStream key_in = new SimpleByteArrayInputStream(key.data);
                    ExtendedDataInputStream din = new ExtendedDataInputStream(key_in)) {
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
