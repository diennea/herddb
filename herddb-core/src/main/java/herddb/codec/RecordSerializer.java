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
                return new Bytes(data).to_int();
            case ColumnTypes.LONG:
                return new Bytes(data).to_long();
            case ColumnTypes.STRING:
                return new Bytes(data).to_string();
            case ColumnTypes.TIMESTAMP:
                return new Bytes(data).to_timestamp();
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
                return Bytes.from_string(v.toString()).data;
            case ColumnTypes.TIMESTAMP:
                if (!(v instanceof java.sql.Timestamp)) {
                    throw new IllegalArgumentException("bad value type for column " + type + ": required java.sql.Timestamp, but was " + v.getClass());
                }
                return Bytes.from_timestamp((java.sql.Timestamp) v).data;
            default:
                throw new IllegalArgumentException("bad column type " + type);

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
            record.put((String) name, value);
        }
        return toRecord(record, table);
    }

    public static Bytes serializePrimaryKey(Map<String, Object> record, ColumnsList table) {
        return serializePrimaryKey(record, table, table.getPrimaryKey());
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

    public static Object deserializePrimaryKey(byte[] key, Table table) {

        if (table.primaryKey.length == 1) {
            return deserializeSingleColumnPrimaryKey(key, table);
        } else {
            Map<String, Object> result = new HashMap<>();
            deserializeMultiColumnPrimaryKey(key, table, result);
            return result;
        }
    }

    public static Bytes serializeValue(Map<String, Object> record, Table table) {
        ByteArrayOutputStream value = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(value);) {
            for (Column c : table.columns) {
                Object v = record.get(c.name);
                if (v != null && !table.isPrimaryKeyColumn(c.name)) {
                    byte[] fieldValue = serialize(v, c.type);
                    doo.writeVInt(c.serialPosition);
                    doo.writeArray(fieldValue);
                }
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }

        return new Bytes(value.toByteArray());
    }

    public static Record toRecord(Map<String, Object> record, Table table) {
        return new Record(serializePrimaryKey(record, table), serializeValue(record, table));
    }

    private static Object deserializeSingleColumnPrimaryKey(byte[] data, Table table) {
        String primaryKeyColumn = table.primaryKey[0];
        return deserialize(data, table.getColumn(primaryKeyColumn).type);
    }

    public static Map<String, Object> toBean(Record record, Table table) {
        try {
            Map<String, Object> res = new HashMap<>();
            if (table.primaryKey.length == 1) {
                Object key = deserializeSingleColumnPrimaryKey(record.key.data, table);
                res.put(table.primaryKey[0], key);
            } else {
                deserializeMultiColumnPrimaryKey(record.key.data, table, res);
            }

            if (record.value != null && record.value.data.length > 0) {
                ByteArrayInputStream s = new ByteArrayInputStream(record.value.data);
                ExtendedDataInputStream din = new ExtendedDataInputStream(s);
                while (true) {
                    int serialPosition;
                    serialPosition = din.readVIntNoEOFException();
                    if (din.isEof()) {
                        break;
                    }
                    byte[] v = din.readArray();
                    Column col = table.getColumnBySerialPosition(serialPosition);
                    if (col != null) {
                        res.put(col.name, deserialize(v, col.type));
                    }
                }
            }
            return res;
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }

    private static void deserializeMultiColumnPrimaryKey(byte[] data, Table table, Map<String, Object> res) {
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
}
