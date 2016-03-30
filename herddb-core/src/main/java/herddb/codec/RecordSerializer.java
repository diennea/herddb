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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Record conversion to byte[]
 *
 * @author enrico.olivelli
 */
public final class RecordSerializer {

    private static Object deserialize(byte[] data, int type) {
        switch (type) {
            case ColumnTypes.BYTEARRAY:
                return data;
            case ColumnTypes.INTEGER:
                return new Bytes(data).to_int();
            case ColumnTypes.LONG:
                return new Bytes(data).to_long();
            case ColumnTypes.STRING:
                return new Bytes(data).to_string();
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

    private static byte[] serialize(Object v, int type) {
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
                    return Bytes.from_long((Integer) v).data;
                } else if (v instanceof Number) {
                    return Bytes.from_long(((Number) v).longValue()).data;
                } else {
                    return Bytes.from_long(Long.parseLong(v.toString())).data;
                }
            case ColumnTypes.STRING:
                return Bytes.from_string(v.toString()).data;
            default:
                throw new IllegalArgumentException("bad column type " + type);

        }
    }

    private RecordSerializer() {
    }

    public static Record toRecord(Map<String, Object> record, Table table) {
        // TODO: better serialization
        ByteArrayOutputStream value = new ByteArrayOutputStream();
        Bytes key = null;
        try (DataOutputStream doo = new DataOutputStream(value);) {
            for (Column c : table.columns) {
                Object v = record.get(c.name);;
                byte[] fieldValue = serialize(v, c.type);
                if (fieldValue != null) {
                    if (table.primaryKeyColumn.equals(c.name)) {
                        key = new Bytes(fieldValue);
                    } else {
                        doo.writeUTF(c.name);
                        doo.writeInt(fieldValue.length);
                        doo.write(fieldValue);
                    }
                }
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        if (key == null) {
            throw new IllegalArgumentException("key field " + table.primaryKeyColumn + " cannot be null");
        }
        return new Record(key, new Bytes(value.toByteArray()));
    }

    public static Map<String, Object> toBean(Record record, Table table) {
        try {
            Map<String, Object> res = new HashMap<>();
            res.put(table.primaryKeyColumn, deserialize(record.key.data, table.getColumn(table.primaryKeyColumn).type));

            ByteArrayInputStream s = new ByteArrayInputStream(record.value.data);
            DataInputStream din = new DataInputStream(s);
            while (true) {
                String name;
                try {
                    name = din.readUTF();
                } catch (EOFException ok) {
                    break;
                }
                int len = din.readInt();
                byte[] v = new byte[len];
                din.readFully(v);
                res.put(name, deserialize(v, table.getColumn(name).type));
            }
            return res;
        } catch (IOException err) {
            throw new IllegalArgumentException("malformed record", err);
        }
    }
}
