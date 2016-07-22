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
package herddb.model;

import herddb.codec.RecordSerializer;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A tuple of values
 *
 * @author enrico.olivelli
 */
public class Tuple {

    /**
     * Effetctive values. This array shoould be threated as immutable
     */
    public final Object[] values;

    public final String[] fieldNames;

    private Map<String, Object> map;

    public Tuple(String[] fieldNames, Object[] values) {
        this.fieldNames = fieldNames;
        this.values = values;
        if (fieldNames.length != values.length) {
            throw new IllegalArgumentException();
        }
    }

    public Tuple(Map<String, Object> record) {
        int size = record.size();
        this.fieldNames = new String[size];
        this.values = new Object[size];
        this.map = record;
        int i = 0;
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            fieldNames[i] = entry.getKey();
            values[i++] = entry.getValue();
        }
    }

    public Tuple(Map<String, Object> record, Column[] columns) {
        int size = columns.length;
        this.fieldNames = new String[size];
        this.values = new Object[size];
        this.map = record;
        int i = 0;
        for (Column c : columns) {
            fieldNames[i] = c.name;
            values[i++] = record.get(c.name);
        }
    }

    public int size() {
        return values.length;
    }

    public Map<String, Object> toMap() {
        if (map != null) {
            return map;
        }
        HashMap _map = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            _map.put(fieldNames[i], values[i]);
        }
        this.map = _map;
        return _map;
    }

    @Override
    public String toString() {
        return "Tuple{" + "values=" + Arrays.toString(values) + ", fieldNames=" + Arrays.toString(fieldNames) + '}';
    }

    public Object get(int i) {
        return values[i];
    }

    public Object get(String name) {
        return toMap().get(name);
    }

    public byte[] serialize(Column[] columns) throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();

        try (ExtendedDataOutputStream eoo = new ExtendedDataOutputStream(oo);) {
            int i = 0;
            for (String fieldName : fieldNames) {
                if (!columns[i].name.equals(fieldName)) {
                    throw new IOException("invalid schema for tuple " + Arrays.toString(fieldNames) + " <> " + Arrays.toString(columns));
                }
                Object value = values[i];
                if (value == null) {
                    eoo.writeVInt(ColumnTypes.NULL);
                } else {
                    byte columnType;
                    if (value instanceof String) {
                        columnType = ColumnTypes.STRING;
                    } else if (value instanceof Integer) {
                        columnType = ColumnTypes.INTEGER;
                    } else if (value instanceof Long) {
                        columnType = ColumnTypes.LONG;
                    } else if (value instanceof java.sql.Timestamp) {
                        columnType = ColumnTypes.TIMESTAMP;
                    } else if (value instanceof byte[]) {
                        columnType = ColumnTypes.BYTEARRAY;
                    } else {
                        throw new IOException("unsupported class " + value.getClass());
                    }
                    eoo.writeVInt(columnType);
                    eoo.writeArray(RecordSerializer.serialize(value, columnType));
                }
                i++;
            }
        }
        return oo.toByteArray();
    }

    public static Tuple deserialize(byte[] data, String[] fieldNames, Column[] columns) throws IOException {
        try (ExtendedDataInputStream eoo = new ExtendedDataInputStream(new ByteArrayInputStream(data));) {

            List<Object> values = new ArrayList<>();
            for (Column column : columns) {
                int type = eoo.readVInt();
                Object value;
                if (type == ColumnTypes.NULL) {
                    value = null;
                } else {
                    byte[] _value = eoo.readArray();
                    value = RecordSerializer.deserialize(_value, type);
                }
                values.add(value);
            }
            return new Tuple(
                    fieldNames,
                    values.toArray(new Object[values.size()]));

        }
    }
}
