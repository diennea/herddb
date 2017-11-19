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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.codec.RecordSerializer;
import herddb.utils.AbstractDataAccessor;
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.RawString;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A tuple of values
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public final class Tuple extends AbstractDataAccessor {

    public final String[] fieldNames;

    private Map<String, Object> map;
    private Object[] values;

    private void buildValues() {
        if (values != null) {
            return;
        }
        int i = 0;
        Object[] newValues = new Object[fieldNames.length];
        for (String name : fieldNames) {
            newValues[i++] = map.get(name);
        }
        values = newValues;
    }

    public Tuple(String[] fieldNames, Object[] values) {
        if (fieldNames == null || values == null) {
            throw new NullPointerException();
        }
        this.fieldNames = fieldNames;
        this.values = values;
        if (fieldNames.length != values.length) {
            throw new IllegalArgumentException();
        }
    }

    public Tuple(Map<String, Object> record) {
        if (record == null) {
            throw new NullPointerException();
        }
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

    public Tuple(Map<String, Object> record, String[] fieldNames) {
        if (record == null) {
            throw new NullPointerException();
        }
        this.fieldNames = fieldNames;
        this.map = record;
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
        if (map != null) {
            map.forEach(consumer);
        } else {
            for (int i = 0; i < fieldNames.length; i++) {
                consumer.accept(fieldNames[i], values[i]);
            }
        }
    }

    public int size() {
        return values.length;
    }

    @Override
    public Object get(String column) {
        if (map != null) {
            return map.get(column);
        }
        return toMap().get(column);
    }

    @Override
    public Object[] getValues() {
        if (values == null) {
            buildValues();
        }
        return values;
    }

    @Override
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
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public String toString() {
        return "Tuple{" + "values=" + Arrays.toString(values) + ", fieldNames=" + Arrays.toString(fieldNames) + '}';
    }

    @Override
    public Object get(int i) {
        buildValues();
        return values[i];
    }

    public static VisibleByteArrayOutputStream serialize(DataAccessor tuple, Column[] columns) throws IOException {
        VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1024);
        try (ExtendedDataOutputStream eoo = new ExtendedDataOutputStream(oo);) {
            int i = 0;
            String[] fieldNames = tuple.getFieldNames();
            for (String fieldName : fieldNames) {
                if (!columns[i].name.equals(fieldName)) {
                    throw new IOException("invalid schema for tuple " + Arrays.toString(fieldNames) + " <> " + Arrays.toString(columns));
                }
                Object value = tuple.get(fieldName);
                if (value == null) {
                    eoo.writeVInt(ColumnTypes.NULL);
                } else {
                    byte columnType;
                    if (value instanceof String) {
                        columnType = ColumnTypes.STRING;
                    } else if (value instanceof RawString) {
                        columnType = ColumnTypes.STRING;
                    } else if (value instanceof Integer) {
                        columnType = ColumnTypes.INTEGER;
                    } else if (value instanceof Long) {
                        columnType = ColumnTypes.LONG;
                    } else if (value instanceof java.sql.Timestamp) {
                        columnType = ColumnTypes.TIMESTAMP;
                    } else if (value instanceof Double) {
                        columnType = ColumnTypes.DOUBLE;
                    } else if (value instanceof Boolean) {
                        columnType = ColumnTypes.BOOLEAN;
                    } else if (value instanceof byte[]) {
                        columnType = ColumnTypes.BYTEARRAY;
                    } else {
                        throw new IOException("unsupported class " + value.getClass());
                    }
                    RecordSerializer.serializeTypeAndValue(value, columnType, eoo);
                }
                i++;
            }
        }
        return oo;
    }

    public static Tuple deserialize(final byte[] data, final String[] fieldNames, final int nColumns) throws IOException {
        try (ExtendedDataInputStream eoo = new ExtendedDataInputStream(new SimpleByteArrayInputStream(data));) {

            List<Object> values = new ArrayList<>();
            for (int i = 0; i < nColumns; i++) {
                Object value = RecordSerializer.deserializeTypeAndValue(eoo);
                values.add(value);
            }
            Object[] _values = values.toArray(new Object[nColumns]);
            return new Tuple(fieldNames, _values);

        }
    }
}
