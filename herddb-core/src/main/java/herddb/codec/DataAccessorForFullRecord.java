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
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.AbstractDataAccessor;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class DataAccessorForFullRecord extends AbstractDataAccessor {

    private final Table table;
    private final Record record;
    private String[] projectedFieldNames;
    private int[] projections;

    public DataAccessorForFullRecord(Table table, Record record) {
        this.table = table;
        this.record = record;
    }

    @Override
    public boolean applyZeroCopyProjection(String[] fieldNames, int[] projections) {
        this.projectedFieldNames = fieldNames;
        this.projections = projections;
        return true;
    }

    @Override
    public Object get(String property) {
        if (projectedFieldNames != null) {
            int i = 0;
            for (String fName : projectedFieldNames) {
                if (fName.equalsIgnoreCase(property)) {
                    return get(i);
                }
                i++;
            }
        }
        try {
            if (table.isPrimaryKeyColumn(property)) {
                return RecordSerializer.accessRawDataFromPrimaryKey(property, record.key, table);
            } else {
                return RecordSerializer.accessRawDataFromValue(property, record.value, table);
            }
        } catch (IOException err) {
            throw new IllegalStateException("bad data:" + err, err);
        }
    }

    @Override
    public Object get(int index) {
        if (projections != null) {
            index = projections[index];
        }
        try {
            if (table.isPrimaryKeyColumn(index)) {
                return RecordSerializer.accessRawDataFromPrimaryKey(index, record.key, table);
            } else {
                return RecordSerializer.accessRawDataFromValue(index, record.value, table);
            }
        } catch (IOException err) {
            throw new IllegalStateException("bad data:" + err, err);
        }
    }

    @Override
    public int getNumFields() {
        if (projectedFieldNames != null) {
            return projectedFieldNames.length;
        }
        return table.columns.length;
    }

    @Override
    public String[] getFieldNames() {
        if (projectedFieldNames != null) {
            return projectedFieldNames;
        }
        return table.columnNames;
    }

    @Override
    public Map<String, Object> toMap() {
        if (projectedFieldNames != null) {
            Map<String, Object> projected = new HashMap<>();
            forEach((k, v) -> projected.put(k, v));
            return projected;
        }
        return record.toBean(table);
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
        if (projectedFieldNames != null) {
            for (int i = 0; i < projectedFieldNames.length; i++) {
                consumer.accept(projectedFieldNames[i], get(i));
            }
            return;
        }

        // no need to create a Map
        if (table.primaryKey.length == 1) {
            String pkField = table.primaryKey[0];
            Object value = RecordSerializer.deserialize(record.key.data, table.getColumn(pkField).type);
            consumer.accept(pkField, value);
        } else {
            try (final SimpleByteArrayInputStream key_in = new SimpleByteArrayInputStream(record.key.data); final ExtendedDataInputStream din = new ExtendedDataInputStream(key_in)) {
                for (String primaryKeyColumn : table.primaryKey) {
                    byte[] value = din.readArray();
                    Object theValue = RecordSerializer.deserialize(value, table.getColumn(primaryKeyColumn).type);
                    consumer.accept(primaryKeyColumn, theValue);
                }
            } catch (IOException err) {
                throw new IllegalStateException("bad data:" + err, err);
            }
        }

        try {
            SimpleByteArrayInputStream s = new SimpleByteArrayInputStream(record.value.data);
            ExtendedDataInputStream din = new ExtendedDataInputStream(s);
            while (!din.isEof()) {
                int serialPosition;
                serialPosition = din.readVIntNoEOFException();
                if (din.isEof()) {
                    break;
                }
                Column col = table.getColumnBySerialPosition(serialPosition);
                if (col != null) {
                    Object value = RecordSerializer.deserializeTypeAndValue(din);
                    consumer.accept(col.name, value);
                } else {
                    // we have to deserialize always the value, even the column is no more present
                    RecordSerializer.skipTypeAndValue(din);
                }
            }
        } catch (IOException err) {
            throw new IllegalStateException("bad data:" + err, err);
        }
    }

    @Override
    public Object[] getValues() {
        if (projectedFieldNames != null) {
            Object[] values = new Object[projectedFieldNames.length];
            for (int i = 0; i < projectedFieldNames.length; i++) {
                values[i] = get(i);
            }
            return values;
        } else {
            return super.getValues();
        }
    }

}
