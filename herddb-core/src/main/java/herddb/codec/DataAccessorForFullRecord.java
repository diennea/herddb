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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.Column;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.AbstractDataAccessor;
import herddb.utils.ByteArrayCursor;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class DataAccessorForFullRecord extends AbstractDataAccessor {

    private final Table table;
    private final Record record;

    public DataAccessorForFullRecord(Table table, Record record) {
        this.table = table;
        this.record = record;
    }

    @Override
    public Object get(String property) {
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
    public boolean fieldEqualsTo(int index, Object value) {
        try {
            if (table.isPrimaryKeyColumn(index)) {
                return RecordSerializer.compareRawDataFromPrimaryKey(index, record.key, table, value) == 0;
            } else {
                return RecordSerializer.compareRawDataFromValue(index, record.value, table, value) == 0;
            }
        } catch (IOException err) {
            throw new IllegalStateException("bad data:" + err, err);
        }
    }

    @Override
    public int fieldCompareTo(int index, Object value) {
        try {
            if (table.isPrimaryKeyColumn(index)) {
                return RecordSerializer.compareRawDataFromPrimaryKey(index, record.key, table, value);
            } else {
                return RecordSerializer.compareRawDataFromValue(index, record.value, table, value);
            }
        } catch (IOException err) {
            throw new IllegalStateException("bad data:" + err, err);
        }
    }

    @Override
    public int getNumFields() {
        return table.columns.length;
    }

    @Override
    public String[] getFieldNames() {
        return table.columnNames;
    }

    @Override
    public Map<String, Object> toMap() {
        return record.toBean(table);
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
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

        try (ByteArrayCursor din = ByteArrayCursor.wrap(record.value.data);) {
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
        return super.getValues();
    }

    @Override
    public String toString() {
        return "DataAccessorForFullRecord{" + "record=" + record + '}';
    }

}
