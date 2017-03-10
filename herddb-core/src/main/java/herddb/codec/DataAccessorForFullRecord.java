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
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class DataAccessorForFullRecord implements DataAccessor {

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
    public String[] getFieldNames() {
        return table.columnNames;
    }

    @Override
    public Map<String, Object> toMap() {
        return record.toBean(table);
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
        List<String> nonNullFields = new ArrayList<>(table.columnNames.length - table.primaryKey.length);
        // no need to create a Map
        if (table.primaryKey.length == 1) {
            String pkField = table.primaryKey[0];
            Object value = RecordSerializer.deserialize(record.key.data, table.getColumn(pkField).type);
            consumer.accept(pkField, value);
            nonNullFields.add(pkField);
        } else {
            try (final SimpleByteArrayInputStream key_in = new SimpleByteArrayInputStream(record.key.data); final ExtendedDataInputStream din = new ExtendedDataInputStream(key_in)) {
                for (String primaryKeyColumn : table.primaryKey) {
                    byte[] value = din.readArray();
                    Object theValue = RecordSerializer.deserialize(value, table.getColumn(primaryKeyColumn).type);
                    consumer.accept(primaryKeyColumn, theValue);
                    nonNullFields.add(primaryKeyColumn);
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
                    nonNullFields.add(col.name);
                    consumer.accept(col.name, value);
                } else {
                    // we have to deserialize always the value, even the column is no more present
                    RecordSerializer.skipTypeAndValue(din);
                }
            }
        } catch (IOException err) {
            throw new IllegalStateException("bad data:" + err, err);
        }
        for (String field : table.columnNames) {
            if (!nonNullFields.contains(field)) {
                consumer.accept(field, null);
            }
        }
    }

}
