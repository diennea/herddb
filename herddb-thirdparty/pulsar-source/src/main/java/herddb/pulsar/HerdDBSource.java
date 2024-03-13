/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.pulsar;

import herddb.cdc.ChangeDataCapture;
import herddb.client.ClientConfiguration;
import herddb.codec.DataAccessorForFullRecord;
import herddb.log.LogSequenceNumber;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Table;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HerdDBSource extends PushSource<KeyValue<GenericRecord, GenericRecord>>
        implements ChangeDataCapture.MutationListener  {

    private static final Logger LOG = Logger.getLogger(HerdDBSource.class.getName());

    private ChangeDataCapture changeDataCapture;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        String tableSpaceUUID = (String) config.get("tableSpaceUUID");
        String url = (String) config.get("url");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.readJdbcUrl(url);
        // TODO: support checkpoints
        LogSequenceNumber startingPosition = LogSequenceNumber.START_OF_TIME;
        changeDataCapture = new ChangeDataCapture(tableSpaceUUID, clientConfig, this, startingPosition, new InMemoryTableHistoryStorage());
    }

    @Override
    public void close() throws Exception {
       if (changeDataCapture != null) {
           changeDataCapture.close();
       }
    }

    @Override
    public void accept(ChangeDataCapture.Mutation mutation) {
        Record<KeyValue<GenericRecord, GenericRecord>> record = buildRecord(mutation);
        this.consume(record);
    }

    private Record<KeyValue<GenericRecord, GenericRecord>> buildRecord(ChangeDataCapture.Mutation mutation) {
        LogSequenceNumber logSequenceNumber = mutation.getLogSequenceNumber();
        Table table = mutation.getTable();
        ChangeDataCapture.MutationType mutationType = mutation.getMutationType();
        DataAccessorForFullRecord record = mutation.getRecord();
        long timestamp = mutation.getTimestamp();
        LOG.log(Level.INFO, "buildRecord for {0}", mutation);

        KeyValueSchema<GenericRecord, GenericRecord> schema = buildSchema(table);
        KeyValue<GenericRecord, GenericRecord> pulsarRecord = buildRecord(schema, table, mutationType, record);
        return new KVRecord<GenericRecord, GenericRecord>() {

            @Override
            public KeyValue<GenericRecord, GenericRecord> getValue() {
                return pulsarRecord;
            }

            @Override
            public Schema getKeySchema() {
                return schema.getKeySchema();
            }

            @Override
            public Schema getValueSchema() {
                return schema.getValueSchema();
            }

            @Override
            public KeyValueEncodingType getKeyValueEncodingType() {
                return KeyValueEncodingType.SEPARATED;
            }
        };
    }

    private KeyValue<GenericRecord, GenericRecord> buildRecord(KeyValueSchema<GenericRecord, GenericRecord> schema,
                                                               Table table, ChangeDataCapture.MutationType mutationType,
                                                               DataAccessorForFullRecord record) {
        GenericRecordBuilder keyBuilder = ((GenericSchema<GenericRecord>) schema.getKeySchema()).newRecordBuilder();
        GenericRecordBuilder valueBuilder = ((GenericSchema<GenericRecord>) schema.getValueSchema()).newRecordBuilder();
        for (Column col : table.columns) {
            boolean isPk = table.isPrimaryKeyColumn(col.name);
            GenericRecordBuilder builder = isPk ? keyBuilder : valueBuilder;
            Object value = record.get(col.name);
            builder.set(col.name, value);
        }
        return new KeyValue(keyBuilder.build(), valueBuilder.build());
    }

    private KeyValueSchema<GenericRecord, GenericRecord> buildSchema(Table table) {
        Schema<GenericRecord> keySchema = buildSchema(table.name+"Key", table.primaryKey, table);
        String[] otherColumns = new String[table.columns.length - table.primaryKey.length];
        int pos = 0;
        for (int i = 0; i < table.columns.length; i++) {
            Column column = table.columns[i];
            if (!table.isPrimaryKeyColumn(column.name)) {
                otherColumns[pos++] = column.name;
            }
        }
        Schema<GenericRecord> valueSchema = buildSchema(table.name+"Value", otherColumns, table);
        return (KeyValueSchema<GenericRecord, GenericRecord>)
                Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);
    }

    private Schema<GenericRecord> buildSchema(String name, String[] columns, Table table) {
        RecordSchemaBuilder builder = SchemaBuilder.record(name);
        for (String column : columns) {
            Column col = table.getColumn(column);
            FieldSchemaBuilder field = builder.field(col.name)
                    .type(convertType(col.type));
            if (ColumnTypes.isNotNullDataType(col.type) || table.isPrimaryKeyColumn(col.name)) {
                field.required();
            }
        }
        SchemaInfo build = builder.build(SchemaType.JSON);
        return GenericSchema.of(build);
    }

    private static SchemaType convertType(int type) {
        switch (type) {
            case ColumnTypes.INTEGER:
            case ColumnTypes.NOTNULL_INTEGER:
                return SchemaType.INT32;
            case ColumnTypes.STRING:
            case ColumnTypes.NOTNULL_STRING:
                return SchemaType.STRING;
            default:
                throw new IllegalArgumentException("Type " +type + " )" + ColumnTypes.typeToString(type)
                        + ") is not supported yet");
        }
    }

    private static class InMemoryTableHistoryStorage implements ChangeDataCapture.TableSchemaHistoryStorage {

        private Map<String, SortedMap<LogSequenceNumber, Table>> definitions = new ConcurrentHashMap<>();

        @Override
        public void storeSchema(LogSequenceNumber lsn, Table table) {
            LOG.log(Level.INFO, "storeSchema {0} {1}", new Object[] {lsn, table.name});
            SortedMap<LogSequenceNumber, Table> tableHistory = definitions.computeIfAbsent(table.name, (n)-> Collections.synchronizedSortedMap(new TreeMap<>()));
            tableHistory.put(lsn, table);
        }

        @Override
        public Table fetchSchema(LogSequenceNumber lsn, String tableName) {
            LOG.log(Level.INFO, "fetchSchema {0} {1}", new Object[] {lsn, tableName});
            SortedMap<LogSequenceNumber, Table> tableHistory = definitions.computeIfAbsent(tableName, (n)-> Collections.synchronizedSortedMap(new TreeMap<>()));
            SortedMap<LogSequenceNumber, Table> after = tableHistory.headMap(lsn);
            if (after.isEmpty()) {
                return after.get(tableHistory.lastKey());
            }
            return after.values().iterator().next();
        }
    }
}