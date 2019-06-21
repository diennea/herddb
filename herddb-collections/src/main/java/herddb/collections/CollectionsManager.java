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
package herddb.collections;

import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.file.FileDataStorageManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.network.ServerHostData;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * Entry Point for HerdDB Collections. A Collections Manager manages a set of Collections, these Collections will share
 * the same pool of memory. It is expected to have only one CollectionsManager per JVM.
 */
public final class CollectionsManager implements AutoCloseable {

    private static final AtomicLong TABLE_NAME_GENERATOR = new AtomicLong();

    private final DBManager server;
    private static final ValueSerializer DEFAULT_VALUE_SERIALIZER = new ValueSerializer() {
        @Override
        public void serialize(Object object, OutputStream oo) throws Exception {
            try (ObjectOutputStream ooo = new ObjectOutputStream(oo)) {
                ooo.writeUnshared(object);
            }
        }

        @Override
        public Object deserialize(Bytes bytes) throws Exception {
            SimpleByteArrayInputStream oo = new SimpleByteArrayInputStream(bytes.getBuffer(), bytes.getOffset(),
                    bytes.getLength());
            try (ObjectInputStream ooo = new ObjectInputStream(oo)) {
                return ooo.readUnshared();
            }
        }

    };
    ;
    private TableSpaceManager tableSpaceManager;

    public static interface ValueSerializer<K> {

        public void serialize(K object, OutputStream outputStream) throws Exception;

        public K deserialize(Bytes bytes) throws Exception;
    }

    public static Builder builder() {
        return new Builder();
    }

    private CollectionsManager(long maxMemory, Path tmpDirectory) {
        ServerConfiguration configuration = new ServerConfiguration();

        configuration.set(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE, maxMemory);

        server = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new FileDataStorageManager(tmpDirectory,
                        tmpDirectory, 0, false /* fsync */,
                        false /* o_direct */, false /* o_direct */, NullStatsLogger.INSTANCE),
                new MemoryCommitLogManager(), tmpDirectory,
                new ServerHostData("localhost", 0, "", false, Collections.emptyMap()),
                configuration, NullStatsLogger.INSTANCE);
        server.setMaxDataUsedMemory(maxMemory);
    }

    public void start() throws Exception {
        server.start();
        server.waitForTablespace(TableSpace.DEFAULT, 60000);
        tableSpaceManager = server.getTableSpaceManager(TableSpace.DEFAULT);
    }

    @Override
    public void close() {
        server.close();
    }

    public static final class Builder {

        private long maxMemory = ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT;
        private Path tmpDirectory;

        public Builder maxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
            return this;
        }

        public Builder tmpDirectory(Path tmpDirectory) {
            this.tmpDirectory = tmpDirectory;
            return this;
        }

        public CollectionsManager build() {
            return new CollectionsManager(maxMemory, tmpDirectory);
        }

    }

    private static <K> Function<K, Bytes> DEFAULT_KEY_SERIALIZER(ValueSerializer<K> serializer) {
        return (K key) -> {
            try {
                VisibleByteArrayOutputStream serializedKey = new VisibleByteArrayOutputStream(32);
                serializer.serialize(key, serializedKey);
                return Bytes.from_array(serializedKey.getBuffer(), 0, serializedKey.size());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    public class TmpMapBuilder<V> {

        private ValueSerializer<V> valueSerializer = DEFAULT_VALUE_SERIALIZER;
        private int expectedValueSize = 64;
        private boolean threadsafe = false;

        public class IntTmpMapBuilder<VI extends V> {

            public IntTmpMapBuilder withValueSerializer(ValueSerializer<V> valueSerializer) {
                TmpMapBuilder.this.valueSerializer = valueSerializer;
                return this;
            }

            public TmpMap<Integer, VI> build() {
                String tmpTableName = generateTmpTableName();
                createTable(tmpTableName, ColumnTypes.NOTNULL_INTEGER);
                return new TmpMapImpl<>(tmpTableName, expectedValueSize, threadsafe,
                        Bytes::from_int, valueSerializer);
            }
        }

        public class StringTmpMapBuilder<VI extends V> {

            public StringTmpMapBuilder withValueSerializer(ValueSerializer<V> valueSerializer) {
                TmpMapBuilder.this.valueSerializer = valueSerializer;
                return this;
            }

            public TmpMap<String, VI> build() {
                String tmpTableName = generateTmpTableName();
                createTable(tmpTableName, ColumnTypes.NOTNULL_STRING);
                return new TmpMapImpl<>(tmpTableName, expectedValueSize, threadsafe,
                        Bytes::from_string, valueSerializer);
            }
        }

        public class ObjectTmpMapBuilder<K, VI extends V> {

            private Function<K, Bytes> keySerializer = DEFAULT_KEY_SERIALIZER(DEFAULT_VALUE_SERIALIZER);

            public ObjectTmpMapBuilder<K, VI> withKeySerializer(Function<K, Bytes> keySerializer) {
                this.keySerializer = keySerializer;
                return this;
            }

            public ObjectTmpMapBuilder<K, VI> withValueSerializer(ValueSerializer<V> valueSerializer) {
                TmpMapBuilder.this.valueSerializer = valueSerializer;
                return this;
            }

            public TmpMap<K, VI> build() {
                String tmpTableName = generateTmpTableName();
                createTable(tmpTableName, ColumnTypes.BYTEARRAY);
                return new TmpMapImpl<>(tmpTableName, expectedValueSize, threadsafe,
                        keySerializer, valueSerializer);
            }
        }

        public TmpMapBuilder<V> threadsafe(boolean threadsafe) {
            this.threadsafe = threadsafe;
            return this;
        }

        public TmpMapBuilder<V> withValueSerializer(ValueSerializer<V> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public TmpMapBuilder<V> withExpectedValueSize(int expectedValueSize) {
            this.expectedValueSize = expectedValueSize;
            return this;
        }

        public IntTmpMapBuilder withIntKeys() {
            return new IntTmpMapBuilder();
        }

        public StringTmpMapBuilder withStringKeys() {
            return new StringTmpMapBuilder();
        }

        public <K> ObjectTmpMapBuilder<K, V> withObjectKeys(Class<K> clazz) {
            return new ObjectTmpMapBuilder<>();
        }
    }

    public <V> TmpMapBuilder<V> newMap() {
        return new TmpMapBuilder<V>();
    }

    private void createTable(String tmpTableName, int pkType) throws StatementExecutionException {
        Table table = Table
                .builder()
                .name(tmpTableName)
                .column("pk", pkType)
                .primaryKey("pk")
                .tablespace(TableSpace.DEFAULT)
                .build();
        CreateTableStatement createTable = new CreateTableStatement(table);
        tableSpaceManager.executeStatement(createTable, new StatementEvaluationContext(),
                TransactionContext.NO_TRANSACTION);
    }

    private Object executeGet(Bytes serializedKey, String tmpTableName) throws StatementExecutionException, Exception {
        GetStatement get = new GetStatement(TableSpace.DEFAULT, tmpTableName, serializedKey, null,
                false);
        GetResult getResult = (GetResult) tableSpaceManager.executeStatement(get,
                new StatementEvaluationContext(),
                herddb.model.TransactionContext.NO_TRANSACTION);
        if (!getResult.found()) {
            return null;
        } else {
            return DEFAULT_VALUE_SERIALIZER
                    .deserialize(getResult.getRecord().value);
        }
    }

    private boolean executeContainsKey(Bytes serializedKey, String tmpTableName) throws StatementExecutionException,
            Exception {
        GetStatement get = new GetStatement(TableSpace.DEFAULT, tmpTableName, serializedKey, null,
                false);
        GetResult getResult = (GetResult) tableSpaceManager.executeStatement(get,
                new StatementEvaluationContext(),
                herddb.model.TransactionContext.NO_TRANSACTION);
        return getResult.found();
    }

    private static final BeginTransactionStatement BEGIN_TRANSACTION_STATEMENT =
            new BeginTransactionStatement(TableSpace.DEFAULT);

    private static String generateTmpTableName() {
        return "tmp" + TABLE_NAME_GENERATOR.incrementAndGet();
    }

    private class TmpMapImpl<K, V> implements TmpMap<K, V> {

        private final String tmpTableName;
        private final Function<K, Bytes> keySerializer;
        private final ValueSerializer valuesSerializer;
        private final int expectedValueSize;
        private final boolean threadsafe;

        public TmpMapImpl(String tmpTableName,
                int expectedValueSize,
                boolean concurrent,
                Function<K, Bytes> keySerializer,
                ValueSerializer valuesSerializer) {
            this.threadsafe = concurrent;
            this.tmpTableName = tmpTableName;
            this.keySerializer = keySerializer;
            this.valuesSerializer = valuesSerializer;
            this.expectedValueSize = expectedValueSize;
        }

        @Override
        public void close() {
            DropTableStatement drop = new DropTableStatement(TableSpace.DEFAULT, tmpTableName, true);
            tableSpaceManager.executeStatement(drop, new StatementEvaluationContext(),
                    herddb.model.TransactionContext.NO_TRANSACTION);
        }

        @Override
        public void put(K key, V value) throws Exception {
            VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream(expectedValueSize);
            valuesSerializer.serialize(value, buffer);
            Bytes valueSerialized = Bytes.from_array(buffer.getBuffer(), 0, buffer.size());
            StatementEvaluationContext context = new StatementEvaluationContext();
            Record record = new Record(keySerializer.apply(key), valueSerialized);

            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, tmpTableName, record);

            if (threadsafe) {

                long tx = ((TransactionResult) tableSpaceManager.executeStatement(BEGIN_TRANSACTION_STATEMENT, context,
                        TransactionContext.NO_TRANSACTION)).transactionId;
                TransactionContext transactionContext = new TransactionContext(tx);
                try {
                    tableSpaceManager.executeStatement(insert,
                            context, transactionContext);
                } catch (DuplicatePrimaryKeyException alreadyExists) {
                    tableSpaceManager.executeStatement(
                            new UpdateStatement(TableSpace.DEFAULT, tmpTableName, record, null),
                            context,
                            transactionContext
                    );
                } finally {
                    tableSpaceManager.executeStatement(
                            new CommitTransactionStatement(TableSpace.DEFAULT, tx), context,
                            TransactionContext.NO_TRANSACTION);
                }
            } else {
                // no concurrent access, no need to create a transaction
                try {
                    tableSpaceManager.executeStatement(insert,
                            context, TransactionContext.NO_TRANSACTION);
                } catch (DuplicatePrimaryKeyException alreadyExists) {
                    tableSpaceManager.executeStatement(
                            new UpdateStatement(TableSpace.DEFAULT, tmpTableName, record, null),
                            context, TransactionContext.NO_TRANSACTION
                    );
                }
            }
        }

        @Override
        public V get(K key) throws Exception {
            Bytes serializedKey = keySerializer.apply(key);
            return (V) executeGet(serializedKey, tmpTableName);
        }

        @Override
        public boolean containsKey(K key) throws Exception {
            Bytes serializedKey = keySerializer.apply(key);
            return executeContainsKey(serializedKey, tmpTableName);
        }
    }

}
