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
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
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

    public static Builder builder() {
        return new Builder();
    }

    private static <K> Function<K, byte[]> DEFAULT_KEY_SERIALIZER(ValueSerializer<K> serializer) {
        return (K key) -> {
            try {
                VisibleByteArrayOutputStream serializedKey = new VisibleByteArrayOutputStream(32);
                serializer.serialize(key, serializedKey);
                return serializedKey.toByteArray();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    private static String generateTmpTableName() {
        return "tmp" + TABLE_NAME_GENERATOR.incrementAndGet();
    }
    private final DBManager server;
    private TableSpaceManager tableSpaceManager;

    private CollectionsManager(long maxMemory, Path tmpDirectory) {
        ServerConfiguration configuration = new ServerConfiguration();

        configuration.set(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE, maxMemory);
        configuration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, maxMemory / 2);
        configuration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, maxMemory / 2);

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

    public <V> TmpMapBuilder<V> newMap() {
        return new TmpMapBuilder<>();
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
                        Bytes::intToByteArray, valueSerializer, tableSpaceManager);
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
                        Bytes::string_to_array, valueSerializer, tableSpaceManager);
            }
        }

        public class ObjectTmpMapBuilder<K, VI extends V> {

            private Function<K, byte[]> keySerializer = DEFAULT_KEY_SERIALIZER(DEFAULT_VALUE_SERIALIZER);

            public ObjectTmpMapBuilder<K, VI> withKeySerializer(Function<K, byte[]> keySerializer) {
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
                        keySerializer, valueSerializer, tableSpaceManager);
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

}
