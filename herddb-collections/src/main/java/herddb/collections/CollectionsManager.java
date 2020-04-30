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
import herddb.utils.ContextClassLoaderAwareObjectInputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * Entry Point for HerdDB Collections. A Collections Manager manages a set of Collections, these Collections will share
 * the same pool of memory.
 * <p>
 * It is expected to have only one CollectionsManager per JVM.
 * <p>
 * All of the collections created from a CollectionsManager share common data structures.
 * <p>
 * In case of low memory the system will swap out to disk (or simply onload from memory) unused data. The system uses
 * the default data placement policy of the underlying HerdDB database.
 * <p>
 * Every collection is supposed not to be thread safe, but do not try to wrap them with custom synchronization
 * mechanisms because this may lead to deadlocks.
 */
public final class CollectionsManager implements AutoCloseable {

    private static final AtomicLong TABLE_NAME_GENERATOR = new AtomicLong();

    private static ValueSerializer defaultValueSerializer(int expectedSize) {
        return new ValueSerializer() {
            @Override
            public byte[] serialize(Object object) throws Exception {
                try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(expectedSize);
                        ObjectOutputStream ooo = new ObjectOutputStream(oo)) {
                    ooo.writeUnshared(object);
                    return oo.toByteArrayNoCopy();
                }
            }

            @Override
            public Object deserialize(Bytes bytes) throws Exception {
                SimpleByteArrayInputStream oo = new SimpleByteArrayInputStream(bytes.getBuffer(), bytes.getOffset(),
                        bytes.getLength());
                try (ObjectInputStream ooo = new ContextClassLoaderAwareObjectInputStream(oo)) {
                    return ooo.readUnshared();
                }
            }

        };
    }

    public static Builder builder() {
        return new Builder();
    }

    private static <K> Function<K, byte[]> defaultKeySerializer(ValueSerializer<K> serializer) {
        return (K key) -> {
            try {
                return serializer.serialize(key);
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

    private CollectionsManager(long maxMemory, Path tmpDirectory, Properties additionalConfiguration, boolean threadsafe) {
        ServerConfiguration configuration = new ServerConfiguration();

        configuration.set(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE, maxMemory);
        if (maxMemory > 0) {
            // HerdDB ergonomics keep into consideration a lot of stuff we are not using
            // here. So we can force the distribution of the max memory limit.
            // Maybe in the future we could make this more tunable.
            configuration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, maxMemory / 2);
            configuration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, maxMemory / 2);
        }

        // do not use additional threads
        // this is very important
        // because by default HerdDB deals with Netty and BookKeeper
        // here we are not using network or async commit logs....
        // so it is better to perform every operation on the same thread
        configuration.set(ServerConfiguration.PROPERTY_ASYNC_WORKER_THREADS, -1);

        // no SQL planner
        configuration.set(ServerConfiguration.PROPERTY_PLANNER_TYPE, ServerConfiguration.PLANNER_TYPE_NONE);

        // disable JMX, not useful in this case
        configuration.set(ServerConfiguration.PROPERTY_JMX_ENABLE, false);

        if (additionalConfiguration != null) {
            additionalConfiguration.forEach((k, v) -> {
                configuration.set(k.toString(), v);
            });
        }


        configuration.set(ServerConfiguration.PROPERTY_TABLEMANAGER_DISABLE_ROWLEVELLOCKS,  !threadsafe);

        // In case of HerdDB Collections the usage of XXHash64 might to be overkilling.
        boolean hashChecksEnabled = configuration.getBoolean(ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED, false);
        boolean hashWritesEnabled = configuration.getBoolean(ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED, false);

        server = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new FileDataStorageManager(tmpDirectory,
                        tmpDirectory, 0, false /* fsync */,
                        false /* o_direct */, false /* o_direct */,
                        hashChecksEnabled, hashWritesEnabled, NullStatsLogger.INSTANCE),
                new MemoryCommitLogManager(false /*serialize*/), tmpDirectory,
                new ServerHostData("localhost", 0, "", false, Collections.emptyMap()),
                configuration, NullStatsLogger.INSTANCE);

        server.setCheckpointPeriod(configuration.getLong(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD,
                ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD_DEFAULT));

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

    /**
     * Access to the internals, mostly for tests and debugging or unsupported tweaks.
     *
     * @return the internal HerdDB runtime.
     */
    public DBManager getServer() {
        return server;
    }

    private Table createTable(String tmpTableName, int pkType) throws StatementExecutionException {
        Table table = Table
                .builder()
                .name(tmpTableName)
                .column("pk", pkType)
                .column("v", ColumnTypes.INTEGER)
                // no need to define other columns
                .primaryKey("pk")
                .tablespace(TableSpace.DEFAULT)
                .build();
        CreateTableStatement createTable = new CreateTableStatement(table);
        tableSpaceManager.executeStatement(createTable, new StatementEvaluationContext(), TransactionContext.NO_TRANSACTION);
        return table;
    }

    public static final class Builder {

        private long maxMemory = ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT;
        private Path tmpDirectory;
        private Properties configuration;
        private boolean threadsafe = true;

        /**
         * Additional configuration for the internal HerdDB server.
         *
         * @param configuration a raw set of properties
         * @return the build itself
         */
        public Builder configuration(Properties configuration) {
            this.configuration = configuration;
            return this;
        }

        /**
         * Max memory to use. This is an upper bound to the amount of memory directly referenced by the
         * CollectionsManager. The system will automatically swap to disk data in order to respect this limit.
         *
         * @param maxMemory the amount of memory, in bytes. If not set it will use HerdDB defaults
         * @return the build itself
         */
        public Builder maxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
            return this;
        }

        /**
         * The directory to store the database. It is expected that the directory is empty.
         *
         * @param tmpDirectory
         * @return the build itself
         */
        public Builder tmpDirectory(Path tmpDirectory) {
            this.tmpDirectory = tmpDirectory;
            return this;
        }

        /**
         * To enable/disable concurrent-access management.
         * This is usefull whether data access is performed by a single thread (that created the table),
         * and row-level locks might be a waste of CPU/memory.
         *
         * @param threadsafe enables/disables concurrently access to db. Default true.
         * @return the build itself
         */
        public Builder threadsafe(boolean threadsafe) {
            this.threadsafe = threadsafe;
            return this;
        }

        /**
         * Creates the CollectionsManager. You must call {@link CollectionsManager#start() }
         * in order to boot the system.
         *
         * @return the new not-yet-started CollectionsManager.
         */
        public CollectionsManager build() {
            return new CollectionsManager(maxMemory, tmpDirectory, configuration, threadsafe);
        }

    }

    public class TmpMapBuilder<V1> {

        private ValueSerializer<V1> valueSerializer;
        private int expectedValueSize = 64;

        public class IntTmpMapBuilder<V2 extends V1> {

            /**
             * Boot the map.
             *
             * @return the handle to the map.
             */
            public TmpMap<Integer, V2> build() {
                String tmpTableName = generateTmpTableName();
                Table table = createTable(tmpTableName, ColumnTypes.NOTNULL_INTEGER);
                return new TmpMapImpl<>(table, expectedValueSize,
                        Bytes::intToByteArray, valueSerializer != null ? valueSerializer : defaultValueSerializer(expectedValueSize), tableSpaceManager);
            }
        }

        public class LongTmpMapBuilder<V2 extends V1> {

            /**
             * Boot the map.
             *
             * @return the handle to the map.
             */
            public TmpMap<Long, V2> build() {
                String tmpTableName = generateTmpTableName();
                Table table = createTable(tmpTableName, ColumnTypes.NOTNULL_LONG);
                return new TmpMapImpl<>(table, expectedValueSize,
                        Bytes::longToByteArray, valueSerializer != null ? valueSerializer : defaultValueSerializer(expectedValueSize), tableSpaceManager);
            }
        }

        public class StringTmpMapBuilder<V2 extends V1> {

            /**
             * Boot the map.
             *
             * @return the handle to the map.
             */
            public TmpMap<String, V2> build() {
                String tmpTableName = generateTmpTableName();
                Table table = createTable(tmpTableName, ColumnTypes.NOTNULL_STRING);
                return new TmpMapImpl<>(table, expectedValueSize,
                        Bytes::string_to_array, valueSerializer != null ? valueSerializer : defaultValueSerializer(expectedValueSize), tableSpaceManager);
            }
        }

        public class ObjectTmpMapBuilder<K, V2 extends V1> {

            @SuppressWarnings("unchecked")
            private Function<K, byte[]> keySerializer;

            /**
             * Define a custom serializer for keys.
             *
             * @return the builder itself.
             */
            public ObjectTmpMapBuilder<K, V2> withKeySerializer(Function<K, byte[]> keySerializer) {
                this.keySerializer = keySerializer;
                return this;
            }

            /**
             * Boot the map.
             *
             * @return the handle to the map.
             */
            public TmpMap<K, V2> build() {
                String tmpTableName = generateTmpTableName();
                Table table = createTable(tmpTableName, ColumnTypes.BYTEARRAY);
                return new TmpMapImpl<>(table, expectedValueSize,
                        keySerializer != null ? keySerializer :  defaultKeySerializer(defaultValueSerializer(4)),
                        valueSerializer != null ? valueSerializer : defaultValueSerializer(expectedValueSize), tableSpaceManager);
            }
        }

        /**
         * Define a custom serializer for values.
         *
         * @param valueSerializer
         * @return the builder itself
         */
        public TmpMapBuilder<V1> withValueSerializer(ValueSerializer<V1> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        /**
         * Define the initial buffer size for each value serialization. Setting an appropriate will help reducing
         * temporary memory copies due to reallocation of internal buffers.
         *
         * @param expectedValueSize the minimum expected size for a value.
         * @return the builder itself
         */
        public TmpMapBuilder<V1> withExpectedValueSize(int expectedValueSize) {
            this.expectedValueSize = expectedValueSize;
            return this;
        }

        /**
         * Start creating a map optimized for "int" keys.
         *
         * @return the builder itself
         */
        public IntTmpMapBuilder withIntKeys() {
            return new IntTmpMapBuilder();
        }

        /**
         * Start creating a map optimized for "long" keys.
         *
         * @return the builder itself
         */
        public LongTmpMapBuilder withLongKeys() {
            return new LongTmpMapBuilder();
        }

        /**
         * Start creating a map optimized for "string" keys.
         *
         * @return the builder itself
         */
        public StringTmpMapBuilder withStringKeys() {
            return new StringTmpMapBuilder();
        }

        /**
         * Start creating a map with a serializer for keys.
         *
         * @return the builder itself
         */
        public <K> ObjectTmpMapBuilder<K, V1> withObjectKeys(Class<K> clazz) {
            return new ObjectTmpMapBuilder<>();
        }
    }

}
