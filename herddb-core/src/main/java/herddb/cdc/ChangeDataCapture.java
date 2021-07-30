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
package herddb.cdc;


import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT;
import herddb.client.ClientConfiguration;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.DataAccessorForFullRecord;
import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Record;
import herddb.model.Table;
import herddb.server.ServerConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * This utility provides a way to Change Data Capture with HerdDB.
 */
public class ChangeDataCapture implements AutoCloseable {

    /**
     * Type of Mutation
     */
    public enum MutationType {
        INSERT,
        UPDATE,
        DELETE,
        CREATE_TABLE,
        DROP_TABLE,
        ALTER_TABLE
    }

    /**
     * Details about a Mutation
     */
    public static class Mutation {
        private final Table table;
        private final MutationType mutationType;
        private final DataAccessorForFullRecord record;
        private final LogSequenceNumber logSequenceNumber;
        private final long timestamp;

        public Mutation(Table table, MutationType mutationType,
                        DataAccessorForFullRecord record, LogSequenceNumber logSequenceNumber,
                        long timestamp) {
            this.table = table;
            this.mutationType = mutationType;
            this.record = record;
            this.logSequenceNumber = logSequenceNumber;
            this.timestamp = timestamp;
        }

        public Table getTable() {
            return table;
        }

        public MutationType getMutationType() {
            return mutationType;
        }

        public DataAccessorForFullRecord getRecord() {
            return record;
        }

        public LogSequenceNumber getLogSequenceNumber() {
            return logSequenceNumber;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Mutation{"
                    + "table=" + table
                    + ", mutationType=" + mutationType
                    + ", record=" + record
                    + ", logSequenceNumber=" + logSequenceNumber
                    + ", timestamp=" + timestamp
                    + '}';
        }
    }

    /**
     * Implement this interface in order to receive the flow of Mutations
     */
    public interface MutationListener {
        void accept(Mutation mutation);
    }

    public interface TableSchemaHistoryStorage {
        /**
         * Stores a schema change for a table
         * @param lsn the lsn at which the change happened
         * @param table the schema
         */
        void storeSchema(LogSequenceNumber lsn, Table table);

        /**
         * Return the schema at the given log sequence number
         * @param lsn
         * @return the schema
         */
        Table fetchSchema(LogSequenceNumber lsn, String tableName);
    }

    private final ClientConfiguration configuration;
    private final MutationListener listener;
    private final TableSchemaHistoryStorage tableSchemaHistoryStorage;
    private LogSequenceNumber lastPosition;
    private final String tableSpaceUUID;
    private volatile boolean closed = false;
    private volatile boolean running = false;

    private ZookeeperMetadataStorageManager zookeeperMetadataStorageManager;
    private BookkeeperCommitLogManager manager;
    private Map<Long, TransactionHolder> transactions = new HashMap<>();

    private static class TransactionHolder {
        private List<Mutation> mutations = new ArrayList<>();
        private Map<String, Table> tablesDefinitions = new HashMap<>();
    }

    public ChangeDataCapture(String tableSpaceUUID, ClientConfiguration configuration, MutationListener listener, LogSequenceNumber startingPosition, TableSchemaHistoryStorage tableSchemaHistoryStorage) {
        this.configuration = configuration;
        this.listener = listener;
        this.lastPosition = startingPosition;
        this.tableSpaceUUID = tableSpaceUUID;
        this.tableSchemaHistoryStorage = tableSchemaHistoryStorage;
    }

    /**
     * Bootstrap the procedure.
     * @throws Exception
     */
    public void start() throws Exception {
        zookeeperMetadataStorageManager = buildMetadataStorageManager(configuration);
        manager = new BookkeeperCommitLogManager(zookeeperMetadataStorageManager, new ServerConfiguration(), NullStatsLogger.INSTANCE);
        manager.start();
    }

    /**
     * Execute one run
     * @return the last sequence number, to be used to configure CDC for the next execution
     * @throws Exception
     */
    public LogSequenceNumber run() throws Exception {
        if (zookeeperMetadataStorageManager == null) {
            throw new IllegalStateException("not started");
        }

        try (BookkeeperCommitLog cdc = manager.createCommitLog(tableSpaceUUID, tableSpaceUUID, "cdc");) {
            running = true;
            CommitLog.FollowerContext context = cdc.startFollowing(lastPosition);
            cdc.followTheLeader(lastPosition, new CommitLog.EntryAcceptor() {
                @Override
                public boolean accept(LogSequenceNumber lsn, LogEntry entry) throws Exception {
                    applyEntry(entry, lsn);
                    lastPosition = lsn;
                    return !closed;
                }
            }, context);
            return lastPosition;
        } finally {
            running = false;
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        long _start = System.currentTimeMillis();

        while (running
                && (System.currentTimeMillis() - _start < 10_000)) {
            Thread.sleep(100);
        }
        if (manager != null) {
            manager.close();
        }
        if (zookeeperMetadataStorageManager != null) {
            zookeeperMetadataStorageManager.close();
        }
    }

    private void fire(Mutation mutation, long transactionId) {
        if (transactionId > 0) {
            TransactionHolder transaction = transactions.get(transactionId);
            transaction.mutations.add(mutation);
        } else {
            listener.accept(mutation);
        }
    }

    private Table lookupTable(LogSequenceNumber lsn, LogEntry entry) {
        String tableName = entry.tableName;
        if (entry.transactionId > 0) {
            TransactionHolder transaction = transactions.get(entry.transactionId);
            Table table = transaction.tablesDefinitions.get(tableName);
            if (table != null) {
                return table;
            }
        }
        return tableSchemaHistoryStorage.fetchSchema(lsn, tableName);
    }

    private void applyEntry(LogEntry entry, LogSequenceNumber lsn) throws Exception {
        switch (entry.type) {
            case LogEntryType.NOOP:
            case LogEntryType.CREATE_INDEX:
            case LogEntryType.DROP_INDEX:
                break;
            case LogEntryType.DROP_TABLE: {
                Table table = lookupTable(lsn, entry);

                if (entry.transactionId > 0) {
                    TransactionHolder transaction = transactions.get(entry.transactionId);
                    // set null to mark the table as DROPPED
                    transaction.tablesDefinitions.put(entry.tableName, null);
                }
                fire(new Mutation(table, MutationType.DROP_TABLE, null, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                if (entry.transactionId > 0) {
                    TransactionHolder transaction = transactions.get(entry.transactionId);
                    transaction.tablesDefinitions.put(entry.tableName, table);
                } else {
                    tableSchemaHistoryStorage.storeSchema(lsn, table);
                }
                fire(new Mutation(table, MutationType.CREATE_TABLE, null, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.ALTER_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                if (entry.transactionId > 0) {
                    TransactionHolder transaction = transactions.get(entry.transactionId);
                    transaction.tablesDefinitions.put(entry.tableName, table);
                } else {
                    tableSchemaHistoryStorage.storeSchema(lsn, table);
                }
                fire(new Mutation(table, MutationType.ALTER_TABLE, null, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.INSERT: {
                Table table = lookupTable(lsn, entry);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                fire(new Mutation(table, MutationType.INSERT, record, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.DELETE: {
                Table table = lookupTable(lsn, entry);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                fire(new Mutation(table, MutationType.DELETE, record, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.UPDATE: {
                Table table = lookupTable(lsn, entry);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                fire(new Mutation(table, MutationType.UPDATE, record, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.BEGINTRANSACTION: {
                transactions.put(entry.transactionId, new TransactionHolder());
            }
            break;
            case LogEntryType.COMMITTRANSACTION: {
                TransactionHolder transaction = transactions.remove(entry.transactionId);
                transaction.tablesDefinitions.forEach((tableName, tableDef) -> {
                    if (tableDef == null) { // DROP TABLE

                    } else { // CREATE/ALTER
                        tableSchemaHistoryStorage.storeSchema(lsn, tableDef);
                    }
                });
                for (Mutation mutation : transaction.mutations) {
                    listener.accept(mutation);
                }
            }
            break;
            case LogEntryType.ROLLBACKTRANSACTION:
                transactions.remove(entry.transactionId);
                break;
            default:
                // discard unknown entry types
                break;
        }
    }

    private static ZookeeperMetadataStorageManager buildMetadataStorageManager(ClientConfiguration configuration)
            throws MetadataStorageManagerException {
        String zkAddress = configuration.getString(PROPERTY_ZOOKEEPER_ADDRESS, PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
        String zkPath = configuration.getString(PROPERTY_ZOOKEEPER_PATH, PROPERTY_ZOOKEEPER_PATH_DEFAULT);
        int sessionTimeout = configuration.getInt(PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, 60000);
        ZookeeperMetadataStorageManager zk = new ZookeeperMetadataStorageManager(zkAddress, sessionTimeout, zkPath);
        zk.start(false);
        return zk;
    }
}
