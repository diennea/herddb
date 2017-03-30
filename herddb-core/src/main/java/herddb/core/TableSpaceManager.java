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
package herddb.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import herddb.backup.DumpedLogEntry;
import herddb.client.ClientConfiguration;
import herddb.client.ClientSideMetadataProvider;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.core.stats.TableManagerStats;
import herddb.core.stats.TableSpaceManagerStats;
import herddb.core.system.SysclientsTableManager;
import herddb.core.system.SyscolumnsTableManager;
import herddb.core.system.SysconfigTableManager;
import herddb.core.system.SysindexesTableManager;
import herddb.core.system.SysnodesTableManager;
import herddb.core.system.SystablesTableManager;
import herddb.core.system.SystablespacereplicastateTableManager;
import herddb.core.system.SystablespacesTableManager;
import herddb.core.system.SystablestatsTableManager;
import herddb.core.system.SystransactionsTableManager;
import herddb.index.MemoryHashIndexManager;
import herddb.index.brin.BRINIndexManager;
import herddb.jmx.JMXUtils;
import herddb.log.CommitLog;
import herddb.log.CommitLogListener;
import herddb.log.CommitLogResult;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.IndexAlreadyExistsException;
import herddb.model.IndexDoesNotExistException;
import herddb.model.NodeMetadata;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAlreadyExistsException;
import herddb.model.TableAwareStatement;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropIndexStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.ScanStatement;
import herddb.network.Channel;
import herddb.network.KeyValue;
import herddb.network.Message;
import herddb.network.SendResultCallback;
import herddb.network.ServerHostData;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.utils.Bytes;

/**
 * Manages a TableSet in memory
 *
 * @author enrico.olivelli
 */
public class TableSpaceManager {

    private static final Logger LOGGER = Logger.getLogger(TableSpaceManager.class.getName());

    private final MetadataStorageManager metadataStorageManager;
    private final DataStorageManager dataStorageManager;
    private final CommitLog log;
    private final String tableSpaceName;
    private final String tableSpaceUUID;
    private final String nodeId;
    private final ConcurrentSkipListSet<String> tablesNeedingCheckPoint = new ConcurrentSkipListSet<>();
    private final ConcurrentHashMap<String, AbstractTableManager> tables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AbstractIndexManager> indexes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, AbstractIndexManager>> indexesByTable = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();
    private final AtomicLong newTransactionId = new AtomicLong();
    private final DBManager dbmanager;
    private final boolean virtual;

    private volatile boolean leader;
    private volatile boolean closed;
    private volatile boolean failed;
    private LogSequenceNumber actualLogSequenceNumber;

    public String getTableSpaceName() {
        return tableSpaceName;
    }

    public String getTableSpaceUUID() {
        return tableSpaceUUID;
    }

    public TableSpaceManager(String nodeId, String tableSpaceName, String tableSpaceUUID, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager, CommitLog log, DBManager manager, boolean virtual) {
        this.nodeId = nodeId;
        this.dbmanager = manager;
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.log = log;
        this.tableSpaceName = tableSpaceName;
        this.tableSpaceUUID = tableSpaceUUID;
        this.virtual = virtual;
    }

    private void bootSystemTables() {
        if (virtual) {
            registerSystemTableManager(new SysconfigTableManager(this));
            registerSystemTableManager(new SysclientsTableManager(this));
        } else {
            registerSystemTableManager(new SystablesTableManager(this));
            registerSystemTableManager(new SystablestatsTableManager(this));
            registerSystemTableManager(new SysindexesTableManager(this));
            registerSystemTableManager(new SyscolumnsTableManager(this));
            registerSystemTableManager(new SystransactionsTableManager(this));
        }
        registerSystemTableManager(new SystablespacesTableManager(this));
        registerSystemTableManager(new SystablespacereplicastateTableManager(this));
        registerSystemTableManager(new SysnodesTableManager(this));
    }

    private void registerSystemTableManager(AbstractTableManager tableManager) {
        tables.put(tableManager.getTable().name, tableManager);
    }

    void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException, DDLException {

        TableSpace tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);

        bootSystemTables();
        if (virtual) {
            startAsLeader();
        } else {
            recover(tableSpaceInfo);

            LOGGER.log(Level.SEVERE, " after recovery of tableSpace " + tableSpaceName + ", actualLogSequenceNumber:" + actualLogSequenceNumber);

            tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
            if (tableSpaceInfo.leaderId.equals(nodeId)) {
                startAsLeader();
            } else {
                startAsFollower();
            }
        }
    }

    void recover(TableSpace tableSpaceInfo) throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {
        LogSequenceNumber logSequenceNumber = dataStorageManager.getLastcheckpointSequenceNumber(tableSpaceUUID);
        actualLogSequenceNumber = logSequenceNumber;
        LOGGER.log(Level.SEVERE, nodeId + " recover " + tableSpaceName + ", logSequenceNumber from DataStorage: " + logSequenceNumber);
        List<Table> tablesAtBoot = dataStorageManager.loadTables(logSequenceNumber, tableSpaceUUID);
        List<Index> indexesAtBoot = dataStorageManager.loadIndexes(logSequenceNumber, tableSpaceUUID);
        String tableNames = tablesAtBoot.stream().map(t -> {
            return t.name;
        }).collect(Collectors.joining(","));

        String indexNames = indexesAtBoot.stream().map(t -> {
            return t.name + " on table " + t.table;
        }).collect(Collectors.joining(","));

        LOGGER.log(Level.SEVERE, nodeId + " " + tableSpaceName + " tablesAtBoot " + tableNames + ", indexesAtBoot " + indexNames);

        for (Table table : tablesAtBoot) {
            TableManager tableManager = bootTable(table, 0, null);
            for (Index index : indexesAtBoot) {
                if (index.table.equals(table.name)) {
                    bootIndex(index, tableManager, 0, false);
                }
            }
        }
        dataStorageManager.loadTransactions(logSequenceNumber, tableSpaceUUID, t -> {
            transactions.put(t.transactionId, t);
            try {
                if (t.newTables != null) {
                    for (Table table : t.newTables.values()) {
                        if (!tables.containsKey(table.name)) {
                            bootTable(table, t.transactionId, null);
                        }
                    }
                }
                if (t.newIndexes != null) {
                    for (Index index : t.newIndexes.values()) {
                        if (!indexes.containsKey(index.name)) {
                            AbstractTableManager tableManager = tables.get(index.table);
                            bootIndex(index, tableManager, t.transactionId, false);
                        }
                    }
                }
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while booting tmp tables " + err, err);
                throw new RuntimeException(err);
            }
        });

        LOGGER.log(Level.SEVERE, nodeId + " recovering tablespace " + tableSpaceName + " log from sequence number " + logSequenceNumber);

        try {
            log.recovery(logSequenceNumber, new ApplyEntryOnRecovery(), false);
        } catch (FullRecoveryNeededException fullRecoveryNeeded) {
            LOGGER.log(Level.SEVERE, nodeId + " full recovery of data is needed for tableSpace " + tableSpaceName, fullRecoveryNeeded);
            downloadTableSpaceData();
            log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), false);
        }
        checkpoint();

    }

    void recoverForLeadership() throws DataStorageManagerException, LogNotAvailableException {
        actualLogSequenceNumber = log.getLastSequenceNumber();
        LOGGER.log(Level.SEVERE, "recovering tablespace " + tableSpaceName + " log from sequence number " + actualLogSequenceNumber + ", with fencing");
        log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), true);
    }

    void apply(CommitLogResult position, LogEntry entry, boolean recovery) throws DataStorageManagerException, DDLException {
        if (!position.deferred) {
            this.actualLogSequenceNumber = position.getLogSequenceNumber();
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "apply entry {0} {1}", new Object[]{position, entry});
        }
        switch (entry.type) {
            case LogEntryType.BEGINTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = new Transaction(id, tableSpaceName, position);
                transactions.put(id, transaction);
            }
            break;
            case LogEntryType.ROLLBACKTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = transactions.get(id);
                if (transaction == null) {
                    throw new DataStorageManagerException("invalid transaction id " + id + ", only " + transactions.keySet());
                }
                List<AbstractTableManager> managers;
                try {
                    generalLock.readLock().lock();
                    managers = new ArrayList<>(tables.values());
                } finally {
                    generalLock.readLock().unlock();
                }
                for (AbstractTableManager manager : managers) {

                    if (transaction.isNewTable(manager.getTable().name)) {
                        LOGGER.log(Level.SEVERE, "rollback CREATE TABLE " + manager.getTable().name);
                        manager.dropTableData();
                        manager.close();
                        tables.remove(manager.getTable().name);
                    } else {
                        manager.onTransactionRollback(transaction);
                    }
                }
                transactions.remove(transaction.transactionId);
            }
            break;
            case LogEntryType.COMMITTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = transactions.get(id);
                if (transaction == null) {
                    throw new DataStorageManagerException("invalid transaction id " + id);
                }
                transaction.synch();
                List<AbstractTableManager> managers = new ArrayList<>(tables.values());
                for (AbstractTableManager manager : managers) {
                    manager.onTransactionCommit(transaction, recovery);
                }
                List<AbstractIndexManager> indexManagers = new ArrayList<>(indexes.values());
                for (AbstractIndexManager indexManager : indexManagers) {
                    indexManager.onTransactionCommit(transaction, recovery);
                }
                if ((transaction.droppedTables != null && !transaction.droppedTables.isEmpty()) || (transaction.droppedIndexes != null && !transaction.droppedIndexes.isEmpty())) {
                    generalLock.writeLock().lock();
                    try {
                        if (transaction.droppedTables != null) {
                            for (String dropped : transaction.droppedTables) {
                                for (AbstractTableManager manager : managers) {
                                    if (manager.getTable().name.equals(dropped)) {
                                        manager.dropTableData();
                                        manager.close();
                                        tables.remove(manager.getTable().name);
                                    }
                                }
                            }
                        }
                        if (transaction.droppedIndexes != null) {
                            for (String dropped : transaction.droppedIndexes) {
                                for (AbstractIndexManager manager : indexManagers) {
                                    if (manager.getIndex().name.equals(dropped)) {
                                        manager.dropIndexData();
                                        manager.close();
                                        indexes.remove(manager.getIndex().name);
                                        Map<String, AbstractIndexManager> indexesForTable = indexesByTable.get(manager.getIndex().table);
                                        if (indexesForTable != null) {
                                            indexesForTable.remove(manager.getIndex().name);
                                        }
                                    }
                                }
                            }
                        }
                    } finally {
                        generalLock.writeLock().unlock();
                    }
                }
                if ((transaction.newTables != null && !transaction.newTables.isEmpty())
                    || (transaction.droppedTables != null && !transaction.droppedTables.isEmpty())
                    || (transaction.newIndexes != null && !transaction.newIndexes.isEmpty())
                    || (transaction.droppedIndexes != null && !transaction.droppedIndexes.isEmpty())) {
                    writeTablesOnDataStorageManager(position);
                    dbmanager.getPlanner().clearCache();
                }
                transactions.remove(transaction.transactionId);
            }
            break;
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value);
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewTable(table, position);
                }

                bootTable(table, entry.transactionId, null);
                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position);
                }
            }
            break;
            case LogEntryType.CREATE_INDEX: {
                Index index = Index.deserialize(entry.value);
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewIndex(index, position);
                }
                AbstractTableManager tableManager = tables.get(index.table);
                if (tableManager == null) {
                    throw new RuntimeException("table " + index.table + " does not exists");
                }
                bootIndex(index, tableManager, entry.transactionId, true);
                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position);
                }
            }
            break;
            case LogEntryType.DROP_TABLE: {
                String tableName = entry.tableName;
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerDropTable(tableName, position);
                } else {
                    try {
                        generalLock.writeLock().lock();
                        AbstractTableManager manager = tables.get(tableName);
                        if (manager != null) {
                            manager.dropTableData();
                            manager.close();
                            tables.remove(manager.getTable().name);
                        }
                    } finally {
                        generalLock.writeLock().unlock();
                    }
                }

                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position);
                }
            }
            break;
            case LogEntryType.DROP_INDEX: {
                String indexName = Bytes.from_array(entry.value).to_string();
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerDropIndex(indexName, position);
                } else {
                    try {
                        generalLock.writeLock().lock();
                        AbstractIndexManager manager = indexes.get(indexName);
                        if (manager != null) {
                            manager.dropIndexData();
                            manager.close();
                            indexes.remove(manager.getIndexName());
                            Map<String, AbstractIndexManager> indexesForTable = indexesByTable.get(manager.getIndex().table);
                            if (indexesForTable != null) {
                                indexesForTable.remove(manager.getIndex().name);
                            }
                        }
                    } finally {
                        generalLock.writeLock().unlock();
                    }
                }

                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position);
                    dbmanager.getPlanner().clearCache();
                }
            }
            break;
            case LogEntryType.ALTER_TABLE: {
                Table table = Table.deserialize(entry.value);
                alterTable(table, null);
                writeTablesOnDataStorageManager(position);
            }
            break;
            default:
                // other entry types are not important for the tablespacemanager
                break;
        }

        if (entry.tableName != null
            && entry.type != LogEntryType.CREATE_TABLE
            && entry.type != LogEntryType.CREATE_INDEX
            && entry.type != LogEntryType.ALTER_TABLE
            && entry.type != LogEntryType.DROP_TABLE) {
            AbstractTableManager tableManager = tables.get(entry.tableName);
            tableManager.apply(position, entry, recovery);
        }

    }

    private void writeTablesOnDataStorageManager(CommitLogResult writeLog) throws DataStorageManagerException,
        LogNotAvailableException {
        LogSequenceNumber logSequenceNumber = writeLog.getLogSequenceNumber();
        List<Table> tablelist = new ArrayList<>();
        List<Index> indexlist = new ArrayList<>();
        for (AbstractTableManager tableManager : tables.values()) {
            if (!tableManager.isSystemTable()) {
                tablelist.add(tableManager.getTable());
            }
        }
        for (AbstractIndexManager indexManager : indexes.values()) {
            indexlist.add(indexManager.getIndex());
        }
        dataStorageManager.writeTables(tableSpaceUUID, logSequenceNumber, tablelist, indexlist);
    }

    DataScanner scan(ScanStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        boolean rollbackOnError = false;
        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID) {
            StatementExecutionResult newTransaction = beginTransaction();
            transactionContext = new TransactionContext(newTransaction.transactionId);
            rollbackOnError = true;
        }
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
            throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
        }
        try {
            String table = statement.getTable();
            AbstractTableManager tableManager = tables.get(table);
            if (tableManager == null) {
                throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName);
            }
            if (tableManager.getCreatedInTransaction() > 0) {
                if (transaction == null || transaction.transactionId != tableManager.getCreatedInTransaction()) {
                    throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName + ". created temporary in transaction " + tableManager.getCreatedInTransaction());
                }
            }
            return tableManager.scan(statement, context, transaction);
        } catch (StatementExecutionException error) {
            if (rollbackOnError) {
                rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, transactionContext.transactionId));
            }
            throw error;
        }
    }

    private void downloadTableSpaceData() throws MetadataStorageManagerException, DataStorageManagerException, LogNotAvailableException {
        TableSpace tableSpaceData = metadataStorageManager.describeTableSpace(tableSpaceName);
        String leaderId = tableSpaceData.leaderId;
        if (this.nodeId.equals(leaderId)) {
            throw new DataStorageManagerException("cannot download data of tableSpace " + tableSpaceName + " from myself");
        }
        Optional<NodeMetadata> leaderAddress = metadataStorageManager.listNodes().stream().filter(n -> n.nodeId.equals(leaderId)).findAny();
        if (!leaderAddress.isPresent()) {
            throw new DataStorageManagerException("cannot download data of tableSpace " + tableSpaceName + " from leader " + leaderId + ", no metadata found");
        }
        NodeMetadata nodeData = leaderAddress.get();
        ClientConfiguration clientConfiguration = new ClientConfiguration(dbmanager.getTmpDirectory());
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, dbmanager.getServerToServerUsername());
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, dbmanager.getServerToServerPassword());
        try (HDBClient client = new HDBClient(clientConfiguration);) {
            client.setClientSideMetadataProvider(new ClientSideMetadataProvider() {
                @Override
                public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
                    return leaderId;
                }

                @Override
                public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
                    return new ServerHostData(nodeData.host, nodeData.port, "?", nodeData.ssl, Collections.emptyMap());
                }
            });
            try (HDBConnection con = client.openConnection()) {
                ReplicaFullTableDataDumpReceiver receiver = new ReplicaFullTableDataDumpReceiver(this);
                int fetchSize = 10000;
                con.dumpTableSpace(tableSpaceName, receiver, fetchSize, false);
                long _start = System.currentTimeMillis();
                boolean ok = receiver.join(1000 * 60 * 60);
                if (!ok) {
                    throw new DataStorageManagerException("Cannot receive dump within " + (System.currentTimeMillis() - _start) + " ms");
                }
                if (receiver.getError() != null) {
                    throw new DataStorageManagerException("Error while receiving dump: " + receiver.getError(), receiver.getError());
                }
                this.actualLogSequenceNumber = receiver.logSequenceNumber;
                LOGGER.log(Level.SEVERE, "After download local actualLogSequenceNumber is " + actualLogSequenceNumber);

            } catch (ClientSideMetadataProviderException | HDBException | InterruptedException networkError) {
                throw new DataStorageManagerException(networkError);
            }

        }

    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    public List<Table> getAllCommittedTables() {
        // No LOCK is necessary, since tables is a concurrent map and this function is only for
        // system monitoring
        return tables.values().stream().filter(s -> s.getCreatedInTransaction() == 0).map(AbstractTableManager::getTable).collect(Collectors.toList());

    }

    private StatementExecutionResult alterTable(AlterTableStatement alterTableStatement, TransactionContext transactionContext) throws TableDoesNotExistException, StatementExecutionException {

        if (transactionContext.transactionId > 0) {
            throw new StatementExecutionException("ALTER TABLE cannot be executed inside a transaction (txid=" + transactionContext.transactionId + ")");
        }
        AbstractTableManager tableManager = tables.get(alterTableStatement.getTable());
        if (tableManager == null) {
            throw new TableDoesNotExistException("no table " + alterTableStatement.getTable() + " in tablespace " + tableSpaceName);
        }

        Table newTable;
        try {
            newTable = tableManager.getTable().applyAlterTable(alterTableStatement);
        } catch (IllegalArgumentException error) {
            throw new StatementExecutionException(error);
        }
        LogEntry entry = LogEntryFactory.alterTable(newTable, null);
        try {
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }
        return new DDLStatementExecutionResult(transactionContext.transactionId);

    }

    public Map<String, AbstractIndexManager> getIndexesOnTable(String name) {
        Map<String, AbstractIndexManager> result = indexesByTable.get(name);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return result;
    }

    boolean isTransactionRunningOnTable(String name) {
        return transactions
            .values()
            .stream()
            .anyMatch((t) -> (t.isOnTable(name)));
    }

    long handleLocalMemoryUsage() {
        long result = 0;
        for (AbstractTableManager tableManager : tables.values()) {
            TableManagerStats stats = tableManager.getStats();
            result += stats.getBuffersUsedMemory();
            result += stats.getKeysUsedMemory();
            result += stats.getDirtyUsedMemory();
        }
        return result;
    }

    private static class CheckpointFuture extends CompletableFuture {

        private final String tableName;

        public CheckpointFuture(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 31 * hash + Objects.hashCode(this.tableName);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final CheckpointFuture other = (CheckpointFuture) obj;
            if (!Objects.equals(this.tableName, other.tableName)) {
                return false;
            }
            return true;
        }

    }

    void requestTableCheckPoint(String name) {
        boolean ok = tablesNeedingCheckPoint.add(name);
        if (ok) {
            LOGGER.log(Level.SEVERE, "Table " + this.tableSpaceName + "." + name + " need a local checkpoint");
        }
        dbmanager.triggerActivator(ActivatorRunRequest.TABLECHECKPOINTS);
    }

    void runLocalTableCheckPoints() {
        Set<String> tablesToDo = new HashSet<>(tablesNeedingCheckPoint);
        tablesNeedingCheckPoint.clear();
        for (String table : tablesToDo) {
            LOGGER.log(Level.SEVERE, "Forcing local checkpoint table " + this.tableSpaceName + "." + table);
            AbstractTableManager tableManager = tables.get(table);
            if (tableManager != null) {
                try {
                    tableManager.checkpoint(log.getLastSequenceNumber());
                } catch (DataStorageManagerException ex) {
                    LOGGER.log(Level.SEVERE, "Bad error on table checkpoint", ex);
                }
            }
        }
    }

    public void restoreRawDumpedEntryLogs(List<DumpedLogEntry> entries) throws DataStorageManagerException, DDLException {
        generalLock.readLock().lock();
        try {
            for (DumpedLogEntry ld : entries) {
                apply(new CommitLogResult(ld.logSequenceNumber, false),
                    LogEntry.deserialize(ld.entryData), true);
            }
        } finally {
            generalLock.readLock().unlock();
        }
    }

    public void beginRestoreTable(byte[] tableDef, LogSequenceNumber dumpLogSequenceNumber) {
        Table table = Table.deserialize(tableDef);
        generalLock.writeLock().lock();
        try {
            if (tables.containsKey(table.name)) {
                throw new TableAlreadyExistsException(table.name);
            }
            bootTable(table, 0, dumpLogSequenceNumber);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    public void restoreTableFinished(String table, List<Index> indexes) {
        TableManager tableManager = (TableManager) tables.get(table);
        tableManager.restoreFinished();

        for (Index index : indexes) {
            bootIndex(index, tableManager, 0, true);
        }
    }

    public void restoreRawDumpedTransactions(List<Transaction> entries) {
        for (Transaction ld : entries) {
            LOGGER.log(Level.SEVERE, "restore transaction " + ld);
            transactions.put(ld.transactionId, ld);
        }
    }

    void dumpTableSpace(String dumpId, Channel _channel, int fetchSize, boolean includeLog) throws DataStorageManagerException, LogNotAvailableException {

        checkpoint();
        LOGGER.log(Level.SEVERE, "dumpTableSpace dumpId:" + dumpId + " channel " + _channel + " fetchSize:" + fetchSize + ", includeLog:" + includeLog);

        List<DumpedLogEntry> txlogentries = new CopyOnWriteArrayList<>();
        CommitLogListener logDumpReceiver = new CommitLogListener() {
            @Override
            public void logEntry(LogSequenceNumber logPos, LogEntry data) {
                // we are going to capture all the canges to the tablespace during the dump, in order to replay
                // eventually 'missed' changes during the dump
                txlogentries.add(new DumpedLogEntry(logPos, data.serialize()));
                LOGGER.log(Level.SEVERE, "dumping entry " + logPos + ", " + data + " nentries: " + txlogentries.size());
            }
        };
        generalLock.readLock().lock();
        try {
            if (includeLog) {
                log.attachCommitLogListener(logDumpReceiver);
            }
            final int timeout = 60000;
            Map<String, Object> startData = new HashMap<>();
            startData.put("command", "start");
            LogSequenceNumber logSequenceNumber = log.getLastSequenceNumber();
            startData.put("ledgerid", logSequenceNumber.ledgerId);
            startData.put("offset", logSequenceNumber.offset);
            Message response_to_start = _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, startData), timeout);
            if (response_to_start.type != Message.TYPE_ACK) {
                LOGGER.log(Level.SEVERE, "error response at start command: " + response_to_start.parameters);
                return;
            }

            if (includeLog) {
                List<Transaction> transactionsSnapshot = new ArrayList<>();
                dataStorageManager.loadTransactions(logSequenceNumber, tableSpaceUUID, transactionsSnapshot::add);
                List<Transaction> batch = new ArrayList<>();
                for (Transaction t : transactionsSnapshot) {
                    batch.add(t);
                    if (batch.size() == 10) {
                        sendTransactionsDump(batch, _channel, dumpId, timeout, response_to_start);
                    }
                }
                sendTransactionsDump(batch, _channel, dumpId, timeout, response_to_start);
            }

            for (AbstractTableManager tableManager : tables.values()) {
                if (tableManager.isSystemTable()) {
                    continue;
                }
                try {
                    FullTableScanConsumer sink = new SingleTableDumper(tableSpaceName, tableManager, _channel, dumpId, timeout, fetchSize);
                    tableManager.dump(sink);
                } catch (DataStorageManagerException err) {
                    Map<String, Object> errorOnData = new HashMap<>();
                    errorOnData.put("command", "error");
                    _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, errorOnData), timeout);
                    LOGGER.log(Level.SEVERE, "error sending dump id " + dumpId, err);
                    return;
                }
            }

            if (!txlogentries.isEmpty()) {
                txlogentries.sort(Comparator.naturalOrder());
                sendDumpedCommitLog(txlogentries, _channel, dumpId, timeout);
            }

            Map<String, Object> finishData = new HashMap<>();
            LogSequenceNumber finishLogSequenceNumber = log.getLastSequenceNumber();
            finishData.put("ledgerid", finishLogSequenceNumber.ledgerId);
            finishData.put("offset", finishLogSequenceNumber.offset);
            finishData.put("command", "finish");
            _channel.sendOneWayMessage(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, finishData), new SendResultCallback() {
                @Override
                public void messageSent(Message originalMessage, Throwable error) {
                }
            });
        } catch (InterruptedException | TimeoutException error) {
            LOGGER.log(Level.SEVERE, "error sending dump id " + dumpId);
        } finally {
            generalLock.readLock().unlock();
            if (includeLog) {
                log.removeCommitLogListener(logDumpReceiver);
            }
        }

    }

    private void sendTransactionsDump(List<Transaction> batch, Channel _channel, String dumpId, final int timeout, Message response_to_start) throws TimeoutException, InterruptedException {
        if (batch.isEmpty()) {
            return;
        }
        Map<String, Object> transactionsData = new HashMap<>();
        transactionsData.put("command", "transactions");
        List<byte[]> encodedTransactions = batch
            .stream()
            .map(tr -> {
                return tr.serialize();
            })
            .
            collect(Collectors.toList());
        transactionsData.put("transactions", encodedTransactions);
        Message response_to_transactionsData = _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, transactionsData), timeout);
        if (response_to_transactionsData.type != Message.TYPE_ACK) {
            LOGGER.log(Level.SEVERE, "error response at transactionsData command: " + response_to_start.parameters);
        }
        batch.clear();
    }

    private void sendDumpedCommitLog(List<DumpedLogEntry> txlogentries, Channel _channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        Map<String, Object> data = new HashMap<>();
        List<KeyValue> batch = new ArrayList<>();
        for (DumpedLogEntry e : txlogentries) {
            batch.add(new KeyValue(e.logSequenceNumber.serialize(), e.entryData));
        }
        data.put("command", "txlog");
        data.put("records", batch);
        _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, data), timeout);
    }

    public void restoreFinished() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "restore finished of tableSpace " + tableSpaceName + ". requesting checkpoint");
        transactions.clear();
        checkpoint();
    }

    public boolean isVirtual() {
        return virtual;
    }

    private class FollowerThread implements Runnable {

        @Override
        public String toString() {
            return "FollowerThread{" + tableSpaceName + '}';
        }

        @Override
        public void run() {
            try {
                while (!isLeader() && !closed) {
                    log.followTheLeader(actualLogSequenceNumber, new BiConsumer< LogSequenceNumber, LogEntry>() {
                        @Override
                        public void accept(LogSequenceNumber num, LogEntry u
                        ) {
                            try {
                                apply(new CommitLogResult(num, false), u, false);
                            } catch (Throwable t) {
                                throw new RuntimeException(t);
                            }
                        }
                    });
                }
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "follower error " + tableSpaceName, t);
                setFailed();
            }
        }

    }

    void setFailed() {
        failed = true;
    }

    public boolean isFailed() {
        if (virtual) {
            return false;
        }
        return failed || log.isFailed();
    }

    void startAsFollower() throws DataStorageManagerException, DDLException, LogNotAvailableException {
        dbmanager.submit(new FollowerThread());
    }

    void startAsLeader() throws DataStorageManagerException, DDLException, LogNotAvailableException {
        if (virtual) {

        } else {

            LOGGER.log(Level.SEVERE, "startAsLeader {0} tablespace {1}", new Object[]{nodeId, tableSpaceName});
            recoverForLeadership();

            // every pending transaction MUST be rollback back
            List<Long> pending_transactions = new ArrayList<>(this.transactions.keySet());
            log.startWriting();
            LOGGER.log(Level.SEVERE, "startAsLeader {0} tablespace {1} log, there were {2} pending transactions to be rolledback", new Object[]{nodeId, tableSpaceName, pending_transactions.size()});
            for (long tx : pending_transactions) {
                LOGGER.log(Level.SEVERE, "rolling back transaction {0}", tx);
                LogEntry rollback = LogEntryFactory.rollbackTransaction(tableSpaceName, tx);
                // let followers see the rollback on the log
                CommitLogResult pos = log.log(rollback, true);
                apply(pos, rollback, false);
            }
        }
        leader = true;
    }

    private final ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();

    StatementExecutionResult executeStatement(Statement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        if (virtual) {
            throw new StatementExecutionException("executeStatement not available on virtual tablespaces");
        }
        boolean rollbackOnError = false;

        /* Do not autostart transaction on alter table statements */
        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID && statement.supportsTransactionAutoCreate()) {
            StatementExecutionResult newTransaction = beginTransaction();
            transactionContext = new TransactionContext(newTransaction.transactionId);
            rollbackOnError = true;
        }

        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
            throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
        }
        if (transactionContext.transactionId > 0 && transaction == null) {
            throw new StatementExecutionException("transaction " + transactionContext.transactionId + " not found on tablespace " + tableSpaceName);
        }
        try {
            if (statement instanceof CreateTableStatement) {
                return createTable((CreateTableStatement) statement, transaction);
            }
            if (statement instanceof CreateIndexStatement) {
                return createIndex((CreateIndexStatement) statement, transaction);
            }
            if (statement instanceof DropTableStatement) {
                return dropTable((DropTableStatement) statement, transaction);
            }
            if (statement instanceof DropIndexStatement) {
                return dropIndex((DropIndexStatement) statement, transaction);
            }
            if (statement instanceof BeginTransactionStatement) {
                if (transaction != null) {
                    throw new IllegalArgumentException("transaction already started");
                }
                return beginTransaction();
            }
            if (statement instanceof RollbackTransactionStatement) {
                return rollbackTransaction((RollbackTransactionStatement) statement);
            }
            if (statement instanceof CommitTransactionStatement) {
                return commitTransaction((CommitTransactionStatement) statement);
            }
            if (statement instanceof AlterTableStatement) {
                return alterTable((AlterTableStatement) statement, transactionContext);
            }
            if (statement instanceof TableAwareStatement) {
                TableAwareStatement st = (TableAwareStatement) statement;
                String table = st.getTable();
                AbstractTableManager manager = tables.get(table);
                if (manager == null) {
                    throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName);
                }
                if (manager.getCreatedInTransaction() > 0) {
                    if (transaction == null || transaction.transactionId != manager.getCreatedInTransaction()) {
                        throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName + ". created temporary in transaction " + manager.getCreatedInTransaction());
                    }
                }
                return manager.executeStatement(statement, transaction, context);
            }

            throw new StatementExecutionException("unsupported statement " + statement);
        } catch (StatementExecutionException error) {
            if (rollbackOnError) {
                rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, transactionContext.transactionId));
            }
            throw error;
        }
    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction) throws StatementExecutionException {
        generalLock.writeLock().lock();
        try {
            if (tables.containsKey(statement.getTableDefinition().name)) {
                throw new TableAlreadyExistsException(statement.getTableDefinition().name);
            }
            for (Index additionalIndex : statement.getAdditionalIndexes()) {
                if (indexes.containsKey(additionalIndex.name)) {
                    throw new IndexAlreadyExistsException(additionalIndex.name);
                }
            }

            LogEntry entry = LogEntryFactory.createTable(statement.getTableDefinition(), transaction);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);

            for (Index additionalIndex : statement.getAdditionalIndexes()) {
                LogEntry index_entry = LogEntryFactory.createIndex(additionalIndex, transaction);
                CommitLogResult index_pos = log.log(index_entry, index_entry.transactionId <= 0);
                apply(index_pos, index_entry, false);
            }

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    private StatementExecutionResult createIndex(CreateIndexStatement statement, Transaction transaction) throws StatementExecutionException {
        generalLock.writeLock().lock();
        try {
            if (indexes.containsKey(statement.getIndexefinition().name)) {
                throw new IndexAlreadyExistsException(statement.getIndexefinition().name);
            }
            LogEntry entry = LogEntryFactory.createIndex(statement.getIndexefinition(), transaction);
            CommitLogResult pos;
            try {
                pos = log.log(entry, entry.transactionId <= 0);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    private StatementExecutionResult dropTable(DropTableStatement statement, Transaction transaction) throws StatementExecutionException {
        try {
            generalLock.writeLock().lock();
            if (!tables.containsKey(statement.getTable())) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction != null ? transaction.transactionId : 0);
                }
                throw new TableDoesNotExistException("table does not exist " + statement.getTable() + " on tableSpace " + statement.getTableSpace());
            }
            if (transaction != null && transaction.isTableDropped(statement.getTable())) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction.transactionId);
                }
                throw new TableDoesNotExistException("table does not exist " + statement.getTable() + " on tableSpace " + statement.getTableSpace());
            }

            Map<String, AbstractIndexManager> indexesOnTable = indexesByTable.get(statement.getTable());
            if (indexesOnTable != null) {
                for (String index : indexesOnTable.keySet()) {
                    LogEntry entry = LogEntryFactory.dropIndex(statement.getTableSpace(), index, transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    apply(pos, entry, false);
                }
            }

            LogEntry entry = LogEntryFactory.dropTable(statement.getTableSpace(), statement.getTable(), transaction);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    private StatementExecutionResult dropIndex(DropIndexStatement statement, Transaction transaction) throws StatementExecutionException {
        try {
            generalLock.writeLock().lock();
            if (!indexes.containsKey(statement.getIndexName())) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction != null ? transaction.transactionId : 0);
                }
                throw new IndexDoesNotExistException("index " + statement.getIndexName() + " does not exist " + statement.getIndexName() + " on tableSpace " + statement.getTableSpace());
            }
            if (transaction != null && transaction.isIndexDropped(statement.getIndexName())) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction.transactionId);
                }
                throw new IndexDoesNotExistException("index does not exist " + statement.getIndexName() + " on tableSpace " + statement.getTableSpace());
            }
            LogEntry entry = LogEntryFactory.dropIndex(statement.getTableSpace(), statement.getIndexName(), transaction);
            CommitLogResult pos;
            try {
                pos = log.log(entry, entry.transactionId <= 0);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    TableManager bootTable(Table table, long transaction, LogSequenceNumber dumpLogSequenceNumber) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "bootTable {0} {1}.{2}", new Object[]{nodeId, tableSpaceName, table.name});
        if (tables.containsKey(table.name)) {
            throw new DataStorageManagerException("Table " + table.name + " already present in tableSpace " + tableSpaceName);
        }
        TableManager tableManager = new TableManager(
                table, log, dbmanager.getMemoryManager(), dataStorageManager, this, tableSpaceUUID, transaction);
        if (dbmanager.getServerConfiguration().getBoolean(
                ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT)) {
            JMXUtils.registerTableManagerStatsMXBean(tableSpaceName, table.name, tableManager.getStats());
        }


        if (dumpLogSequenceNumber != null) {
            tableManager.prepareForRestore(dumpLogSequenceNumber);
        }
        tables.put(table.name, tableManager);
        tableManager.start();
        LOGGER.log(Level.SEVERE, "bootTable {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, table.name, (System.currentTimeMillis() - _start) + ""});
        dbmanager.getPlanner().clearCache();
        return tableManager;
    }

    private AbstractIndexManager bootIndex(Index index, AbstractTableManager tableManager, long transaction, boolean rebuild) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "bootIndex {0} {1}.{2}.{3}", new Object[]{nodeId, tableSpaceName, index.table, index.name});
        if (indexes.containsKey(index.name)) {
            throw new DataStorageManagerException("Index" + index.name + " already present in tableSpace " + tableSpaceName);
        }

        AbstractIndexManager indexManager;
        switch (index.type) {
            case Index.TYPE_HASH:
                indexManager = new MemoryHashIndexManager(index, tableManager, log, dataStorageManager, this, tableSpaceUUID, transaction);
                break;
            case Index.TYPE_BRIN:
                indexManager = new BRINIndexManager(index, dbmanager.getMemoryManager(), tableManager, log, dataStorageManager, this, tableSpaceUUID, transaction);
                break;
            default:
                throw new DataStorageManagerException("invalid index type " + index.type);
        }

        indexes.put(index.name, indexManager);

        Map<String, AbstractIndexManager> newMap = new HashMap<>(); // this must be mutable (see DROP INDEX)
        newMap.put(index.name, indexManager);

        indexesByTable.merge(index.table, newMap, (a, b) -> {
            Map<String, AbstractIndexManager> newList = new HashMap<>(a);
            newList.putAll(b);
            return newList;
        });
        indexManager.start();
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "bootIndex {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, index.name, (_stop - _start) + ""});
        if (rebuild) {
            indexManager.rebuild();
            LOGGER.log(Level.SEVERE, "bootIndex - rebuild {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, index.name, (System.currentTimeMillis() - _stop) + ""});
        }
        dbmanager.getPlanner().clearCache();
        return indexManager;
    }

    private AbstractTableManager alterTable(Table table, Transaction transaction) throws DDLException {
        LOGGER.log(Level.SEVERE, "alterTable {0} {1}.{2}", new Object[]{nodeId, tableSpaceName, table.name});
        AbstractTableManager tableManager = tables.get(table.name);
        tableManager.tableAltered(table, transaction);
        return tableManager;
    }

    public void close() throws LogNotAvailableException {
        boolean useJmx = dbmanager.getServerConfiguration().getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT);
        closed = true;
        if (!virtual) {
            generalLock.writeLock().lock();
            try {
                for (Map.Entry<String, AbstractTableManager> table : tables.entrySet()) {
                    if (useJmx) {
                        JMXUtils.unregisterTableManagerStatsMXBean(tableSpaceName, table.getKey());
                    }
                    table.getValue().close();

                }
                for (AbstractIndexManager index : indexes.values()) {
                    index.close();
                }
                log.close();
            } finally {
                generalLock.writeLock().unlock();
            }
        }
        if (useJmx) {
            JMXUtils.unregisterTableSpaceManagerStatsMXBean(tableSpaceName);
        }
    }

    LogSequenceNumber checkpoint() throws DataStorageManagerException, LogNotAvailableException {
        if (virtual) {
            return null;
        }
        long _start = System.currentTimeMillis();
        LogSequenceNumber logSequenceNumber;
        LogSequenceNumber _logSequenceNumber;
        List<PostCheckpointAction> actions = new ArrayList<>();
        generalLock.writeLock().lock();
        try {
            logSequenceNumber = log.getLastSequenceNumber();

            if (logSequenceNumber.isStartOfTime()) {
                LOGGER.log(Level.SEVERE, nodeId + " checkpoint " + tableSpaceName + " at " + logSequenceNumber + ". skipped (no write ever issued to log)");
                return logSequenceNumber;
            }
            LOGGER.log(Level.SEVERE, nodeId + " checkpoint start " + tableSpaceName + " at " + logSequenceNumber);
            if (actualLogSequenceNumber == null) {
                throw new DataStorageManagerException("actualLogSequenceNumber cannot be null");
            }

            // TODO: transactions checkpoint is not atomic
            dataStorageManager.writeTransactionsAtCheckpoint(tableSpaceUUID, logSequenceNumber, new ArrayList<>(transactions.values()));
            writeTablesOnDataStorageManager(new CommitLogResult(logSequenceNumber, false));
            // we are sure that all data as been flushed. upon recovery we will replay the log starting from this position
            dataStorageManager.writeCheckpointSequenceNumber(tableSpaceUUID, logSequenceNumber);

            // we checkpoint all data to disk and save the actual log sequence number
            for (AbstractTableManager tableManager : tables.values()) {
                // each TableManager will save its own checkpoint sequence number (on TableStatus) and upon recovery will replay only actions with log position after the actual table-local checkpoint
                // remember that the checkpoint for a table can last "minutes" and we do not want to stop the world
                LogSequenceNumber sequenceNumber = log.getLastSequenceNumber();
                List<PostCheckpointAction> postCheckPointActions = tableManager.checkpoint(sequenceNumber);
                actions.addAll(postCheckPointActions);
            }

            for (AbstractIndexManager indexManager : indexes.values()) {
                // same as for TableManagers
                LogSequenceNumber sequenceNumber = log.getLastSequenceNumber();
                List<PostCheckpointAction> postCheckPointActions = indexManager.checkpoint(sequenceNumber);
                actions.addAll(postCheckPointActions);
            }

            log.dropOldLedgers(logSequenceNumber);

            _logSequenceNumber = log.getLastSequenceNumber();
        } finally {
            generalLock.writeLock().unlock();
        }

        for (PostCheckpointAction action : actions) {
            try {
                action.run();
            } catch (Exception error) {
                LOGGER.log(Level.SEVERE, "postcheckpoint error:" + error, error);
            }
        }
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, nodeId + " checkpoint finish " + tableSpaceName
            + " started at " + logSequenceNumber
            + ", finished at " + _logSequenceNumber
            + ", total time " + (_stop - _start) + " ms");

        return logSequenceNumber;
    }

    private StatementExecutionResult beginTransaction() throws StatementExecutionException {
        long id = newTransactionId.incrementAndGet();

        LogEntry entry = LogEntryFactory.beginTransaction(tableSpaceName, id);
        CommitLogResult pos;
        try {
            pos = log.log(entry, false);
            apply(pos, entry, false);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }

        return new TransactionResult(id, TransactionResult.OutcomeType.BEGIN);
    }

    private StatementExecutionResult rollbackTransaction(RollbackTransactionStatement rollbackTransactionStatement) throws StatementExecutionException {
        Transaction tx = transactions.get(rollbackTransactionStatement.getTransactionId());
        if (tx == null) {
            throw new StatementExecutionException("no such transaction " + rollbackTransactionStatement.getTransactionId());
        }
        LogEntry entry = LogEntryFactory.rollbackTransaction(tableSpaceName, tx.transactionId);
        try {
            CommitLogResult pos = log.log(entry, true);
            apply(pos, entry, false);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }

        return new TransactionResult(tx.transactionId, TransactionResult.OutcomeType.ROLLBACK);
    }

    private StatementExecutionResult commitTransaction(CommitTransactionStatement commitTransactionStatement) throws StatementExecutionException {
        Transaction tx = transactions.get(commitTransactionStatement.getTransactionId());
        if (tx == null) {
            throw new StatementExecutionException("no such transaction " + commitTransactionStatement.getTransactionId());
        }
        LogEntry entry = LogEntryFactory.commitTransaction(tableSpaceName, tx.transactionId);

        try {
            CommitLogResult pos = log.log(entry, true);
            apply(pos, entry, false);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }

        return new TransactionResult(tx.transactionId, TransactionResult.OutcomeType.COMMIT);
    }

    public boolean isLeader() {
        return leader;
    }

    Transaction getTransaction(long transactionId) {
        return transactions.get(transactionId);
    }

    public AbstractTableManager getTableManager(String tableName) {
        return tables.get(tableName);
    }

    public Collection<Long> getOpenTransactions() {
        return new HashSet<>(this.transactions.keySet());
    }

    public List<Transaction> getTransactions() {
        return new ArrayList<>(this.transactions.values());
    }

    private class ApplyEntryOnRecovery implements BiConsumer<LogSequenceNumber, LogEntry> {

        public ApplyEntryOnRecovery() {
        }

        @Override
        public void accept(LogSequenceNumber t, LogEntry u) {
            try {
                apply(new CommitLogResult(t, false), u, true);
            } catch (DDLException | DataStorageManagerException err) {
                throw new RuntimeException(err);
            }
        }
    }

    public DBManager getDbmanager() {
        return dbmanager;
    }

    private final TableSpaceManagerStats stats = new TableSpaceManagerStats() {

        @Override
        public int getLoadedpages() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToInt(TableManagerStats::getLoadedpages)
                .sum();

        }

        @Override
        public long getLoadedPagesCount() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getLoadedPagesCount)
                .sum();
        }

        @Override
        public long getUnloadedPagesCount() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getUnloadedPagesCount)
                .sum();
        }

        @Override
        public long getTablesize() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getTablesize)
                .sum();
        }

        @Override
        public int getDirtypages() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToInt(TableManagerStats::getDirtypages)
                .sum();
        }

        @Override
        public int getDirtyrecords() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToInt(TableManagerStats::getDirtyrecords)
                .sum();
        }

        @Override
        public long getDirtyUsedMemory() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getDirtyUsedMemory)
                .sum();
        }

        @Override
        public long getMaxLogicalPageSize() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getMaxLogicalPageSize)
                .findFirst()
                .orElse(0);
        }

        @Override
        public long getBuffersUsedMemory() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getBuffersUsedMemory)
                .sum();
        }

        @Override
        public long getKeysUsedMemory() {
            return tables.values()
                .stream()
                .map(AbstractTableManager::getStats)
                .mapToLong(TableManagerStats::getKeysUsedMemory)
                .sum();
        }
    };

    public TableSpaceManagerStats getStats() {
        return stats;
    }

    public CommitLog getLog() {
        return log;
    }

}
