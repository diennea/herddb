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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
import herddb.core.AbstractTableManager.TableCheckpoint;
import herddb.core.stats.TableManagerStats;
import herddb.core.stats.TableSpaceManagerStats;
import herddb.core.system.SysclientsTableManager;
import herddb.core.system.SyscolumnsTableManager;
import herddb.core.system.SysconfigTableManager;
import herddb.core.system.SysindexcolumnsTableManager;
import herddb.core.system.SysindexesTableManager;
import herddb.core.system.SyslogstatusManager;
import herddb.core.system.SysnodesTableManager;
import herddb.core.system.SysstatementsTableManager;
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
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.commands.ScanStatement;
import herddb.network.Channel;
import herddb.utils.KeyValue;
import herddb.network.MessageBuilder;
import herddb.network.MessageWrapper;
import herddb.network.ServerHostData;
import herddb.proto.flatbuf.MessageType;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.utils.Bytes;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

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
    private final StampedLock generalLock = new StampedLock();
    private final AtomicLong newTransactionId = new AtomicLong();
    private final DBManager dbmanager;
    private final boolean virtual;

    private volatile boolean leader;
    private volatile boolean closed;
    private volatile boolean failed;
    private LogSequenceNumber actualLogSequenceNumber;

    // only for tests
    private Runnable afterTableCheckPointAction;

    public Runnable getAfterTableCheckPointAction() {
        return afterTableCheckPointAction;
    }

    public void setAfterTableCheckPointAction(Runnable afterTableCheckPointAction) {
        this.afterTableCheckPointAction = afterTableCheckPointAction;
    }

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
            registerSystemTableManager(new SysindexcolumnsTableManager(this));
            registerSystemTableManager(new SyscolumnsTableManager(this));
            registerSystemTableManager(new SystransactionsTableManager(this));
            registerSystemTableManager(new SyslogstatusManager(this));
        }
        registerSystemTableManager(new SystablespacesTableManager(this));
        registerSystemTableManager(new SystablespacereplicastateTableManager(this));
        registerSystemTableManager(new SysnodesTableManager(this));
        registerSystemTableManager(new SysstatementsTableManager(this));

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

            LOGGER.log(Level.INFO, " after recovery of tableSpace " + tableSpaceName + ", actualLogSequenceNumber:" + actualLogSequenceNumber);

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
        LOGGER.log(Level.INFO, "{0} recover {1}, logSequenceNumber from DataStorage: {2}", new Object[]{nodeId, tableSpaceName, logSequenceNumber});
        List<Table> tablesAtBoot = dataStorageManager.loadTables(logSequenceNumber, tableSpaceUUID);
        List<Index> indexesAtBoot = dataStorageManager.loadIndexes(logSequenceNumber, tableSpaceUUID);
        String tableNames = tablesAtBoot.stream().map(t -> {
            return t.name;
        }).collect(Collectors.joining(","));

        String indexNames = indexesAtBoot.stream().map(t -> {
            return t.name + " on table " + t.table;
        }).collect(Collectors.joining(","));

        LOGGER.log(Level.INFO, "{0} {1} tablesAtBoot {2}, indexesAtBoot {3}", new Object[]{nodeId, tableSpaceName, tableNames, indexNames});

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
            LOGGER.log(Level.FINER, "{0} {1} tx {2} at boot", new Object[]{nodeId, tableSpaceName, t.transactionId});
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

        try {
            log.recovery(logSequenceNumber, new ApplyEntryOnRecovery(), false);
        } catch (FullRecoveryNeededException fullRecoveryNeeded) {
            LOGGER.log(Level.SEVERE, nodeId + " full recovery of data is needed for tableSpace " + tableSpaceName, fullRecoveryNeeded);
            downloadTableSpaceData();
            log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), false);
        }
        checkpoint(false, false, false);

    }

    void recoverForLeadership() throws DataStorageManagerException, LogNotAvailableException {
        actualLogSequenceNumber = log.getLastSequenceNumber();
        LOGGER.log(Level.INFO, "recovering tablespace " + tableSpaceName + " log from sequence number " + actualLogSequenceNumber + ", with fencing");
        log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), true);
    }

    void apply(CommitLogResult position, LogEntry entry, boolean recovery) throws DataStorageManagerException, DDLException {
        if (!position.deferred || position.sync) {
            // this will wait for the write to be acknowledged by the log
            // it can throw LogNotAvailableException
            this.actualLogSequenceNumber = position.getLogSequenceNumber();
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "apply entry {0} {1}", new Object[]{position, entry});
        }
        switch (entry.type) {
            case LogEntryType.NOOP: {
                // NOOP
            }
            break;
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
                List<AbstractTableManager> managers = new ArrayList<>(tables.values());
                for (AbstractTableManager manager : managers) {

                    Table table = manager.getTable();
                    if (transaction.isNewTable(table.name)) {
                        LOGGER.log(Level.INFO, "rollback CREATE TABLE " + table.tablespace + "." + table.name);
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
                    AbstractTableManager manager = tables.get(tableName);
                    if (manager != null) {
                        manager.dropTableData();
                        manager.close();
                        tables.remove(manager.getTable().name);
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

    private Collection<PostCheckpointAction> writeTablesOnDataStorageManager(CommitLogResult writeLog) throws DataStorageManagerException,
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
        return dataStorageManager.writeTables(tableSpaceUUID, logSequenceNumber, tablelist, indexlist);
    }

    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context,
            TransactionContext transactionContext, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        boolean rollbackOnError = false;
        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID) {
            try {
                // sync on beginTransaction
                StatementExecutionResult newTransaction = FutureUtils.result(beginTransactionAsync());
                transactionContext = new TransactionContext(newTransaction.transactionId);
                rollbackOnError = true;
            } catch (Exception err) {
                if (err.getCause() instanceof HerdDBInternalException) {
                    throw (HerdDBInternalException) err.getCause();
                } else {
                    throw new StatementExecutionException(err.getCause());
                }
            }
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
            return tableManager.scan(statement, context, transaction, lockRequired, forWrite);
        } catch (StatementExecutionException error) {
            if (rollbackOnError) {
                try {
                    rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, transactionContext.transactionId)).get();
                } catch (ExecutionException err) {
                    throw new StatementExecutionException(err.getCause());
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    error.addSuppressed(ex);
                }
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
                LOGGER.log(Level.INFO, "After download local actualLogSequenceNumber is " + actualLogSequenceNumber);

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

    public List<Table> getAllTablesForPlanner() {
        // No LOCK is necessary, since tables is a concurrent map
        return tables.values().stream().map(AbstractTableManager::getTable).collect(Collectors.toList());

    }

    private StatementExecutionResult alterTable(AlterTableStatement alterTableStatement, TransactionContext transactionContext) throws TableDoesNotExistException, StatementExecutionException {
        long lockStamp = acquireWriteLock(alterTableStatement);
        try {
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
        } finally {
            releaseWriteLock(lockStamp, alterTableStatement);
        }

    }

    private void releaseWriteLock(long lockStamp, Object description) {
        System.out.println("RELEASE WRITE LOCK " + lockStamp + " for " + description);
        generalLock.unlockWrite(lockStamp);
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
            LOGGER.log(Level.INFO, "Table " + this.tableSpaceName + "." + name + " need a local checkpoint");
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
                    tableManager.checkpoint(false);
                } catch (DataStorageManagerException ex) {
                    LOGGER.log(Level.SEVERE, "Bad error on table checkpoint", ex);
                }
            }
        }
    }

    public void restoreRawDumpedEntryLogs(List<DumpedLogEntry> entries) throws DataStorageManagerException, DDLException, EOFException {
        long lockStamp = acquireWriteLock("restoreRawDumpedEntryLogs");
        try {
            for (DumpedLogEntry ld : entries) {
                apply(new CommitLogResult(ld.logSequenceNumber, false, false),
                        LogEntry.deserialize(ld.entryData), true);
            }
        } finally {
            releaseWriteLock(lockStamp, "restoreRawDumpedEntryLogs");
        }
    }

    public void beginRestoreTable(byte[] tableDef, LogSequenceNumber dumpLogSequenceNumber) {
        Table table = Table.deserialize(tableDef);
        long lockStamp = acquireWriteLock("beginRestoreTable " + table.name);
        try {
            if (tables.containsKey(table.name)) {
                throw new TableAlreadyExistsException(table.name);
            }
            bootTable(table, 0, dumpLogSequenceNumber);
        } finally {
            releaseWriteLock(lockStamp, "beginRestoreTable " + table.name);
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
            LOGGER.log(Level.INFO, "restore transaction " + ld);
            transactions.put(ld.transactionId, ld);
        }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE")
    void dumpTableSpace(String dumpId, Channel _channel, int fetchSize, boolean includeLog) throws DataStorageManagerException, LogNotAvailableException {

        LOGGER.log(Level.INFO, "dumpTableSpace dumpId:{0} channel {1} fetchSize:{2}, includeLog:{3}", new Object[]{dumpId, _channel, fetchSize, includeLog});

        TableSpaceCheckpoint checkpoint;

        List<DumpedLogEntry> txlogentries = new CopyOnWriteArrayList<>();
        CommitLogListener logDumpReceiver = new CommitLogListener() {
            @Override
            public void logEntry(LogSequenceNumber logPos, LogEntry data) {
                // we are going to capture all the changes to the tablespace during the dump, in order to replay
                // eventually 'missed' changes during the dump
                txlogentries.add(new DumpedLogEntry(logPos, data.serialize()));
                //LOGGER.log(Level.SEVERE, "dumping entry " + logPos + ", " + data + " nentries: " + txlogentries.size());
            }
        };

        long lockStamp = acquireWriteLock(null);

        if (includeLog) {
            log.attachCommitLogListener(logDumpReceiver);
        }

        checkpoint = checkpoint(true, true, true /* already locked */);

        /* Downgrade lock */
//        System.err.println("DOWNGRADING LOCK " + lockStamp + " TO READ");
        lockStamp = generalLock.tryConvertToReadLock(lockStamp);
        if (lockStamp == 0) {
            throw new DataStorageManagerException("unable to downgrade lock");
        }
        try {
            final int timeout = 60000;
            LogSequenceNumber logSequenceNumber = log.getLastSequenceNumber();

            long id = _channel.generateRequestId();
            LOGGER.log(Level.INFO, "start sending dump, dumpId: {0} to client {1}", new Object[]{dumpId, _channel});
            try (MessageWrapper response_to_start = _channel.sendMessageWithReply(id, MessageBuilder.TABLESPACE_DUMP_DATA(
                    id, tableSpaceName, dumpId, "start", null, stats.getTablesize(), logSequenceNumber.ledgerId, logSequenceNumber.offset, null, null), timeout);) {

                if (response_to_start.getResponse().type() != MessageType.TYPE_ACK) {
                    LOGGER.log(Level.SEVERE, "error response at start command: " + response_to_start.getResponse().error());
                    return;
                }
            }

            if (includeLog) {
                List<Transaction> transactionsSnapshot = new ArrayList<>();
                dataStorageManager.loadTransactions(logSequenceNumber, tableSpaceUUID, transactionsSnapshot::add);
                List<Transaction> batch = new ArrayList<>();
                for (Transaction t : transactionsSnapshot) {
                    batch.add(t);
                    if (batch.size() == 10) {
                        sendTransactionsDump(batch, _channel, dumpId, timeout);
                    }
                }
                sendTransactionsDump(batch, _channel, dumpId, timeout);
            }

            for (Entry<String, LogSequenceNumber> entry : checkpoint.tablesCheckpoints.entrySet()) {
                final AbstractTableManager tableManager = tables.get(entry.getKey());
                final LogSequenceNumber sequenceNumber = entry.getValue();
                if (tableManager.isSystemTable()) {
                    continue;
                }
                try {
                    FullTableScanConsumer sink = new SingleTableDumper(tableSpaceName, tableManager, _channel, dumpId, timeout, fetchSize);
                    tableManager.dump(sequenceNumber, sink);
                } catch (DataStorageManagerException err) {
                    LOGGER.log(Level.SEVERE, "error sending dump id " + dumpId, err);
                    long errorid = _channel.generateRequestId();
                    try (MessageWrapper response = _channel.sendMessageWithReply(errorid, MessageBuilder.TABLESPACE_DUMP_DATA(
                            id, tableSpaceName, dumpId, "error", null, 0,
                            0, 0,
                            null, null),
                            timeout);) {
                    }
                    return;
                }
            }

            if (!txlogentries.isEmpty()) {
                txlogentries.sort(Comparator.naturalOrder());
                sendDumpedCommitLog(txlogentries, _channel, dumpId, timeout);
            }

            LogSequenceNumber finishLogSequenceNumber = log.getLastSequenceNumber();
            long requestId2 = _channel.generateRequestId();

            _channel.sendMessageWithReply(requestId2, MessageBuilder.TABLESPACE_DUMP_DATA(
                    id, tableSpaceName, dumpId, "finish", null, 0,
                    finishLogSequenceNumber.ledgerId, finishLogSequenceNumber.offset,
                    null, null), timeout);
        } catch (InterruptedException | TimeoutException error) {
            LOGGER.log(Level.SEVERE, "error sending dump id " + dumpId, error);
        } finally {
            releaseReadLock(lockStamp, "senddump");

            if (includeLog) {
                log.removeCommitLogListener(logDumpReceiver);
            }

            for (Entry<String, LogSequenceNumber> entry : checkpoint.tablesCheckpoints.entrySet()) {
                String tableName = entry.getKey();
                AbstractTableManager tableManager = tables.get(tableName);
                String tableUUID = tableManager.getTable().uuid;
                LogSequenceNumber seqNumber = entry.getValue();
                LOGGER.log(Level.INFO, "unPinTableCheckpoint {0}.{1} ({2}) {3}", new Object[]{tableSpaceUUID, tableName, tableUUID, seqNumber});
                dataStorageManager.unPinTableCheckpoint(tableSpaceUUID, tableUUID, seqNumber);
            }
        }

    }

    private void sendTransactionsDump(List<Transaction> batch, Channel _channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        if (batch.isEmpty()) {
            return;
        }
        List<KeyValue> encodedTransactions = batch
                .stream()
                .map(tr -> {
                    return new KeyValue(Bytes.longToByteArray(tr.transactionId), tr.serialize());
                })
                .collect(Collectors.toList());
        long id = _channel.generateRequestId();
        try (MessageWrapper response_to_transactionsData = _channel.sendMessageWithReply(id, MessageBuilder.TABLESPACE_DUMP_DATA(
                id, tableSpaceName, dumpId, "transactions", null, 0,
                0, 0,
                null, encodedTransactions), timeout);) {
            if (response_to_transactionsData.getResponse().type() != MessageType.TYPE_ACK) {
                LOGGER.log(Level.SEVERE, "error response at transactionsData command: " + response_to_transactionsData.getResponse().error());
            }
        }
        batch.clear();
    }

    private void sendDumpedCommitLog(List<DumpedLogEntry> txlogentries, Channel _channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        List<KeyValue> batch = new ArrayList<>();
        for (DumpedLogEntry e : txlogentries) {
            batch.add(new KeyValue(e.logSequenceNumber.serialize(), e.entryData));
        }
        long id = _channel.generateRequestId();
        try (MessageWrapper response_to_txlog = _channel.sendMessageWithReply(id, MessageBuilder.TABLESPACE_DUMP_DATA(
                id, tableSpaceName, dumpId, "txlog", null, 0,
                0, 0,
                null, batch), timeout);) {

            if (response_to_txlog.getResponse().type() != MessageType.TYPE_ACK) {
                LOGGER.log(Level.SEVERE, "error response at txlog command: " + response_to_txlog.getResponse().error());
            }
        }

    }

    public void restoreFinished() throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "restore finished of tableSpace " + tableSpaceName + ". requesting checkpoint");
        transactions.clear();
        checkpoint(false, false, false);
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
                                apply(new CommitLogResult(num, false, true), u, false);
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

            LOGGER.log(Level.INFO, "startAsLeader {0} tablespace {1}", new Object[]{nodeId, tableSpaceName});
            recoverForLeadership();

            // every pending transaction MUST be rollback back
            List<Long> pending_transactions = new ArrayList<>(this.transactions.keySet());
            log.startWriting();
            LOGGER.log(Level.INFO, "startAsLeader {0} tablespace {1} log, there were {2} pending transactions to be rolledback", new Object[]{nodeId, tableSpaceName, pending_transactions.size()});
            for (long tx : pending_transactions) {
                LOGGER.log(Level.FINER, "rolling back transaction {0}", tx);
                LogEntry rollback = LogEntryFactory.rollbackTransaction(tx);
                // let followers see the rollback on the log
                CommitLogResult pos = log.log(rollback, true);
                apply(pos, rollback, false);
            }
        }
        leader = true;
    }

    private final ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();

    public StatementExecutionResult executeStatement(Statement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        CompletableFuture<StatementExecutionResult> res = executeStatementAsync(statement, context, transactionContext);
        try {
            return res.get();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new StatementExecutionException(err);
        } catch (ExecutionException err) {
            Throwable cause = err.getCause();
            if (cause instanceof StatementExecutionException) {
                throw (StatementExecutionException) cause;
            } else {
                throw new StatementExecutionException(cause);
            }
        } catch (Throwable t) {
            throw new StatementExecutionException(t);
        }
    }

    public CompletableFuture<StatementExecutionResult> executeStatementAsync(Statement statement, StatementEvaluationContext context,
            TransactionContext transactionContext) throws StatementExecutionException {

        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID
                && statement.supportsTransactionAutoCreate() // Do not autostart transaction on alter table statements
                ) {
            AtomicLong capturedTx = new AtomicLong();
            CompletableFuture<StatementExecutionResult> newTransaction = beginTransactionAsync();
            CompletableFuture<StatementExecutionResult> finalResult = newTransaction
                    .thenCompose((StatementExecutionResult begineTransactionResult) -> {
                        TransactionContext newtransactionContext = new TransactionContext(begineTransactionResult.transactionId);
                        capturedTx.set(newtransactionContext.transactionId);
                        return executeStatementAsyncInternal(statement, context, newtransactionContext, true);
                    });
            finalResult.whenComplete((xx, error) -> {
                long txId = capturedTx.get();
                if (error != null && txId > 0) {
                    LOGGER.log(Level.SEVERE, "forcing rollback of tx " + txId + " due to " + error);
                    try {
                        rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, txId))
                                .get();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        error.addSuppressed(ex);
                    } catch (ExecutionException ex) {
                        error.addSuppressed(ex.getCause());
                    }
                    throw new HerdDBInternalException(error);
                }
            });

        }
        return executeStatementAsyncInternal(statement, context, transactionContext, false);
    }

    private CompletableFuture<StatementExecutionResult> executeStatementAsyncInternal(Statement statement, StatementEvaluationContext context,
            TransactionContext transactionContext, boolean rollbackOnError) throws StatementExecutionException {
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null
                && !transaction.tableSpace.equals(tableSpaceName)) {
            return FutureUtils.exception(
                    new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName));
        }
        if (transactionContext.transactionId > 0
                && transaction == null) {
            return FutureUtils.exception(
                    new StatementExecutionException("transaction " + transactionContext.transactionId + " not found on tablespace " + tableSpaceName));
        }
        CompletableFuture<StatementExecutionResult> res;
        try {
            if (statement instanceof TableAwareStatement) {
                res = executeTableAwareStatement(statement, transaction, context);
            } else if (statement instanceof SQLPlannedOperationStatement) {
                res = executePlannedOperationStatement(statement, transactionContext, context);
            } else if (statement instanceof BeginTransactionStatement) {
                if (transaction != null) {
                    res = FutureUtils.exception(new StatementExecutionException("transaction already started"));
                } else {
                    res = beginTransactionAsync();
                }
            } else if (statement instanceof CommitTransactionStatement) {
                res = commitTransaction((CommitTransactionStatement) statement);
            } else if (statement instanceof RollbackTransactionStatement) {
                res = rollbackTransaction((RollbackTransactionStatement) statement);
            } else if (statement instanceof CreateTableStatement) {
                res = CompletableFuture.completedFuture(createTable((CreateTableStatement) statement, transaction));
            } else if (statement instanceof CreateIndexStatement) {
                res = CompletableFuture.completedFuture(createIndex((CreateIndexStatement) statement, transaction));
            } else if (statement instanceof DropTableStatement) {
                res = CompletableFuture.completedFuture(dropTable((DropTableStatement) statement, transaction));
            } else if (statement instanceof DropIndexStatement) {
                return CompletableFuture.completedFuture(dropIndex((DropIndexStatement) statement, transaction));
            } else if (statement instanceof AlterTableStatement) {
                return CompletableFuture.completedFuture(alterTable((AlterTableStatement) statement, transactionContext));
            } else {
                res = FutureUtils.exception(new StatementExecutionException("unsupported statement " + statement)
                        .fillInStackTrace());
            }
        } catch (StatementExecutionException error) {
            res = FutureUtils.exception(error);
        }
        if (rollbackOnError) {
            long txId = transactionContext.transactionId;
            if (txId > 0) {
                res = res.whenComplete((xx, error) -> {
                    if (error != null) {
                        LOGGER.log(Level.SEVERE, "forcing rollback of tx " + txId + " due to " + error);
                        try {
                            rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, txId))
                                    .get();
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            error.addSuppressed(ex);
                        } catch (ExecutionException ex) {
                            error.addSuppressed(ex.getCause());
                        }
                        throw new HerdDBInternalException(error);
                    }
                });
            }
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> executePlannedOperationStatement(Statement statement,
            TransactionContext transactionContext, StatementEvaluationContext context) {
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        SQLPlannedOperationStatement planned = (SQLPlannedOperationStatement) statement;
        CompletableFuture<StatementExecutionResult> res
                = planned.getRootOp().executeAsync(this, transactionContext, context, false, false);
        res.whenComplete((ee,err)-> {
            LOGGER.log(Level.SEVERE, "COMPLETED "+statement+": "+ee, err);
        });
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        LOGGER.log(Level.SEVERE, "CREATED "+res);
        return res;
    }

    private CompletableFuture<StatementExecutionResult> executeTableAwareStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws TableDoesNotExistException, StatementExecutionException {
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        TableAwareStatement st = (TableAwareStatement) statement;
        String table = st.getTable();
        AbstractTableManager manager = tables.get(table);
        CompletableFuture<StatementExecutionResult> res;
        if (manager == null) {
            res = FutureUtils.exception(new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName));
        } else if (manager.getCreatedInTransaction() > 0
                && (transaction == null || transaction.transactionId != manager.getCreatedInTransaction())) {
            res = FutureUtils.exception(new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName + ". created temporary in transaction " + manager.getCreatedInTransaction()));
        } else {
            res = manager.executeStatementAsync(statement, transaction, context);
        }
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;

    }

    private long acquireReadLock(Object statement) {
        long lockStamp = generalLock.readLock();
        System.err.println("ACQUIRED TS READLOCK " + lockStamp + " for " + statement);
        return lockStamp;
    }

    private long acquireWriteLock(Object statement) {
        long lockStamp = generalLock.writeLock();
        System.err.println("ACQUIRED TS WRITELOCK " + lockStamp + " for " + statement);
        return lockStamp;
    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction) throws StatementExecutionException {
        long lockStamp = acquireWriteLock(statement);
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
            releaseWriteLock(lockStamp, statement);
        }
    }

    private StatementExecutionResult createIndex(CreateIndexStatement statement, Transaction transaction) throws StatementExecutionException {
        long lockStamp = acquireWriteLock(statement);
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
            releaseWriteLock(lockStamp, statement);
        }
    }

    private StatementExecutionResult dropTable(DropTableStatement statement, Transaction transaction) throws StatementExecutionException {
        long lockStamp = acquireWriteLock(statement);
        try {
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
                    LogEntry entry = LogEntryFactory.dropIndex(index, transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    apply(pos, entry, false);
                }
            }

            LogEntry entry = LogEntryFactory.dropTable(statement.getTable(), transaction);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            releaseWriteLock(lockStamp, statement);
        }
    }

    private StatementExecutionResult dropIndex(DropIndexStatement statement, Transaction transaction) throws StatementExecutionException {
        long lockStamp = acquireWriteLock(statement);
        try {
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
            LogEntry entry = LogEntryFactory.dropIndex(statement.getIndexName(), transaction);
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
            releaseWriteLock(lockStamp, statement);
        }
    }

    TableManager bootTable(Table table, long transaction, LogSequenceNumber dumpLogSequenceNumber) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.INFO, "bootTable {0} {1}.{2}", new Object[]{nodeId, tableSpaceName, table.name});
        AbstractTableManager prevTableManager = tables.remove(table.name);
        if (prevTableManager != null) {

            if (dumpLogSequenceNumber != null) {
                // restoring a table already booted in a previous life
                LOGGER.log(Level.INFO, "bootTable {0} {1}.{2} already exists on this tablespace. It will be truncated", new Object[]{nodeId, tableSpaceName, table.name});
                prevTableManager.dropTableData();
            } else {
                LOGGER.log(Level.INFO, "bootTable {0} {1}.{2} already exists on this tablespace", new Object[]{nodeId, tableSpaceName, table.name});
                throw new DataStorageManagerException("Table " + table.name + " already present in tableSpace " + tableSpaceName);
            }
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
        LOGGER.log(Level.INFO, "bootTable {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, table.name, (System.currentTimeMillis() - _start) + ""});
        dbmanager.getPlanner().clearCache();
        return tableManager;
    }

    private AbstractIndexManager bootIndex(Index index, AbstractTableManager tableManager, long transaction, boolean rebuild) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.INFO, "bootIndex {0} {1}.{2}.{3} uuid {4}", new Object[]{nodeId, tableSpaceName, index.table, index.name, index.uuid});
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
            Map<String, AbstractIndexManager> map = new HashMap<>(a);
            map.putAll(b);
            return map;
        });
        indexManager.start(tableManager.getBootSequenceNumber());
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.INFO, "bootIndex {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, index.name, (_stop - _start) + ""});
        if (rebuild) {
            indexManager.rebuild();
            LOGGER.log(Level.INFO, "bootIndex - rebuild {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, index.name, (System.currentTimeMillis() - _stop) + ""});
        }
        dbmanager.getPlanner().clearCache();
        return indexManager;
    }

    private AbstractTableManager alterTable(Table table, Transaction transaction) throws DDLException {
        LOGGER.log(Level.INFO, "alterTable {0} {1}.{2} uuid {3}", new Object[]{nodeId, tableSpaceName, table.name,
            table.uuid});
        AbstractTableManager tableManager = null;
        String oldTableName = null;
        for (AbstractTableManager tm : tables.values()) {
            if (tm.getTable().uuid.equals(table.uuid)) {
                tableManager = tm;
                oldTableName = tm.getTable().name;
            }
        }
        if (tableManager == null || oldTableName == null) {
            throw new TableDoesNotExistException("Cannot find table " + table.name + " with uuid " + table.uuid);
        }
        tableManager.tableAltered(table, transaction);
        if (!oldTableName.equalsIgnoreCase(table.name)) {
            tables.remove(oldTableName);
            tables.put(table.name, tableManager);
        }
        return tableManager;
    }

    public void close() throws LogNotAvailableException {
        boolean useJmx = dbmanager.getServerConfiguration().getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT);
        closed = true;
        if (!virtual) {
            long lockStamp = acquireWriteLock("closeTablespace");
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
                releaseWriteLock(lockStamp, "closeTablespace");
            }
        }
        if (useJmx) {
            JMXUtils.unregisterTableSpaceManagerStatsMXBean(tableSpaceName);
        }
    }

    private static final class TableSpaceCheckpoint {

        private final LogSequenceNumber sequenceNumber;
        private final Map<String, LogSequenceNumber> tablesCheckpoints;

        public TableSpaceCheckpoint(LogSequenceNumber sequenceNumber,
                Map<String, LogSequenceNumber> tablesCheckpoints) {
            super();
            this.sequenceNumber = sequenceNumber;
            this.tablesCheckpoints = tablesCheckpoints;
        }
    }

    TableSpaceCheckpoint checkpoint(boolean full, boolean pin, boolean alreadLocked) throws DataStorageManagerException, LogNotAvailableException {
        if (virtual) {
            return null;
        }
        long _start = System.currentTimeMillis();
        LogSequenceNumber logSequenceNumber;
        LogSequenceNumber _logSequenceNumber;
        List<PostCheckpointAction> actions = new ArrayList<>();
        Map<String, LogSequenceNumber> checkpointsTableNameSequenceNumber = new HashMap<>();

        long lockStamp = 0;
        if (!alreadLocked) {
            lockStamp = acquireWriteLock("checkpoint");
        }
        try {
            logSequenceNumber = log.getLastSequenceNumber();

            if (logSequenceNumber.isStartOfTime()) {
                LOGGER.log(Level.INFO, "{0} checkpoint {1} at {2}. skipped (no write ever issued to log)", new Object[]{nodeId, tableSpaceName, logSequenceNumber});
                return new TableSpaceCheckpoint(logSequenceNumber, checkpointsTableNameSequenceNumber);
            }
            LOGGER.log(Level.INFO, "{0} checkpoint start {1} at {2}", new Object[]{nodeId, tableSpaceName, logSequenceNumber});
            if (actualLogSequenceNumber == null) {
                throw new DataStorageManagerException("actualLogSequenceNumber cannot be null");
            }

            // TODO: transactions checkpoint is not atomic
            actions.addAll(dataStorageManager.writeTransactionsAtCheckpoint(tableSpaceUUID, logSequenceNumber, new ArrayList<>(transactions.values())));
            actions.addAll(writeTablesOnDataStorageManager(new CommitLogResult(logSequenceNumber, false, true)));

            // we checkpoint all data to disk and save the actual log sequence number
            for (AbstractTableManager tableManager : tables.values()) {
                // each TableManager will save its own checkpoint sequence number (on TableStatus) and upon recovery will replay only actions with log position after the actual table-local checkpoint
                // remember that the checkpoint for a table can last "minutes" and we do not want to stop the world

                if (!tableManager.isSystemTable()) {
                    TableCheckpoint checkpoint = full ? tableManager.fullCheckpoint(pin) : tableManager.checkpoint(pin);

                    if (checkpoint != null) {
                        LOGGER.log(Level.INFO, "checkpoint done for table {0}.{1} (pin: {2})", new Object[]{tableSpaceName, tableManager.getTable().name, pin});
                        actions.addAll(checkpoint.actions);
                        checkpointsTableNameSequenceNumber.put(checkpoint.tableName, checkpoint.sequenceNumber);
                        if (afterTableCheckPointAction != null) {
                            afterTableCheckPointAction.run();
                        }
                    }
                }
            }

            // we are sure that all data as been flushed. upon recovery we will replay the log starting from this position
            actions.addAll(dataStorageManager.writeCheckpointSequenceNumber(tableSpaceUUID, logSequenceNumber));

            /* Indexes checkpoint is handled by TableManagers */
            if (leader) {
                log.dropOldLedgers(logSequenceNumber);
            }

            _logSequenceNumber = log.getLastSequenceNumber();
        } finally {
            if (!alreadLocked) {
                releaseWriteLock(lockStamp, "checkpoint");
            }
        }

        for (PostCheckpointAction action : actions) {
            try {
                action.run();
            } catch (Exception error) {
                LOGGER.log(Level.SEVERE, "postcheckpoint error:" + error, error);
            }
        }
        long _stop = System.currentTimeMillis();

        LOGGER.log(Level.INFO, "{0} checkpoint finish {1} started ad {2}, finished at {3}, total time {4} ms",
                new Object[]{nodeId, tableSpaceName, logSequenceNumber, _logSequenceNumber, Long.toString(_stop - _start)});

        return new TableSpaceCheckpoint(logSequenceNumber, checkpointsTableNameSequenceNumber);
    }

    private CompletableFuture<StatementExecutionResult> beginTransactionAsync() throws StatementExecutionException {

        long id = newTransactionId.incrementAndGet();

        LogEntry entry = LogEntryFactory.beginTransaction(id);
        CommitLogResult pos;
        long lockStamp = acquireReadLock(new BeginTransactionStatement(tableSpaceName));
        pos = log.log(entry, false);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApply((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(id, TransactionResult.OutcomeType.BEGIN);
        });
        releaseReadLock(res, lockStamp, "begin transaction " + id);
        return res;

    }

    private CompletableFuture<StatementExecutionResult> rollbackTransaction(RollbackTransactionStatement statement) throws StatementExecutionException {
        long txId = statement.getTransactionId();
        LogEntry entry = LogEntryFactory.rollbackTransaction(txId);
        final long lockStamp = acquireReadLock(statement);
        Transaction tx = transactions.get(txId);
        if (tx == null) {
            throw new StatementExecutionException("no such transaction " + statement.getTransactionId());
        }
        CommitLogResult pos = log.log(entry, true);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(txId, TransactionResult.OutcomeType.ROLLBACK);
        });
        return releaseReadLock(res, lockStamp, statement);
    }

    private CompletableFuture<StatementExecutionResult> commitTransaction(CommitTransactionStatement statement) throws StatementExecutionException {
        long txId = statement.getTransactionId();
        LogEntry entry = LogEntryFactory.commitTransaction(txId);
        final long lockStamp = acquireReadLock(statement);
        CommitLogResult pos = log.log(entry, true);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApply((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(txId, TransactionResult.OutcomeType.COMMIT);
        });
        return releaseReadLock(res, lockStamp, statement);
    }

    private CompletableFuture<StatementExecutionResult> releaseReadLock(
            CompletableFuture<StatementExecutionResult> promise, long lockStamp, Object description) {
        return promise.whenComplete((r, error) -> {
            releaseReadLock(lockStamp, description);
        });
    }

    private void releaseReadLock(long lockStamp, Object description) {
        System.err.println("RELEASED TS READLOCK " + lockStamp + " for " + description);
        generalLock.unlockRead(lockStamp);
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
                apply(new CommitLogResult(t, false, true), u, true);
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

    @Override
    public String toString() {
        return "TableSpaceManager [nodeId=" + nodeId
                + ", tableSpaceName=" + tableSpaceName
                + ", tableSpaceUUID=" + tableSpaceUUID + "]";
    }
}
