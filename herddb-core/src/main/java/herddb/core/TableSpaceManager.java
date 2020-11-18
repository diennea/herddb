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

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import herddb.core.system.SysdualTableManager;
import herddb.core.system.SysforeignkeysTableManager;
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
import herddb.data.consistency.TableChecksum;
import herddb.data.consistency.TableDataChecksum;
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
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DDLException;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ForeignKeyDef;
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
import herddb.model.TableSpaceDoesNotExistException;
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
import herddb.network.ServerHostData;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.utils.Bytes;
import herddb.utils.Futures;
import herddb.utils.KeyValue;
import herddb.utils.SystemProperties;
import java.io.EOFException;
import java.io.IOException;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a TableSet in memory
 *
 * @author enrico.olivelli
 */
public class TableSpaceManager {
    private static final boolean ENABLE_PENDING_TRANSACTION_CHECK = SystemProperties.getBooleanSystemProperty("herddb.tablespace.checkpendingtransactions", true);

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSpaceManager.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    final StatsLogger tablespaceStasLogger;
    final OpStatsLogger checkpointTimeStats;

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
    private volatile FollowerThread followerThread;
    private final ExecutorService callbacksExecutor;
    private final boolean virtual;

    private volatile boolean recoveryInProgress;
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

    public TableSpaceManager(String nodeId, String tableSpaceName,
                             String tableSpaceUUID,
                             int expectedReplicaCount,
                             MetadataStorageManager metadataStorageManager,
                             DataStorageManager dataStorageManager,
                             CommitLog log, DBManager manager, boolean virtual) {
        this.nodeId = nodeId;
        this.dbmanager = manager;
        this.callbacksExecutor = dbmanager.getCallbacksExecutor();
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.log = log;
        this.tableSpaceName = tableSpaceName;
        this.tableSpaceUUID = tableSpaceUUID;
        this.virtual = virtual;
        this.tablespaceStasLogger = this.dbmanager.getStatsLogger().scope(this.tableSpaceName);
        this.checkpointTimeStats = this.tablespaceStasLogger.getOpStatsLogger("checkpointTime");
        this.dataStorageManager.tableSpaceMetadataUpdated(tableSpaceUUID, expectedReplicaCount);
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
            registerSystemTableManager(new SysdualTableManager(this));
            registerSystemTableManager(new SysforeignkeysTableManager(this));
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
            startAsLeader(1);
        } else {
            dataStorageManager.initTablespace(tableSpaceUUID);
            recover(tableSpaceInfo);

            LOGGER.info("after recovery of tableSpace {}, actualLogSequenceNumber:{}", tableSpaceName, actualLogSequenceNumber);

            tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
            if (tableSpaceInfo.leaderId.equals(nodeId)) {
                startAsLeader(tableSpaceInfo.expectedReplicaCount);
            } else {
                startAsFollower();
            }
        }
    }

    void recover(TableSpace tableSpaceInfo) throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {
        if (recoveryInProgress) {
            throw new HerdDBInternalException("Cannot run recovery twice");
        }
        recoveryInProgress = true;
        LogSequenceNumber logSequenceNumber = dataStorageManager.getLastcheckpointSequenceNumber(tableSpaceUUID);
        actualLogSequenceNumber = logSequenceNumber;
        LOGGER.info("{} recover {}, logSequenceNumber from DataStorage: {}", nodeId, tableSpaceName, logSequenceNumber);
        List<Table> tablesAtBoot = dataStorageManager.loadTables(logSequenceNumber, tableSpaceUUID);
        List<Index> indexesAtBoot = dataStorageManager.loadIndexes(logSequenceNumber, tableSpaceUUID);
        String tableNames = tablesAtBoot.stream().map(t -> {
            return t.name;
        }).collect(Collectors.joining(","));

        String indexNames = indexesAtBoot.stream().map(t -> {
            return t.name + " on table " + t.table;
        }).collect(Collectors.joining(","));

        if (!tableNames.isEmpty()) {
            LOGGER.info("{} {} tablesAtBoot: {}, indexesAtBoot: {}", nodeId, tableSpaceName, tableNames, indexNames);
        }

        for (Table table : tablesAtBoot) {
            TableManager tableManager = bootTable(table, 0, null, false);
            for (Index index : indexesAtBoot) {
                if (index.table.equals(table.name)) {
                    bootIndex(index, tableManager, false, 0, false, false);
                }
            }
        }
        dataStorageManager.loadTransactions(logSequenceNumber, tableSpaceUUID, t -> {
            transactions.put(t.transactionId, t);
            LOGGER.trace("{} {} tx {} at boot lsn {}", nodeId, tableSpaceName, t.transactionId, t.lastSequenceNumber);
            try {
                if (t.newTables != null) {
                    for (Table table : t.newTables.values()) {
                        if (!tables.containsKey(table.name)) {
                            bootTable(table, t.transactionId, null, false);
                        }
                    }
                }
                if (t.newIndexes != null) {
                    for (Index index : t.newIndexes.values()) {
                        if (!indexes.containsKey(index.name)) {
                            AbstractTableManager tableManager = tables.get(index.table);
                            bootIndex(index, tableManager, false, t.transactionId, false, false);
                        }
                    }
                }
            } catch (Exception err) {
                LOGGER.error("error while booting tmp tables " + err, err);
                throw new RuntimeException(err);
            }
        });

        if (LogSequenceNumber.START_OF_TIME.equals(logSequenceNumber)
                && dbmanager.getServerConfiguration().getBoolean(ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT, ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT_DEFAULT)) {
            LOGGER.error(nodeId + " full recovery of data is forced (" + ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT + "=true) for tableSpace " + tableSpaceName);
            downloadTableSpaceData();
            log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), false);
        } else {
            try {
                log.recovery(logSequenceNumber, new ApplyEntryOnRecovery(), false);
            } catch (FullRecoveryNeededException fullRecoveryNeeded) {
                LOGGER.error(nodeId + " full recovery of data is needed for tableSpace " + tableSpaceName, fullRecoveryNeeded);
                downloadTableSpaceData();
                log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), false);
            }
        }
        recoveryInProgress = false;
        if (!LogSequenceNumber.START_OF_TIME.equals(actualLogSequenceNumber)) {
            LOGGER.info("Recovery finished for {} seqNum {}", tableSpaceName, actualLogSequenceNumber);
            checkpoint(false, false, false);
        }

    }

    void recoverForLeadership() throws DataStorageManagerException, LogNotAvailableException {
        if (recoveryInProgress) {
            throw new HerdDBInternalException("Cannot run recovery twice");
        }
        recoveryInProgress = true;
        actualLogSequenceNumber = log.getLastSequenceNumber();
        LOGGER.info("recovering tablespace {} log from sequence number {}, with fencing", tableSpaceName, actualLogSequenceNumber);
        log.recovery(actualLogSequenceNumber, new ApplyEntryOnRecovery(), true);
        LOGGER.info("Recovery (with fencing) finished for {}", tableSpaceName);
        recoveryInProgress = false;
    }

    void apply(CommitLogResult position, LogEntry entry, boolean recovery) throws DataStorageManagerException, DDLException {
        if (!position.deferred || position.sync) {
            // this will wait for the write to be acknowledged by the log
            // it can throw LogNotAvailableException
            this.actualLogSequenceNumber = position.getLogSequenceNumber();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("apply {} {}", position.getLogSequenceNumber(), entry);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("apply {} {}", position, entry);
            }
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
                List<AbstractIndexManager> indexManagers = new ArrayList<>(indexes.values());
                for (AbstractIndexManager indexManager : indexManagers) {
                    if (indexManager.getCreatedInTransaction() == 0 || indexManager.getCreatedInTransaction() == id) {
                        indexManager.onTransactionRollback(transaction);
                    }
                }
                List<AbstractTableManager> managers = new ArrayList<>(tables.values());
                for (AbstractTableManager manager : managers) {
                    if (manager.getCreatedInTransaction() == 0 || manager.getCreatedInTransaction() == id) {
                        Table table = manager.getTable();
                        if (transaction.isNewTable(table.name)) {
                            LOGGER.info("rollback CREATE TABLE " + table.tablespace + "." + table.name);
                            disposeTable(manager);
                            Map<String, AbstractIndexManager> indexes = indexesByTable.remove(manager.getTable().name);
                            if (indexes != null) {
                                for (AbstractIndexManager indexManager : indexes.values()) {
                                    disposeIndexManager(indexManager);
                                }
                            }
                        } else {
                            manager.onTransactionRollback(transaction);
                        }
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
                LogSequenceNumber commit = position.getLogSequenceNumber();
                transaction.sync(commit);
                List<AbstractTableManager> managers = new ArrayList<>(tables.values());
                for (AbstractTableManager manager : managers) {
                    if (manager.getCreatedInTransaction() == 0 || manager.getCreatedInTransaction() == id) {
                        manager.onTransactionCommit(transaction, recovery);
                    }
                }
                List<AbstractIndexManager> indexManagers = new ArrayList<>(indexes.values());
                for (AbstractIndexManager indexManager : indexManagers) {
                    if (indexManager.getCreatedInTransaction() == 0 || indexManager.getCreatedInTransaction() == id) {
                        indexManager.onTransactionCommit(transaction, recovery);
                    }
                }
                if ((transaction.droppedTables != null && !transaction.droppedTables.isEmpty()) || (transaction.droppedIndexes != null && !transaction.droppedIndexes.isEmpty())) {

                    if (transaction.droppedTables != null) {
                        for (String dropped : transaction.droppedTables) {
                            for (AbstractTableManager manager : managers) {
                                if (manager.getTable().name.equals(dropped)) {
                                    disposeTable(manager);
                                }
                            }
                        }
                    }
                    if (transaction.droppedIndexes != null) {
                        for (String dropped : transaction.droppedIndexes) {
                            for (AbstractIndexManager manager : indexManagers) {
                                if (manager.getIndex().name.equals(dropped)) {
                                    disposeIndexManager(manager);
                                }
                            }
                        }
                    }

                }
                if ((transaction.newTables != null && !transaction.newTables.isEmpty())
                        || (transaction.droppedTables != null && !transaction.droppedTables.isEmpty())
                        || (transaction.newIndexes != null && !transaction.newIndexes.isEmpty())
                        || (transaction.droppedIndexes != null && !transaction.droppedIndexes.isEmpty())) {
                    writeTablesOnDataStorageManager(position, false);
                    dbmanager.getPlanner().clearCache();
                }
                transactions.remove(transaction.transactionId);
            }
            break;
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewTable(table, position);
                }

                bootTable(table, entry.transactionId, null, true);
                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                }
            }
            break;
            case LogEntryType.CREATE_INDEX: {
                Index index = Index.deserialize(entry.value.to_array());
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewIndex(index, position);
                }
                AbstractTableManager tableManager = tables.get(index.table);
                if (tableManager == null) {
                    throw new RuntimeException("table " + index.table + " does not exists");
                }
                bootIndex(index, tableManager, true, entry.transactionId, true, false);
                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
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
                        disposeTable(manager);
                        Map<String, AbstractIndexManager> indexes = indexesByTable.get(tableName);
                        if (indexes != null && !indexes.isEmpty()) {
                            LOGGER.error("It looks like we are dropping a table " + tableName + " with these indexes " + indexes);
                        }
                    }
                }

                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                }
            }
            break;
            case LogEntryType.DROP_INDEX: {
                String indexName = entry.value.to_string();
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerDropIndex(indexName, position);
                } else {
                    AbstractIndexManager manager = indexes.get(indexName);
                    if (manager != null) {
                        disposeIndexManager(manager);
                    }
                }

                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                    dbmanager.getPlanner().clearCache();
                }
            }
            break;
            case LogEntryType.ALTER_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                alterTable(table, null);
                writeTablesOnDataStorageManager(position, false);
            }
            break;
            case LogEntryType.TABLE_CONSISTENCY_CHECK: {
                /*
                    In recovery mode, we need to skip the consistency check.
                    The tablespace may not be avaible yet and therefore calcite will not able to performed the select query.
                */
                if (recovery) {
                   LOGGER.info("skip {} consistency check LogEntry {}", tableSpaceName, entry);
                   break;
                }
                try {
                    TableChecksum check = MAPPER.readValue(entry.value.to_array(), TableChecksum.class);
                    String tableSpace = check.getTableSpaceName();
                    String query = check.getQuery();
                    String tableName = entry.tableName;
                    //In the entry type = 14, the follower will have to run the query on the transaction log
                    if (!isLeader()) {
                        AbstractTableManager tablemanager = this.getTableManager(tableName);
                        DBManager manager = this.getDbmanager();

                        if (tablemanager == null || tablemanager.getCreatedInTransaction() > 0) {
                            throw new TableDoesNotExistException(String.format("Table %s does not exist.", tablemanager));
                        }
                        /*
                            scan = true
                            allowCache = false
                            returnValues = false
                            maxRows = -1
                        */
                        TranslatedQuery translated = manager.getPlanner().translate(tableSpace, query, Collections.emptyList(), true, false, false, -1);
                        TableChecksum scanResult = TableDataChecksum.createChecksum(manager, translated, this, tableSpace, tableName);
                        long followerDigest = scanResult.getDigest();
                        long leaderDigest = check.getDigest();
                        long leaderNumRecords = check.getNumRecords();
                        long followerNumRecords = scanResult.getNumRecords();
                        //the necessary condition to pass the check is to have exactly the same digest and the number of records processed
                        if (followerDigest == leaderDigest && leaderNumRecords == followerNumRecords) {
                            LOGGER.info("Data consistency check PASS for table {}  tablespace {} with  Checksum {}", tableName, tableSpace, followerDigest);
                        } else {
                            LOGGER.error("Data consistency check FAILED for table {} in tablespace {} with Checksum {}", tableName, tableSpace, followerDigest);
                        }
                    } else {
                        long digest = check.getDigest();
                        LOGGER.info("Created checksum {}  for table {} in tablespace {} on node {}", digest, entry.tableName, tableSpace, this.getDbmanager().getNodeId());
                    }
                } catch (IOException | DataScannerException ex) {
                    LOGGER.error("Error during table consistency check ", ex);
                }
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
                && entry.type != LogEntryType.DROP_TABLE
                && entry.type != LogEntryType.TABLE_CONSISTENCY_CHECK) {
            AbstractTableManager tableManager = tables.get(entry.tableName);
            tableManager.apply(position, entry, recovery);
        }

    }

    private void disposeTable(AbstractTableManager manager) throws DataStorageManagerException {
        manager.dropTableData();
        manager.close();
        tables.remove(manager.getTable().name);
    }

    private void disposeIndexManager(AbstractIndexManager indexManager) throws DataStorageManagerException {
        indexManager.dropIndexData();
        indexManager.close();
        indexes.remove(indexManager.getIndex().name);
        Map<String, AbstractIndexManager> indexesForTable =
                indexesByTable.get(indexManager.getIndex().table);
        if (indexesForTable != null) {
            indexesForTable.remove(indexManager.getIndex().name);
        }
    }

    private Collection<PostCheckpointAction> writeTablesOnDataStorageManager(CommitLogResult writeLog, boolean prepareActions) throws DataStorageManagerException,
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
        return dataStorageManager.writeTables(tableSpaceUUID, logSequenceNumber, tablelist, indexlist, prepareActions);
    }

    public DataScanner scan(
            ScanStatement statement, StatementEvaluationContext context,
            TransactionContext transactionContext, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        boolean rollbackOnError = false;
        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID
                && (lockRequired || forWrite || context.isForceAcquireWriteLock() || context.isForceRetainReadLock())) {
            try {
                // sync on beginTransaction
                StatementExecutionResult newTransaction = Futures.result(beginTransactionAsync(context, true));
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
        if (transactionContext.transactionId > 0 && transaction == null) {
            throw new StatementExecutionException("transaction " + transactionContext.transactionId + " does not exist on tablespace " + tableSpaceName);
        }
        if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
            throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
        }
        if (transaction != null) {
            transaction.touch();
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
                LOGGER.debug(tableSpaceName + " forcing rollback of implicit tx " + transactionContext.transactionId, error);
                try {
                    rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, transactionContext.transactionId), context).get();
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

        // ensure we do not have any data on disk and in memory

        actualLogSequenceNumber = LogSequenceNumber.START_OF_TIME;
        newTransactionId.set(0);
        LOGGER.info("tablespace " + tableSpaceName + " at downloadTableSpaceData " + tables + ", " + indexes + ", " + transactions);
        for (AbstractTableManager manager : tables.values()) {
            // this is like a truncate table, and it releases all pages
            // and all indexes
            if (!manager.isSystemTable()) {
                manager.dropTableData();
            }
            manager.close();
        }
        tables.clear();

        // this map should be empty
        for (AbstractIndexManager manager : indexes.values()) {
            manager.dropIndexData();
            manager.close();
        }
        indexes.clear();
        transactions.clear();

        dataStorageManager.eraseTablespaceData(tableSpaceUUID);

        NodeMetadata nodeData = leaderAddress.get();
        ClientConfiguration clientConfiguration = new ClientConfiguration(dbmanager.getTmpDirectory());
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, dbmanager.getServerToServerUsername());
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, dbmanager.getServerToServerPassword());
        // always use network, we want to run tests with this case
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_CONNECT_LOCALVM_SERVER, false);
        try (HDBClient client = new HDBClient(clientConfiguration)) {
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
                receiver.getLatch().get(1, TimeUnit.HOURS);
                this.actualLogSequenceNumber = receiver.logSequenceNumber;
                LOGGER.info(tableSpaceName + " After download local actualLogSequenceNumber is " + actualLogSequenceNumber);

            } catch (ClientSideMetadataProviderException | HDBException | InterruptedException | ExecutionException | TimeoutException internalError) {
                LOGGER.error(tableSpaceName + " error downloading snapshot", internalError);
                throw new DataStorageManagerException(internalError);
            }

        }

    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    public List<Table> getAllVisibleTables(Transaction t) {
        return tables
                .values()
                .stream()
                .filter(s -> s.getCreatedInTransaction() == 0 || (t != null && t.transactionId == s.getCreatedInTransaction()))
                .map(AbstractTableManager::getTable)
                .collect(Collectors.toList());
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

    private void releaseWriteLock(long lockStamp, Object description) {
        generalLock.unlockWrite(lockStamp);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} ts {} relwlock {}", tableSpaceName, description, lockStamp);
        }
//        LOGGER.error("RELEASE TS WRITELOCK for " + description + " -> " + lockStamp + " " + generalLock);
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

    void metadataUpdated(TableSpace tableSpace) {
        if (!tableSpace.uuid.equals(this.tableSpaceUUID)) {
            throw new IllegalArgumentException();
        }
        log.metadataUpdated(tableSpace.expectedReplicaCount);
        dataStorageManager.tableSpaceMetadataUpdated(tableSpace.uuid, tableSpace.expectedReplicaCount);
    }

    AbstractTableManager getTableManagerByUUID(String uuid) {
        //TODO: make this method more efficient
        for (AbstractTableManager manager : tables.values()) {
            if (manager.getTable().uuid.equals(uuid)) {
                return manager;
            }
        }
        throw new HerdDBInternalException("Cannot find tablemanager for " + uuid);
    }

    public Table[] collectChildrenTables(Table parentTable) {
        List<Table> list = new ArrayList<>();
        for (AbstractTableManager manager : tables.values()) {
            Table table = manager.getTable();
            if (table.isChildOfTable(parentTable.uuid)) {
                list.add(table);
            }
        }
        // selft reference
        if (parentTable.isChildOfTable(parentTable.uuid)) {
            list.add(parentTable);
        }
        return list.isEmpty() ? null : list.toArray(new Table[0]);
    }

    void rebuildForeignKeyReferences(Table table) {
        for (AbstractTableManager manager : tables.values()) {
            manager.rebuildForeignKeyReferences(table);
        }
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
            return Objects.equals(this.tableName, other.tableName);
        }

    }

    void processAbandonedTransactions() {
        if (!leader) {
            return;
        }
        long now = System.currentTimeMillis();
        long timeout = dbmanager.getAbandonedTransactionsTimeout();
        if (timeout <= 0) {
            return;
        }
        long abandonedTransactionTimeout = now - timeout;
        for (Transaction t : transactions.values()) {
            if (t.isAbandoned(abandonedTransactionTimeout)) {
                LOGGER.error("forcing rollback of abandoned transaction {},"
                                + " created locally at {},"
                                + " last activity locally at {}",
                        t.transactionId,
                                new java.sql.Timestamp(t.localCreationTimestamp),
                                new java.sql.Timestamp(t.lastActivityTs));
                try {
                    if (!validateTransactionBeforeTxCommand(t.transactionId, false /* no wait */)) {
                        // Continue to check next transaction
                        continue;
                    }
                } catch (StatementExecutionException e) {
                    LOGGER.error("Failed to validate transaction {}: {}",
                            new Object[] { t.transactionId, e.getMessage() });
                    // Continue to check next transaction
                    continue;
                } catch (RuntimeException e) {
                    LOGGER.error("Failed to validate transaction {}", new Object[] { t.transactionId, e });
                    // Continue to check next transaction
                    continue;
                }
                long lockStamp = acquireReadLock("forceRollback" + t.transactionId);
                try {
                    forceTransactionRollback(t.transactionId);
                } finally {
                    releaseReadLock(lockStamp, "forceRollback" + t.transactionId);
                }
            }
        }
    }

    void runLocalTableCheckPoints() {
        Set<String> tablesToDo = new HashSet<>(tablesNeedingCheckPoint);
        tablesNeedingCheckPoint.clear();
        for (String table : tablesToDo) {
            LOGGER.info("Forcing local checkpoint table " + this.tableSpaceName + "." + table);
            AbstractTableManager tableManager = tables.get(table);
            if (tableManager != null) {
                try {
                    tableManager.checkpoint(false);
                } catch (DataStorageManagerException ex) {
                    LOGGER.error("Bad error on table checkpoint", ex);
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
            bootTable(table, 0, dumpLogSequenceNumber, true);
        } finally {
            releaseWriteLock(lockStamp, "beginRestoreTable " + table.name);
        }
    }

    public void restoreTableFinished(String table, List<Index> indexes) {
        TableManager tableManager = (TableManager) tables.get(table);
        tableManager.restoreFinished();

        for (Index index : indexes) {
            bootIndex(index, tableManager, false, 0, true, true);
        }
    }

    public void restoreRawDumpedTransactions(List<Transaction> entries) {
        for (Transaction ld : entries) {
            LOGGER.info("restore transaction " + ld);
            transactions.put(ld.transactionId, ld);
        }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE")
    void dumpTableSpace(String dumpId, Channel channel, int fetchSize, boolean includeLog) throws DataStorageManagerException, LogNotAvailableException {

        LOGGER.info("dumpTableSpace dumpId:{} channel {} fetchSize:{}, includeLog:{}", dumpId, channel, fetchSize, includeLog);

        TableSpaceCheckpoint checkpoint;

        List<DumpedLogEntry> txlogentries = new CopyOnWriteArrayList<>();
        CommitLogListener logDumpReceiver = new CommitLogListener() {
            @Override
            public void logEntry(LogSequenceNumber logPos, LogEntry data) {
                // we are going to capture all the changes to the tablespace during the dump, in order to replay
                // eventually 'missed' changes during the dump
                txlogentries.add(new DumpedLogEntry(logPos, data.serialize()));
                //LOGGER.error("dumping entry " + logPos + ", " + data + " nentries: " + txlogentries.size());
            }
        };

        long lockStamp = acquireWriteLock(null);

        if (includeLog) {
            log.attachCommitLogListener(logDumpReceiver);
        }

        checkpoint = checkpoint(true /* compact records*/, true, true /* already locked */);
        LOGGER.info("Created checkpoint at {}", checkpoint);
        if (checkpoint == null) {
            throw new DataStorageManagerException("failed to create a checkpoint, check logs for the reason");
        }

        /* Downgrade lock */
//        System.err.println("DOWNGRADING LOCK " + lockStamp + " TO READ");
        lockStamp = generalLock.tryConvertToReadLock(lockStamp);
        if (lockStamp == 0) {
            throw new DataStorageManagerException("unable to downgrade lock");
        }
        try {
            final int timeout = 60000;
            LogSequenceNumber checkpointSequenceNumber = checkpoint.sequenceNumber;

            long id = channel.generateRequestId();
            LOGGER.info("start sending dump, dumpId: {} to client {}", dumpId, channel);
            try (Pdu response_to_start = channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                    id, tableSpaceName, dumpId, "start", null, stats.getTablesize(), checkpointSequenceNumber.ledgerId, checkpointSequenceNumber.offset, null, null), timeout)) {
                if (response_to_start.type != Pdu.TYPE_ACK) {
                    LOGGER.error("error response at start command");
                    return;
                }
            }

            if (includeLog) {
                List<Transaction> transactionsSnapshot = new ArrayList<>();
                dataStorageManager.loadTransactions(checkpointSequenceNumber, tableSpaceUUID, transactionsSnapshot::add);
                List<Transaction> batch = new ArrayList<>();
                for (Transaction t : transactionsSnapshot) {
                    batch.add(t);
                    if (batch.size() == 10) {
                        sendTransactionsDump(batch, channel, dumpId, timeout);
                    }
                }
                sendTransactionsDump(batch, channel, dumpId, timeout);
            }

            for (Entry<String, LogSequenceNumber> entry : checkpoint.tablesCheckpoints.entrySet()) {
                final AbstractTableManager tableManager = tables.get(entry.getKey());
                final LogSequenceNumber sequenceNumber = entry.getValue();
                if (tableManager.isSystemTable()) {
                    continue;
                }
                try {
                    LOGGER.info("Sending table checkpoint for {} took at sequence number {}", tableManager.getTable().name, sequenceNumber);
                    FullTableScanConsumer sink = new SingleTableDumper(tableSpaceName, tableManager, channel, dumpId, timeout, fetchSize);
                    tableManager.dump(sequenceNumber, sink);
                } catch (DataStorageManagerException err) {
                    LOGGER.error("error sending dump id " + dumpId, err);
                    long errorid = channel.generateRequestId();
                    try (Pdu response = channel.sendMessageWithPduReply(errorid, PduCodec.TablespaceDumpData.write(
                            id, tableSpaceName, dumpId, "error", null, 0,
                            0, 0,
                            null, null),
                            timeout)) {
                    }
                    return;
                }
            }

            if (!txlogentries.isEmpty()) {
                txlogentries.sort(Comparator.naturalOrder());
                sendDumpedCommitLog(txlogentries, channel, dumpId, timeout);
            }

            LogSequenceNumber finishLogSequenceNumber = log.getLastSequenceNumber();
            channel.sendOneWayMessage(PduCodec.TablespaceDumpData.write(
                    id, tableSpaceName, dumpId, "finish", null, 0,
                    finishLogSequenceNumber.ledgerId, finishLogSequenceNumber.offset,
                    null, null), (Throwable error) -> {
                        if (error != null) {
                            LOGGER.error("Cannot send last dump msg for " + dumpId, error);
                        } else {
                            LOGGER.info("Sent last dump msg for " + dumpId);
                        }
            });
        } catch (InterruptedException | TimeoutException error) {
            LOGGER.error("error sending dump id " + dumpId, error);
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
                LOGGER.info("unPinTableCheckpoint {}.{} ({}) {}", tableSpaceUUID, tableName, tableUUID, seqNumber);
                dataStorageManager.unPinTableCheckpoint(tableSpaceUUID, tableUUID, seqNumber);
            }
        }

    }

    private void sendTransactionsDump(List<Transaction> batch, Channel channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        if (batch.isEmpty()) {
            return;
        }
        List<KeyValue> encodedTransactions = batch
                .stream()
                .map(tr -> {
                    return new KeyValue(Bytes.from_long(tr.transactionId), Bytes.from_array(tr.serialize()));
                })
                .collect(Collectors.toList());
        long id = channel.generateRequestId();
        try (Pdu response_to_transactionsData = channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                id, tableSpaceName, dumpId, "transactions", null, 0,
                0, 0,
                null, encodedTransactions), timeout)) {
            if (response_to_transactionsData.type != Pdu.TYPE_ACK) {
                LOGGER.error("error response at transactionsData command");
            }
        }
        batch.clear();
    }

    private void sendDumpedCommitLog(List<DumpedLogEntry> txlogentries, Channel channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        List<KeyValue> batch = new ArrayList<>();
        for (DumpedLogEntry e : txlogentries) {
            batch.add(new KeyValue(Bytes.from_array(e.logSequenceNumber.serialize()),
                    Bytes.from_array(e.entryData)));
        }
        long id = channel.generateRequestId();
        try (Pdu response_to_txlog = channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                id, tableSpaceName, dumpId, "txlog", null, 0,
                0, 0,
                null, batch), timeout)) {

            if (response_to_txlog.type != Pdu.TYPE_ACK) {
                LOGGER.error("error response at txlog command");
            }
        }

    }

    public void restoreFinished() throws DataStorageManagerException {
        LOGGER.info("restore finished of tableSpace " + tableSpaceName + ". requesting checkpoint");
        transactions.clear();
        checkpoint(false, false, false);
    }

    public boolean isVirtual() {
        return virtual;
    }

    private class FollowerThread implements Runnable {

        private volatile CountDownLatch running = new CountDownLatch(1);

        @Override
        public String toString() {
            return "FollowerThread{" + tableSpaceName + '}';
        }

        @Override
        public void run() {
            try (CommitLog.FollowerContext context = log.startFollowing(actualLogSequenceNumber)) {
                while (!isLeader() && !closed) {
                    long readLock = acquireReadLock("follow");
                    try {
                        log.followTheLeader(actualLogSequenceNumber, (LogSequenceNumber num, LogEntry u) -> {
                            try {
                                apply(new CommitLogResult(num, false, true), u, false);
                            } catch (Throwable t) {
                                throw new RuntimeException(t);
                            }
                            return !isLeader() && !closed;
                        }, context);
                    } finally {
                        releaseReadLock(readLock, "follow");
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("follower error " + tableSpaceName, t);
                setFailed();
            } finally {
                running.countDown();
            }
        }

        void waitForStop() throws InterruptedException {
            LOGGER.info("Waiting for FollowerThread of {} to stop", tableSpaceName);
            running.await();
            LOGGER.info("FollowerThread of {} stopped", tableSpaceName);
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

    private void startAsFollower() throws DataStorageManagerException, LogNotAvailableException {
        if (dbmanager.getMode().equals(ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER)) {
            // in diskless cluster mode there is no need to really 'follow' the leader
        } else {
            followerThread = new FollowerThread();
            dbmanager.submit(followerThread);
        }
    }

    private void startAsLeader(int expectedReplicaCount) throws DataStorageManagerException, DDLException, LogNotAvailableException {
        if (virtual) {

        } else {

            LOGGER.info("startAsLeader {} tablespace {}", nodeId, tableSpaceName);
            recoverForLeadership();

            // every pending transaction MUST be rollback back
            List<Long> pending_transactions = new ArrayList<>(this.transactions.keySet());
            log.startWriting(expectedReplicaCount);
            LOGGER.info("startAsLeader {} tablespace {} log, there were {} pending transactions to be rolledback", nodeId, tableSpaceName, pending_transactions.size());
            for (long tx : pending_transactions) {
                forceTransactionRollback(tx);
            }
        }
        leader = true;
    }

    private void forceTransactionRollback(long tx) throws LogNotAvailableException, DataStorageManagerException, DDLException {
        LOGGER.trace("rolling back transaction {}", tx);
        LogEntry rollback = LogEntryFactory.rollbackTransaction(tx);
        // let followers see the rollback on the log
        CommitLogResult pos = log.log(rollback, true);
        apply(pos, rollback, false);
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

    public CompletableFuture<StatementExecutionResult> executeStatementAsync(
            Statement statement, StatementEvaluationContext context,
            TransactionContext transactionContext
    ) throws StatementExecutionException {

        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID
                && statement.supportsTransactionAutoCreate() // Do not autostart transaction on alter table statements
        ) {
            AtomicLong capturedTx = new AtomicLong();
            boolean wasHoldingTableSpaceLock = context.getTableSpaceLock() != 0;
            CompletableFuture<StatementExecutionResult> newTransaction = beginTransactionAsync(context, false);
            CompletableFuture<StatementExecutionResult> finalResult = newTransaction
                    .thenCompose((StatementExecutionResult begineTransactionResult) -> {
                        TransactionContext newtransactionContext = new TransactionContext(begineTransactionResult.transactionId);
                        capturedTx.set(newtransactionContext.transactionId);
                        return executeStatementAsyncInternal(statement, context, newtransactionContext, true);
                    });
            finalResult.whenComplete((xx, error) -> {
                if (!wasHoldingTableSpaceLock) {
                    releaseReadLock(context.getTableSpaceLock(), "begin implicit transaction");
                }
                long txId = capturedTx.get();
                if (error != null && txId > 0) {
                    LOGGER.debug(tableSpaceName + " force rollback of implicit transaction " + txId, error);
                    try {
                        rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, txId), context)
                                .get(); // block until rollback is complete
                    } catch (InterruptedException ex) {
                        LOGGER.error(tableSpaceName + " Cannot rollback implicit tx " + txId, ex);
                        Thread.currentThread().interrupt();
                        error.addSuppressed(ex);
                    } catch (ExecutionException ex) {
                        LOGGER.error(tableSpaceName + " Cannot rollback implicit tx " + txId, ex.getCause());
                        error.addSuppressed(ex.getCause());
                    } catch (Throwable t) {
                        LOGGER.error(tableSpaceName + " Cannot rollback  implicittx " + txId, t);
                        error.addSuppressed(t);
                    }
                }
            });
            return finalResult;
        } else {
            return executeStatementAsyncInternal(statement, context, transactionContext, false);
        }
    }

    private CompletableFuture<StatementExecutionResult> executeStatementAsyncInternal(
            Statement statement, StatementEvaluationContext context,
            TransactionContext transactionContext, boolean rollbackOnError
    ) throws StatementExecutionException {
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null
                && !transaction.tableSpace.equals(tableSpaceName)) {
            return Futures.exception(
                    new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName));
        }
        if (transactionContext.transactionId > 0
                && transaction == null) {
            return Futures.exception(
                    new StatementExecutionException("transaction " + transactionContext.transactionId + " not found on tablespace " + tableSpaceName));
        }
        boolean isTransactionCommand = statement instanceof CommitTransactionStatement
                || statement instanceof RollbackTransactionStatement
                || statement instanceof AlterTableStatement; // AlterTable implictly commits the transaction
        if (transaction != null) {
            transaction.touch();
            if (!isTransactionCommand) {
                transaction.increaseRefcount();
            }
        }
        CompletableFuture<StatementExecutionResult> res;
        try {
            if (statement instanceof TableAwareStatement) {
                res = executeTableAwareStatement(statement, transaction, context);
            } else if (statement instanceof SQLPlannedOperationStatement) {
                res = executePlannedOperationStatement(statement, transactionContext, context);
            } else if (statement instanceof BeginTransactionStatement) {
                if (transaction != null) {
                    res = Futures.exception(new StatementExecutionException("transaction already started"));
                } else {
                    res = beginTransactionAsync(context, true);
                }
            } else if (statement instanceof CommitTransactionStatement) {
                res = commitTransaction((CommitTransactionStatement) statement, context);
            } else if (statement instanceof RollbackTransactionStatement) {
                res = rollbackTransaction((RollbackTransactionStatement) statement, context);
            } else if (statement instanceof CreateTableStatement) {
                res = CompletableFuture.completedFuture(createTable((CreateTableStatement) statement, transaction, context));
            } else if (statement instanceof CreateIndexStatement) {
                res = CompletableFuture.completedFuture(createIndex((CreateIndexStatement) statement, transaction, context));
            } else if (statement instanceof DropTableStatement) {
                res = CompletableFuture.completedFuture(dropTable((DropTableStatement) statement, transaction, context));
            } else if (statement instanceof DropIndexStatement) {
                res = CompletableFuture.completedFuture(dropIndex((DropIndexStatement) statement, transaction, context));
            } else if (statement instanceof AlterTableStatement) {
                res = CompletableFuture.completedFuture(alterTable((AlterTableStatement) statement, transactionContext, context));
            } else {
                res = Futures.exception(new StatementExecutionException("unsupported statement " + statement)
                        .fillInStackTrace());
            }
        } catch (StatementExecutionException error) {
            res = Futures.exception(error);
        }
        if (transaction != null && !isTransactionCommand) {
            res = res.whenComplete((a, b) -> {
                transaction.decreaseRefCount();
            });
        }
        if (rollbackOnError) {
            long txId = transactionContext.transactionId;
            if (txId > 0) {
                res = res.whenComplete((xx, error) -> {
                    if (error != null) {
                        LOGGER.debug(tableSpaceName + " force rollback of implicit transaction " + txId, error);
                        try {
                            rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, txId), context)
                                    .get(); // block until operation completes
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

    private CompletableFuture<StatementExecutionResult> executePlannedOperationStatement(
            Statement statement,
            TransactionContext transactionContext, StatementEvaluationContext context
    ) {
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        SQLPlannedOperationStatement planned = (SQLPlannedOperationStatement) statement;
        CompletableFuture<StatementExecutionResult> res;
        try {
            res = planned.getRootOp().executeAsync(this, transactionContext, context, false, false);
        } catch (HerdDBInternalException err) {
            // ensure we are able to release locks correctly
            LOGGER.error("Internal error", err);
            res = Futures.exception(err);
        }
//        res.whenComplete((ee, err) -> {
//            LOGGER.error("COMPLETED " + statement + ": " + ee, err);
//        });
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> executeTableAwareStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
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
            res = Futures.exception(new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName));
        } else if (manager.getCreatedInTransaction() > 0
                && (transaction == null || transaction.transactionId != manager.getCreatedInTransaction())) {
            res = Futures.exception(new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName + ". created temporary in transaction " + manager.getCreatedInTransaction()));
        } else {
            res = manager.executeStatementAsync(statement, transaction, context);
        }
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .whenComplete((s, err) -> {
                        context.setTableSpaceLock(0);
                    });
        }
        return res;

    }

    private long acquireReadLock(Object statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} rlock {}", tableSpaceName, statement);
        }
        long lockStamp = generalLock.readLock();
//        LOGGER.error("ACQUIRED READLOCK for " + statement + ", " + generalLock);
        return lockStamp;
    }

    private long acquireWriteLock(Object statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} wlock {}", tableSpaceName, statement);
        }
//        LOGGER.error("ACQUIRINGTS WRITELOCK for " + statement + ", " + generalLock);

        long lockStamp = generalLock.writeLock();
//        LOGGER.error("ACQUIRED WRITELOCK for " + statement + " -> " + lockStamp + ", " + generalLock);
        return lockStamp;
    }

    private StatementExecutionResult alterTable(AlterTableStatement statement, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            if (transactionContext.transactionId > 0) {
                Transaction transaction = transactions.get(transactionContext.transactionId);
                if (transactionContext.transactionId > 0 && transaction == null) {
                    throw new StatementExecutionException("transaction " + transactionContext.transactionId + " does not exist on tablespace " + tableSpaceName);
                }
                if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
                    throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
                }
                LOGGER.info("Implicitly committing transaction " + transactionContext.transactionId + " due to an ALTER TABLE statement in tablespace " + tableSpaceName);
                try {
                    commitTransaction(new CommitTransactionStatement(tableSpaceName, transactionContext.transactionId), context).join();
                } catch (CompletionException err) {
                    throw new StatementExecutionException(err);
                }
                transactionContext = TransactionContext.NO_TRANSACTION;
            }
            AbstractTableManager tableManager = tables.get(statement.getTable());
            if (tableManager == null) {
                throw new TableDoesNotExistException("no table " + statement.getTable() + " in tablespace " + tableSpaceName + ","
                        + " only " + tables.keySet());
            }

            Table oldTable = tableManager.getTable();
            Table[] childrenTables = collectChildrenTables(oldTable);
            if (childrenTables != null) {
                for (Table child : childrenTables) {
                    for (String col : statement.getDropColumns()) {
                        for (ForeignKeyDef fk : child.foreignKeys) {
                            if (fk.parentTableId.equals(oldTable.uuid)) {
                                if (Stream
                                        .of(fk.parentTableColumns)
                                        .anyMatch(c -> c.equalsIgnoreCase(col))) {
                                    throw new StatementExecutionException(
                                            "Cannot drop column " + oldTable.name + "." + col + " because of foreign key constraint " + fk.name + " on table " + child.name);
                                }
                            }
                        }
                    }
                }
            }


            Table newTable;
            try {
                newTable = tableManager.getTable().applyAlterTable(statement);
            } catch (IllegalArgumentException error) {
                throw new StatementExecutionException(error);
            }
            validateAlterTable(newTable, context);
            LogEntry entry = LogEntryFactory.alterTable(newTable, null);
            try {
                CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                apply(pos, entry, false);
            } catch (Exception err) {
                throw new StatementExecutionException(err);
            }
            // Here transactionId is always 0, because transaction is implicitly committed
            return new DDLStatementExecutionResult(transactionContext.transactionId);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }

    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            if (tables.containsKey(statement.getTableDefinition().name)) {
                if (statement.isIfExistsClause()) {
                    return new DDLStatementExecutionResult(
                            transaction != null ? transaction.transactionId : 0);
                }
                throw new TableAlreadyExistsException(statement.getTableDefinition().name);
            }
            for (Index additionalIndex : statement.getAdditionalIndexes()) {
                AbstractIndexManager exists = indexes.get(additionalIndex.name);
                if (exists != null) {
                    LOGGER.info("Error while creating index " + additionalIndex.name + ", there is already an index " + exists.getIndex().name + " on table " + exists.getIndex().table);
                    throw new IndexAlreadyExistsException(additionalIndex.name);
                }
            }
            Table table = statement.getTableDefinition();
            // validate foreign keys
            if (table.foreignKeys != null) {
                for (ForeignKeyDef def: table.foreignKeys) {
                    AbstractTableManager parentTableManager = null;
                    for (AbstractTableManager ab : tables.values()) {
                        if (ab.getTable().uuid.equals(def.parentTableId)) {
                            parentTableManager = ab;
                            break;
                        }
                    }
                    if (parentTableManager == null) {
                        throw new StatementExecutionException("Table " + def.parentTableId + " does not exist in tablespace " + tableSpaceName);
                    }
                    Table parentTable = parentTableManager.getTable();
                    int i = 0;
                    for (String col : def.columns) {
                        Column column = table.getColumn(col);
                        Column parentColumn = parentTable.getColumn(def.parentTableColumns[i]);
                        if (column == null) {
                            throw new StatementExecutionException("Cannot find column " + col);
                        }
                        if (parentColumn == null) {
                            throw new StatementExecutionException("Cannot find column " + def.parentTableColumns[i]);
                        }
                        if (!ColumnTypes.sameRawDataType(column.type, parentColumn.type)) {
                            throw new StatementExecutionException("Column " + table.name + "." + column.name + " is not the same tyepe of column " + parentTable.name + "." + parentColumn.name);
                        }
                        i++;
                    }
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
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    private StatementExecutionResult createIndex(CreateIndexStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            AbstractIndexManager exists = indexes.get(statement.getIndexDefinition().name);
            if (exists != null) {
                LOGGER.info("Error while creating index " + statement.getIndexDefinition().name
                        + ", there is already an index " + exists.getIndex().name + " on table " + exists.getIndex().table);
                throw new IndexAlreadyExistsException(statement.getIndexDefinition().name);
            }
            LogEntry entry = LogEntryFactory.createIndex(statement.getIndexDefinition(), transaction);
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
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    private StatementExecutionResult dropTable(DropTableStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            String tableNameUpperCase = statement.getTable().toUpperCase();
            String tableNameNormalized = tables.keySet()
                    .stream()
                    .filter(t -> t.toUpperCase().equals(tableNameUpperCase))
                    .findFirst()
                    .orElse(statement.getTable());
            AbstractTableManager tableManager = tables.get(tableNameNormalized);
            if (tableManager == null) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction != null ? transaction.transactionId : 0);
                }
                throw new TableDoesNotExistException("table does not exist " + tableNameNormalized + " on tableSpace " + statement.getTableSpace());
            }
            if (transaction != null && transaction.isTableDropped(tableNameNormalized)) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction.transactionId);
                }
                throw new TableDoesNotExistException("table does not exist " + tableNameNormalized + " on tableSpace " + statement.getTableSpace());
            }
            Table table = tableManager.getTable();
            Table[] childrenTables = collectChildrenTables(table);
            if (childrenTables != null) {
                String errorMsg = "Cannot drop table " + table.tablespace + "." + table.name
                        + " because it has children tables: "
                        + Stream.of(childrenTables).map(t -> t.name).collect(Collectors.joining(","));
                throw new StatementExecutionException(errorMsg);
            }
            Map<String, AbstractIndexManager> indexesOnTable = indexesByTable.get(tableNameNormalized);
            if (indexesOnTable != null) {
                for (String index : new ArrayList<>(indexesOnTable.keySet())) {
                    LogEntry entry = LogEntryFactory.dropIndex(index, transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    apply(pos, entry, false);
                }
            }

            LogEntry entry = LogEntryFactory.dropTable(tableNameNormalized, transaction);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    private StatementExecutionResult dropIndex(DropIndexStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
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
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    TableManager bootTable(Table table, long transaction, LogSequenceNumber dumpLogSequenceNumber, boolean freshNew) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        if (!freshNew) {
            LOGGER.info("bootTable {} {}.{}", nodeId, tableSpaceName, table.name);
        }
        AbstractTableManager prevTableManager = tables.remove(table.name);
        if (prevTableManager != null) {
            if (dumpLogSequenceNumber != null) {
                // restoring a table already booted in a previous life
                LOGGER.info("bootTable {} {}.{} already exists on this tablespace. It will be truncated", nodeId, tableSpaceName, table.name);
                prevTableManager.dropTableData();
            } else {
                LOGGER.info("bootTable {} {}.{} already exists on this tablespace", nodeId, tableSpaceName, table.name);
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
        tableManager.start(freshNew);
        if (!freshNew) {
            LOGGER.info("bootTable {} {}.{} time {} ms", nodeId, tableSpaceName, table.name, (System.currentTimeMillis() - _start) + "");
        }
        dbmanager.getPlanner().clearCache();
        return tableManager;
    }

    AbstractIndexManager bootIndex(Index index, AbstractTableManager tableManager, boolean created, long transaction, boolean rebuild, boolean restore) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        if (!created) {
            LOGGER.info("bootIndex {} {}.{}.{} uuid {} - {}",
                new Object[] { nodeId, tableSpaceName, index.table, index.name, index.uuid, index.type });
        }
        AbstractIndexManager prevIndexManager = indexes.remove(index.name);
        if (prevIndexManager != null) {
            if (restore) {
                // restoring an index already booted in a previous life
                LOGGER.info(
                        "bootIndex {} {}.{}.{} uuid {} - {} already exists on this tablespace. It will be truncated",
                        new Object[] { nodeId, tableSpaceName, index.table, index.name, index.uuid, index.type });
                prevIndexManager.dropIndexData();
            } else {
                LOGGER.info("bootIndex {} {}.{}.{} uuid {} - {}",
                        new Object[] { nodeId, tableSpaceName, index.table, index.name, index.uuid, index.type });
                if (indexes.containsKey(index.name)) {
                    throw new DataStorageManagerException(
                            "Index" + index.name + " already present in tableSpace " + tableSpaceName);
                }
            }
        }
        final int writeLockTimeout = dbmanager.getServerConfiguration().getInt(
                ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT,
                ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT_DEFAULT
        );
        final int readLockTimeout = dbmanager.getServerConfiguration().getInt(
                ServerConfiguration.PROPERTY_READLOCK_TIMEOUT,
                ServerConfiguration.PROPERTY_READLOCK_TIMEOUT_DEFAULT
        );
        AbstractIndexManager indexManager;
        switch (index.type) {
            case Index.TYPE_HASH:
                indexManager = new MemoryHashIndexManager(index, tableManager, log, dataStorageManager, this, tableSpaceUUID, transaction,
                        writeLockTimeout, readLockTimeout);
                break;
            case Index.TYPE_BRIN:
                indexManager = new BRINIndexManager(index, dbmanager.getMemoryManager(), tableManager, log, dataStorageManager, this, tableSpaceUUID, transaction,
                        writeLockTimeout, readLockTimeout);
                break;
            default:
                throw new DataStorageManagerException("invalid NON-UNIQUE index type " + index.type);
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
        if (!created) {
            long _stop = System.currentTimeMillis();
            LOGGER.info("bootIndex {} {}.{} time {} ms", nodeId, tableSpaceName, index.name, (_stop - _start) + "");
        }
        if (rebuild) {
            indexManager.rebuild();
        }
        dbmanager.getPlanner().clearCache();
        return indexManager;
    }

    private void validateAlterTable(Table table, StatementEvaluationContext context) {
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
        tableManager.validateAlterTable(table, context);
    }

    private AbstractTableManager alterTable(Table table, Transaction transaction) throws DDLException {
        LOGGER.info("alterTable {} {}.{} uuid {}", nodeId, tableSpaceName, table.name, table.uuid);
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
            Map<String, AbstractIndexManager> removed = indexesByTable.remove(oldTableName);
            if (removed != null && !removed.isEmpty()) {
                indexesByTable.put(table.name, removed);
            }
        }
        rebuildForeignKeyReferences(table);
        return tableManager;
    }

    public void close() throws LogNotAvailableException {
        boolean useJmx = dbmanager.getServerConfiguration().getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT);
        closed = true;

        if (followerThread != null) {
            try {
                followerThread.waitForStop();
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                LOGGER.error("Cannot wait for FollowerThread to stop", err);
            }
        }
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

    public boolean isClosed() {
        return closed;
    }

    private static class TableSpaceCheckpoint {

        private final LogSequenceNumber sequenceNumber;
        private final Map<String, LogSequenceNumber> tablesCheckpoints;

        public TableSpaceCheckpoint(
                LogSequenceNumber sequenceNumber,
                Map<String, LogSequenceNumber> tablesCheckpoints
        ) {
            super();
            this.sequenceNumber = sequenceNumber;
            this.tablesCheckpoints = tablesCheckpoints;
        }
    }

    //this method return a tableCheckSum object contain scan values (record numbers , table digest,digestType, next autoincrement value, table name, tablespacename, query used for table scan )
    public TableChecksum createAndWriteTableCheksum(TableSpaceManager tableSpaceManager, String tableSpaceName, String tableName, StatementEvaluationContext context) throws IOException, DataScannerException {
        CommitLogResult pos;
        boolean lockAcquired = false;
        if (context == null) {
           context = StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT();
        }
        long lockStamp = context.getTableSpaceLock();
        LOGGER.info("Create and write table {} checksum in tablespace " , tableName, tableSpaceName);
        if (lockStamp == 0) {
            lockStamp = acquireWriteLock("checkDataConsistency_" + tableName);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            AbstractTableManager tablemanager = tableSpaceManager.getTableManager(tableName);
            if (tableSpaceManager == null) {
                throw new TableSpaceDoesNotExistException(String.format("Tablespace %s does not exist.", tableSpaceName));
            }
            if (tablemanager == null || tablemanager.getCreatedInTransaction() > 0) {
                throw new TableDoesNotExistException(String.format("Table %s does not exist.", tablemanager));
            }
            TableChecksum scanResult = TableDataChecksum.createChecksum(tableSpaceManager.getDbmanager(), null, tableSpaceManager, tableSpaceName, tableName);
            byte[] serialize = MAPPER.writeValueAsBytes(scanResult);

            Bytes value = Bytes.from_array(serialize);
            LogEntry entry = LogEntryFactory.dataConsistency(tableName, value);
            pos = log.log(entry, false);
            apply(pos, entry, false);
            return scanResult;

        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), "checkDataConsistency");
                context.setTableSpaceLock(0);
            }
        }
    }

    // visible for testing
    public TableSpaceCheckpoint checkpoint(boolean full, boolean pin, boolean alreadLocked) throws DataStorageManagerException, LogNotAvailableException {
        if (virtual) {
            return null;
        }

        if (recoveryInProgress) {
            LOGGER.info("Checkpoint for tablespace {} skipped. Recovery is still in progress", tableSpaceName);
            return null;
        }

        long _start = System.currentTimeMillis();
        LogSequenceNumber logSequenceNumber = null;
        LogSequenceNumber _logSequenceNumber = null;
        Map<String, LogSequenceNumber> checkpointsTableNameSequenceNumber = new HashMap<>();

        try {
            List<PostCheckpointAction> actions = new ArrayList<>();

            long lockStamp = 0;
            if (!alreadLocked) {
                lockStamp = acquireWriteLock("checkpoint");
            }
            try {
                logSequenceNumber = log.getLastSequenceNumber();

                if (logSequenceNumber.isStartOfTime()) {
                    LOGGER.info("{} checkpoint {} at {}. skipped (no write ever issued to log)", nodeId, tableSpaceName, logSequenceNumber);
                    return new TableSpaceCheckpoint(logSequenceNumber, checkpointsTableNameSequenceNumber);
                }
                LOGGER.info("{} checkpoint start {} at {}", nodeId, tableSpaceName, logSequenceNumber);
                if (actualLogSequenceNumber == null) {
                    throw new DataStorageManagerException("actualLogSequenceNumber cannot be null");
                }
                // TODO: transactions checkpoint is not atomic
                Collection<Transaction> currentTransactions = new ArrayList<>(transactions.values());
                for (Transaction t : currentTransactions) {
                    LogSequenceNumber txLsn = t.lastSequenceNumber;
                    if (txLsn != null && txLsn.after(logSequenceNumber)) {
                        LOGGER.error("Found transaction {} with LSN {} in the future", t.transactionId, txLsn);
                    }
                }
                actions.addAll(dataStorageManager.writeTransactionsAtCheckpoint(tableSpaceUUID, logSequenceNumber, currentTransactions));
                actions.addAll(writeTablesOnDataStorageManager(new CommitLogResult(logSequenceNumber, false, true), true));

                // we checkpoint all data to disk and save the actual log sequence number
                for (AbstractTableManager tableManager : tables.values()) {
                    // each TableManager will save its own checkpoint sequence number (on TableStatus) and upon recovery will replay only actions with log position after the actual table-local checkpoint
                    // remember that the checkpoint for a table can last "minutes" and we do not want to stop the world

                    if (!tableManager.isSystemTable()) {
                        TableCheckpoint checkpoint = full ? tableManager.fullCheckpoint(pin) : tableManager.checkpoint(pin);

                        if (checkpoint != null) {
                            LOGGER.info("checkpoint done for table {}.{} (pin: {})", tableSpaceName, tableManager.getTable().name, pin);
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
                    LOGGER.error("postcheckpoint error:" + error, error);
                }
            }
            return new TableSpaceCheckpoint(logSequenceNumber, checkpointsTableNameSequenceNumber);
        } finally {
            long _stop = System.currentTimeMillis();
            LOGGER.info("{} checkpoint finish {} started ad {}, finished at {}, total time {} ms",
                    nodeId, tableSpaceName, logSequenceNumber, _logSequenceNumber, Long.toString(_stop - _start));
            checkpointTimeStats.registerSuccessfulEvent(_stop, TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<StatementExecutionResult> beginTransactionAsync(StatementEvaluationContext context, boolean releaseLock) throws StatementExecutionException {

        long id = newTransactionId.incrementAndGet();

        LogEntry entry = LogEntryFactory.beginTransaction(id);
        CommitLogResult pos;
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireReadLock("begin transaction");
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        pos = log.log(entry, false);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(id, TransactionResult.OutcomeType.BEGIN);
        }, callbacksExecutor);
        if (lockAcquired && releaseLock) {
            releaseReadLock(res, context.getTableSpaceLock(), "begin transaction");
        }
        return res;

    }

    private CompletableFuture<StatementExecutionResult> rollbackTransaction(RollbackTransactionStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        long txId = statement.getTransactionId();
        validateTransactionBeforeTxCommand(txId);
        LogEntry entry = LogEntryFactory.rollbackTransaction(txId);
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        CommitLogResult pos = log.log(entry, true);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(txId, TransactionResult.OutcomeType.ROLLBACK);
        }, callbacksExecutor);

        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> commitTransaction(CommitTransactionStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        long txId = statement.getTransactionId();

        validateTransactionBeforeTxCommand(txId);
        LogEntry entry = LogEntryFactory.commitTransaction(txId);
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        CommitLogResult pos = log.log(entry, true);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.handleAsync((lsn, error) -> {
            if (error == null) {
                apply(pos, entry, false);
                return new TransactionResult(txId, TransactionResult.OutcomeType.COMMIT);
            } else {
                // if the log is not able to write the commit
                // apply a dummy "rollback", we are no more going to accept commands
                // in the scope of this transaction
                LogEntry rollback = LogEntryFactory.rollbackTransaction(txId);
                apply(new CommitLogResult(LogSequenceNumber.START_OF_TIME, false, false), rollback, false);
                throw new CompletionException(error);
            }
        }, callbacksExecutor);
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;
    }

    private void validateTransactionBeforeTxCommand(long txId) throws StatementExecutionException {
        validateTransactionBeforeTxCommand(txId, true);
    }

    private boolean validateTransactionBeforeTxCommand(long txId, boolean wait) throws StatementExecutionException {
        Transaction tc = transactions.get(txId);
        if (tc == null) {
            throw new StatementExecutionException("no such transaction " + txId + " in tablespace " + tableSpaceName);
        }
        while (tc.hasPendingActivities() && !closed) {
            LOGGER.info("Transaction {} ({}) has {} pending activities", txId, tableSpaceName, tc.getRefCount());
            if (!ENABLE_PENDING_TRANSACTION_CHECK) {
                return true;
            }
            if (!wait) {
                return false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new StatementExecutionException("Error while waiting for pending activities of transaction " + txId + " in " + tableSpaceName,
                        ex);
            }
        }
        if (closed) {
            throw new StatementExecutionException("tablespace closed during commit of transaction " + txId + " in tablespace " + tableSpaceName);
        }
        return true;
    }

    private CompletableFuture<StatementExecutionResult> releaseReadLock(
            CompletableFuture<StatementExecutionResult> promise, long lockStamp, Object description
    ) {
        return promise.whenComplete((r, error) -> {
            releaseReadLock(lockStamp, description);
        });
    }

    private void releaseReadLock(long lockStamp, Object description) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} ts {} relrlock {}", tableSpaceName, lockStamp, description);
        }
//        LOGGER.error("RELEASED READLOCK for " + description + ", " + generalLock);
        generalLock.unlockRead(lockStamp);
    }

    public boolean isLeader() {
        return leader;
    }

    public Transaction getTransaction(long transactionId) {
        if (transactionId <= 0) {
            return null;
        }
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
            if (dbmanager.isStopped()) {
                throw new RuntimeException("System was requested to stop, aborting recovery at " + t);
            }
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

    public ExecutorService getCallbacksExecutor() {
        return callbacksExecutor;
    }

    @Override
    public String toString() {
        return "TableSpaceManager [nodeId=" + nodeId
                + ", tableSpaceName=" + tableSpaceName
                + ", tableSpaceUUID=" + tableSpaceUUID + "]";
    }
}
