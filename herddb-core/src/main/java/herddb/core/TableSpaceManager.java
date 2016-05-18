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

import herddb.client.ClientConfiguration;
import herddb.client.ClientSideMetadataProvider;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.log.CommitLog;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.TransactionResult;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.NodeMetadata;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAwareStatement;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.ScanStatement;
import herddb.network.ServerHostData;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
    private final String nodeId;
    private final Map<Bytes, TableManager> tables = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();
    private final AtomicLong newTransactionId = new AtomicLong();
    private final Map<String, Table> tablesMetadata = new ConcurrentHashMap<>();
    private final DBManager manager;
    private boolean leader;
    private boolean closed;
    private boolean failed;
    private LogSequenceNumber actualLogSequenceNumber;

    public TableSpaceManager(String nodeId, String tableSpaceName, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager, CommitLog log, DBManager manager) {
        this.nodeId = nodeId;
        this.manager = manager;
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.log = log;
        this.tableSpaceName = tableSpaceName;
    }

    void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException, DDLException {

        TableSpace tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
        recover(tableSpaceInfo);

        actualLogSequenceNumber = log.getActualSequenceNumber();
        LOGGER.log(Level.SEVERE, " after recovery of tableSpace " + tableSpaceName + ", actualLogSequenceNumber:" + actualLogSequenceNumber);

        tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
        if (tableSpaceInfo.leaderId.equals(nodeId)) {
            startAsLeader();
        } else {
            startAsFollower();
        }
    }

    void recover(TableSpace tableSpaceInfo) throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {
        LogSequenceNumber logSequenceNumber = dataStorageManager.getLastcheckpointSequenceNumber(tableSpaceName);
        LOGGER.log(Level.SEVERE, "recover, logSequenceNumber from DataStorage: " + logSequenceNumber);
        List<Table> tablesAtBoot = dataStorageManager.loadTables(logSequenceNumber, tableSpaceName);
        LOGGER.log(Level.SEVERE, "tablesAtBoot", tablesAtBoot.stream().map(t -> {
            return t.name;
        }).collect(Collectors.joining()));
        for (Table table : tablesAtBoot) {
            bootTable(table);
        }

        LOGGER.log(Level.SEVERE, "recovering tablespace " + tableSpaceName + " log from sequence number " + logSequenceNumber);

        try {
            log.recovery(logSequenceNumber, new BiConsumer<LogSequenceNumber, LogEntry>() {
                @Override
                public void accept(LogSequenceNumber t, LogEntry u) {
                    try {
                        apply(t, u);
                    } catch (Exception err) {
                        throw new RuntimeException(err);
                    }
                }
            }, false);
            return;
        } catch (FullRecoveryNeededException fullRecoveryNeeded) {
            LOGGER.log(Level.SEVERE, "full recovery of data is needed for tableSpace " + tableSpaceName, fullRecoveryNeeded);
        }
        downloadTableSpaceData();
        log.recovery(logSequenceNumber, new BiConsumer<LogSequenceNumber, LogEntry>() {
            @Override
            public void accept(LogSequenceNumber t, LogEntry u) {
                try {
                    apply(t, u);
                } catch (Exception err) {
                    throw new RuntimeException(err);
                }
            }
        }, false);

    }

    void apply(LogSequenceNumber position, LogEntry entry) throws DataStorageManagerException, DDLException {
        this.actualLogSequenceNumber = position;
        LOGGER.log(Level.SEVERE, "apply entry {0} {1}", new Object[]{position, entry});
        switch (entry.type) {
            case LogEntryType.BEGINTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = new Transaction(id, tableSpaceName);
                transactions.put(id, transaction);
            }
            break;
            case LogEntryType.ROLLBACKTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = transactions.get(id);
                List<TableManager> managers;
                try {
                    generalLock.writeLock().lock();
                    managers = new ArrayList<>(tables.values());
                } finally {
                    generalLock.writeLock().unlock();
                }
                for (TableManager manager : managers) {
                    if (transaction.getNewTables().containsKey(manager.getTable().name)) {
                        LOGGER.log(Level.SEVERE, "rollback CREATE TABLE " + manager.getTable().name);
                        manager.dropTableData();
                        manager.close();
                        tables.remove(Bytes.from_string(manager.getTable().name));
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
                List<TableManager> managers;
                try {
                    generalLock.writeLock().lock();
                    managers = new ArrayList<>(tables.values());
                } finally {
                    generalLock.writeLock().unlock();
                }
                for (TableManager manager : managers) {
                    manager.onTransactionCommit(transaction);
                }
                if (!transaction.getNewTables().isEmpty()) {
                    writeTablesOnDataStorageManager();
                }
                transactions.remove(transaction.transactionId);
            }
            break;
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value);
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewTable(table);
                }
                bootTable(table);
                if (entry.transactionId < 0) {
                    writeTablesOnDataStorageManager();
                }
            }
            break;
        }

        if (entry.tableName != null) {
            TableManager tableManager = tables.get(new Bytes(entry.tableName));
            tableManager.apply(entry);
        }

    }

    private void writeTablesOnDataStorageManager() throws DataStorageManagerException {
        List<Table> tablelist = new ArrayList<>();
        for (TableManager tableManager : tables.values()) {
            tablelist.add(tableManager.getTable());
        }
        dataStorageManager.writeTables(tableSpaceName, actualLogSequenceNumber, tablelist);
    }

    DataScanner scan(ScanStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
            throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
        }
        String table = statement.getTable();
        TableManager manager;
        generalLock.readLock().lock();
        try {
            manager = tables.get(Bytes.from_string(table));
        } finally {
            generalLock.readLock().unlock();
        }
        if (manager == null) {
            throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName);
        }
        return manager.scan(statement, context, transaction);
    }

    private void downloadTableSpaceData() throws MetadataStorageManagerException, DataStorageManagerException {
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
        try (HDBClient client = new HDBClient(new ClientConfiguration());) {
            client.setClientSideMetadataProvider(new ClientSideMetadataProvider() {
                @Override
                public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
                    return leaderId;
                }

                @Override
                public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
                    return new ServerHostData(leaderId, nodeData.port, nodeData.host, nodeData.ssl, Collections.emptyMap());
                }
            });
            try (HDBConnection con = client.openConnection()) {
                List<String> tables = con.listTables(tableSpaceName);
                LOGGER.log(Level.SEVERE, "discovered tables " + tables + " on " + tableSpaceName + " from server " + leaderId);
                for (String table : tables) {
                    downloadTabledata(table, con);
                }
            } catch (ClientSideMetadataProviderException | HDBException networkError) {
                throw new DataStorageManagerException(networkError);
            }

        }

    }

    private void downloadTabledata(String table, HDBConnection con) throws DataStorageManagerException {
        dataStorageManager.dropTable(tableSpaceName, table);
        dataStorageManager.downloadTable(tableSpaceName, table, con);
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
                            LOGGER.log(Level.SEVERE, "follow " + num + ", " + u.toString());
                            try {
                                apply(num, u);
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
        LOGGER.log(Level.SEVERE, "failed!", new Exception().fillInStackTrace());
        failed = true;
    }

    boolean isFailed() {
        return failed;
    }

    void startAsFollower() throws DataStorageManagerException, DDLException, LogNotAvailableException {
        manager.submit(new FollowerThread());
    }

    void startAsLeader() throws DataStorageManagerException, DDLException, LogNotAvailableException {

        // every pending transaction MUST be rollback back
        List<Long> pending_transactions = new ArrayList<>(this.transactions.keySet());
        log.startWriting();
        LOGGER.log(Level.SEVERE, "startAsLeader tablespace {0} log, there were {1} pending transactions to be rolledback", new Object[]{tableSpaceName, transactions.size()});
        for (long tx : pending_transactions) {
            LOGGER.log(Level.SEVERE, "rolling back transaction {0}", tx);
            LogEntry rollback = LogEntryFactory.rollbackTransaction(tableSpaceName, tx);
            // let followers see the rollback on the log
            LogSequenceNumber pos = log.log(rollback);
            apply(pos, rollback);
        }
        leader = true;
    }

    private final ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();

    StatementExecutionResult executeStatement(Statement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
            throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
        }
        if (statement instanceof CreateTableStatement) {
            return createTable((CreateTableStatement) statement, transaction);
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

        if (statement instanceof TableAwareStatement) {
            TableAwareStatement st = (TableAwareStatement) statement;
            String table = st.getTable();
            TableManager manager;
            generalLock.readLock().lock();
            try {
                manager = tables.get(Bytes.from_string(table));
            } finally {
                generalLock.readLock().unlock();
            }
            if (manager == null) {
                throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName);
            }
            return manager.executeStatement(statement, transaction, context);
        }

        throw new StatementExecutionException("unsupported statement " + statement);
    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction) throws StatementExecutionException {
        try {
            generalLock.writeLock().lock();

            LogEntry entry = LogEntryFactory.createTable(statement.getTableDefinition(), transaction);
            LogSequenceNumber pos;
            try {
                pos = log.log(entry);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            apply(pos, entry);

            return new DDLStatementExecutionResult();
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    private void bootTable(Table table) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "bootTable {0} {1}.{2}", new Object[]{nodeId, tableSpaceName, table.name});
        TableManager tableManager = new TableManager(table, log, dataStorageManager, this);
        tables.put(Bytes.from_string(table.name), tableManager);
        tableManager.start();
    }

    public void close() throws LogNotAvailableException {
        closed = true;
        try {
            generalLock.writeLock().lock();
            for (TableManager table : tables.values()) {
                table.close();
            }
            log.close();
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    void flush() throws DataStorageManagerException {
        List<TableManager> managers;
        try {
            generalLock.writeLock().lock();
            managers = new ArrayList<>(tables.values());
        } finally {
            generalLock.writeLock().unlock();
        }
        for (TableManager manager : managers) {
            manager.flush();
        }
    }

    void checkpoint() throws LogNotAvailableException, DataStorageManagerException {
        log.checkpoint();
    }

    private StatementExecutionResult beginTransaction() throws StatementExecutionException {
        long id = newTransactionId.incrementAndGet();

        LogEntry entry = LogEntryFactory.beginTransaction(tableSpaceName, id);
        LogSequenceNumber pos;
        try {
            pos = log.log(entry);
            apply(pos, entry);
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
        LogSequenceNumber pos;
        try {
            pos = log.log(entry);
            apply(pos, entry);
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
            LogSequenceNumber pos = log.log(entry);
            apply(pos, entry);
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

    public TableManager getTableManager(String tableName) {
        return tables.get(Bytes.from_string(tableName));
    }

    public Collection<Long> getOpenTransactions() {
        return new HashSet<>(this.transactions.keySet());
    }

}
