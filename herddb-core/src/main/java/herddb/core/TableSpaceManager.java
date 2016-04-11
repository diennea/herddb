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

import herddb.log.CommitLog;
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
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAwareStatement;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private boolean leader;

    public TableSpaceManager(String nodeId, String tableSpaceName, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager, CommitLog log) {
        this.nodeId = nodeId;
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.log = log;
        this.tableSpaceName = tableSpaceName;
    }

    void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException, DDLException {

        TableSpace tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
        recover(tableSpaceInfo);

        tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
        if (tableSpaceInfo.leaderId.equals(nodeId)) {
            startAsLeader();
        }
    }

    void recover(TableSpace tableSpaceInfo) throws DataStorageManagerException, LogNotAvailableException {

        List<Table> tablesAtBoot = dataStorageManager.loadTables(tableSpaceInfo.lastCheckpointLogPosition, tableSpaceName);
        LOGGER.log(Level.SEVERE, "tablesAtBoot", tablesAtBoot.stream().map(t -> {
            return t.name;
        }).collect(Collectors.joining()));
        for (Table table : tablesAtBoot) {
            bootTable(table);
        }

        LOGGER.log(Level.SEVERE, "recovering tablespace " + tableSpaceName + " log from sequence number " + tableSpaceInfo.lastCheckpointLogPosition);

        log.recovery(tableSpaceInfo.lastCheckpointLogPosition, new BiConsumer<LogSequenceNumber, LogEntry>() {
            @Override
            public void accept(LogSequenceNumber t, LogEntry u) {
                try {
                    apply(u);
                } catch (Exception err) {
                    throw new RuntimeException(err);
                }
            }
        }, false);

    }

    void apply(LogEntry entry) throws DataStorageManagerException, DDLException {
        LOGGER.log(Level.SEVERE, "apply entry {0}", new Object[]{entry});
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
            }
            ;
            break;
        }

        if (entry.tableName != null) {
            TableManager tableManager = tables.get(new Bytes(entry.tableName));
            tableManager.apply(entry);
        }

    }

    void startAsLeader() throws DataStorageManagerException, DDLException, LogNotAvailableException {

        // every pending transaction MUST be rollback back
        List<Long> pending_transactions = new ArrayList<>(this.transactions.keySet());
        log.startWriting();
        LOGGER.log(Level.SEVERE, "startAsLeader tablespace " + tableSpaceName + " log, there were " + transactions.size() + " pending transactions to be rolledback");
        for (long tx : pending_transactions) {
            LOGGER.log(Level.SEVERE, "rolling back transaction " + tx);
            LogEntry rollback = LogEntryFactory.rollbackTransaction(tableSpaceName, tx);
            // let followers see the rollback on the log
            log.log(rollback);
            apply(rollback);
        }
        leader = true;
    }

    private ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();

    StatementExecutionResult executeStatement(Statement statement) throws StatementExecutionException {
        Transaction transaction = transactions.get(statement.getTransactionId());
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
            return manager.executeStatement(statement, transaction);
        }

        throw new StatementExecutionException("unsupported statement " + statement);
    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction) throws StatementExecutionException {
        try {
            generalLock.writeLock().lock();

            LogEntry entry = LogEntryFactory.createTable(statement.getTableDefinition(), transaction);
            try {
                log.log(entry);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            apply(entry);

            return new DDLStatementExecutionResult();
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    private void bootTable(Table table) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "bootTable", table.name);
        TableManager tableManager = new TableManager(table, log, dataStorageManager, this);
        tables.put(Bytes.from_string(table.name), tableManager);
        tableManager.start();
    }

    public void close() throws LogNotAvailableException {
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

    private StatementExecutionResult beginTransaction() throws StatementExecutionException {
        long id = newTransactionId.incrementAndGet();

        LogEntry entry = LogEntryFactory.beginTransaction(tableSpaceName, id);
        try {
            log.log(entry);
            apply(entry);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }

        return new TransactionResult(id);
    }

    private StatementExecutionResult rollbackTransaction(RollbackTransactionStatement rollbackTransactionStatement) throws StatementExecutionException {
        Transaction tx = transactions.get(rollbackTransactionStatement.getTransactionId());
        if (tx == null) {
            throw new StatementExecutionException("no such transaction " + rollbackTransactionStatement.getTransactionId());
        }
        LogEntry entry = LogEntryFactory.rollbackTransaction(tableSpaceName, tx.transactionId);
        try {
            log.log(entry);
            apply(entry);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }

        return new TransactionResult(tx.transactionId);
    }

    private StatementExecutionResult commitTransaction(CommitTransactionStatement commitTransactionStatement) throws StatementExecutionException {
        Transaction tx = transactions.get(commitTransactionStatement.getTransactionId());
        if (tx == null) {
            throw new StatementExecutionException("no such transaction " + commitTransactionStatement.getTransactionId());
        }
        LogEntry entry = LogEntryFactory.commitTransaction(tableSpaceName, tx.transactionId);
        try {
            log.log(entry);
            apply(entry);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }

        return new TransactionResult(tx.transactionId);
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

}
