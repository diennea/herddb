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

import herddb.utils.EnsureLongIncrementAccumulator;
import herddb.core.stats.TableManagerStats;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.index.PrimaryIndexSeek;
import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DDLException;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Predicate;
import herddb.model.RecordFunction;
import herddb.model.commands.InsertStatement;
import herddb.model.Statement;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.Projection;
import herddb.model.Record;
import herddb.model.ScanLimits;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.model.Tuple;
import herddb.model.commands.TruncateTableStatement;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.BatchOrderedExecutor;
import herddb.utils.Bytes;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import herddb.utils.SystemProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.ws.Holder;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 */
public final class TableManager implements AbstractTableManager {

    private static final Logger LOGGER = Logger.getLogger(TableManager.class.getName());

    private static final int UNLOAD_PAGES_MIN_BATCH = SystemProperties.
        getIntSystemProperty(TableManager.class.getName() + ".unloadMinBatch", 10);

    private static final int SORTED_PAGE_ACCESS_WINDOW_SIZE = SystemProperties.
        getIntSystemProperty(TableManager.class.getName() + ".sortedPageAccessWindowSize", 2000);

    private static final boolean ENABLE_LOCAL_SCAN_PAGE_CACHE = SystemProperties.
        getBooleanSystemProperty(TableManager.class.getName() + ".enableLocalScanPageCache", false);

    public static final Long NEW_PAGE = Long.valueOf(-1);

    private final DataPage dirtyRecordsPage = new DataPage(NEW_PAGE, 0, new ConcurrentHashMap<>(), false);

    private final ConcurrentHashMap<Long, DataPage> pages = new ConcurrentHashMap<>();

    /**
     * A structure which maps each key to the ID of the page (map<byte[], long>) (this can be quite large)
     */
    private final KeyToPageIndex keyToPage;

    /**
     * Keys deleted since the last checkpoint
     */
    private final Set<Bytes> deletedKeys = new ConcurrentSkipListSet<>();

    private final PageSet pageSet = new PageSet();

    private final AtomicInteger dirtyRecords = new AtomicInteger();

    private final AtomicLong newPageId = new AtomicLong(1);

    /**
     * Local locks
     */
    private final LocalLockManager locksManager = new LocalLockManager();

    /**
     * Access to Pages
     */
    private final ReentrantLock pagesLock = new ReentrantLock(true);

    private volatile boolean checkPointRunning = false;

    /**
     * Allow checkpoint
     */
    private final ReentrantReadWriteLock checkpointLock = new ReentrantReadWriteLock(false);

    /**
     * auto_increment support
     */
    private final AtomicLong nextPrimaryKeyValue = new AtomicLong(1);

    private final TableContext tableContext;

    /**
     * Phisical ID of the TableSpace
     */
    private final String tableSpaceUUID;

    /**
     * Definition of the table
     */
    private Table table;
    private final CommitLog log;
    private final DataStorageManager dataStorageManager;
    private final TableSpaceManager tableSpaceManager;

    /**
     * Max logical size of a page (raw key size + raw value size)
     */
    private final long maxLogicalPageSize;

    private final long maxTableUsedMemory;

    /**
     * This value is not empty until the transaction who creates the table does not commit
     */
    private long createdInTransaction;

    private final TableManagerStats stats;

    void prepareForRestore(LogSequenceNumber dumpLogSequenceNumber) {
        LOGGER.log(Level.SEVERE, "Table " + table.name + ", receiving dump,"
            + "done at external logPosition " + dumpLogSequenceNumber);
        this.dumpLogSequenceNumber = dumpLogSequenceNumber;
    }

    void restoreFinished() {
        dumpLogSequenceNumber = null;
        LOGGER.log(Level.SEVERE, "Table " + table.name + ", received dump");
    }

    private final class TableManagerStatsImpl implements TableManagerStats {

        @Override
        public int getLoadedpages() {
            // dirty records pages (-1) is not counted
            return pages.size() - 1;
        }

        @Override
        public long getTablesize() {
            return keyToPage.size();
        }

        @Override
        public int getDirtypages() {
            return pageSet.getDirtyPagesCount();
        }

        @Override
        public int getDirtyrecords() {
            return dirtyRecords.get();
        }

        @Override
        public long getMaxLogicalPageSize() {
            return maxLogicalPageSize;
        }

        @Override
        public long getBuffersUsedMemory() {
            long value = 0;
            for (DataPage page : pages.values()) {
                value += page.getUsedMemory();
            }
            return value;
        }

        @Override
        public long getKeysUsedMemory() {
            return keyToPage.getUsedMemory();
        }

        @Override
        public long getMaxTableUsedMemory() {
            return maxTableUsedMemory;
        }
    }

    TableManager(Table table, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID,
        long maxLogicalPageSize,
        long maxTableUsedMemory, long createdInTransaction) throws DataStorageManagerException {
        this.stats = new TableManagerStatsImpl();

        this.pages.put(NEW_PAGE, dirtyRecordsPage);

        this.table = table;
        this.tableSpaceManager = tableSpaceManager;
        this.log = log;
        this.dataStorageManager = dataStorageManager;
        this.createdInTransaction = createdInTransaction;
        this.tableSpaceUUID = tableSpaceUUID;
        this.tableContext = buildTableContext();
        this.maxLogicalPageSize = maxLogicalPageSize;
        this.keyToPage = dataStorageManager.createKeyToPageMap(tableSpaceUUID, table.name);
        this.maxTableUsedMemory = maxTableUsedMemory;
    }

    private TableContext buildTableContext() {
        TableContext tableContext;
        if (!table.auto_increment) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    throw new UnsupportedOperationException("no auto_increment function on this table");
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    return Bytes.from_int((int) nextPrimaryKeyValue.getAndIncrement()).data;
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.LONG) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    return Bytes.from_long((int) nextPrimaryKeyValue.getAndIncrement()).data;
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    throw new UnsupportedOperationException("no auto_increment function on this table");
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        }
        return tableContext;
    }

    @Override
    public Table getTable() {
        return table;
    }

    private LogSequenceNumber bootSequenceNumber;
    private LogSequenceNumber dumpLogSequenceNumber;

    @Override
    public void start() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "loading in memory all the keys for table {0}", new Object[]{table.name});
        Set<Long> activePagesAtBoot = new HashSet<>();
        bootSequenceNumber = log.getLastSequenceNumber();

        dataStorageManager.fullTableScan(tableSpaceUUID, table.name,
            new FullTableScanConsumer() {

            Long currentPage;

            @Override
            public void acceptTableStatus(TableStatus tableStatus) {
                LOGGER.log(Level.SEVERE, "recovery table at " + tableStatus.sequenceNumber);
                nextPrimaryKeyValue.set(Bytes.toLong(tableStatus.nextPrimaryKeyValue, 0, 8));
                newPageId.set(tableStatus.nextPageId);
                bootSequenceNumber = tableStatus.sequenceNumber;
            }

            @Override
            public void startPage(long pageId) {
                currentPage = pageId;
            }

            @Override
            public void acceptRecord(Record record) {
                if (currentPage < 0) {
                    throw new IllegalStateException();
                }
                keyToPage.put(record.key, currentPage);
                activePagesAtBoot.add(currentPage);
            }

            @Override
            public void endPage() {
                currentPage = null;
            }

            @Override
            public void endTable() {
            }

        });
        dataStorageManager.cleanupAfterBoot(tableSpaceUUID, table.name, activePagesAtBoot);
        pageSet.setActivePagesAtBoot(activePagesAtBoot);
        LOGGER.log(Level.SEVERE, "loaded {0} keys for table {1}, newPageId {2}, nextPrimaryKeyValue {3}, activePages {4}", new Object[]{keyToPage.size(), table.name, newPageId.get(), nextPrimaryKeyValue.get(), pageSet.getActivePages() + ""});
    }

    @Override
    public StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        checkpointLock.readLock().lock();
        try {
            if (statement instanceof UpdateStatement) {
                UpdateStatement update = (UpdateStatement) statement;
                return executeUpdate(update, transaction, context);
            }
            if (statement instanceof InsertStatement) {
                InsertStatement insert = (InsertStatement) statement;
                return executeInsert(insert, transaction, context);
            }
            if (statement instanceof GetStatement) {
                GetStatement get = (GetStatement) statement;
                return executeGet(get, transaction, context);
            }
            if (statement instanceof DeleteStatement) {
                DeleteStatement delete = (DeleteStatement) statement;
                return executeDelete(delete, transaction, context);
            }
            if (statement instanceof TruncateTableStatement) {
                TruncateTableStatement truncate = (TruncateTableStatement) statement;
                return executeTruncate(truncate, transaction, context);
            }
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException("internal data error: " + err, err);
        } finally {
            checkpointLock.readLock().unlock();
            if (statement instanceof TruncateTableStatement) {
                try {
                    flush();
                } catch (DataStorageManagerException err) {
                    throw new StatementExecutionException("internal data error: " + err, err);
                }
            }
        }
        throw new StatementExecutionException("unsupported statement " + statement);

    }

    private void unloadPages(int count) {

        /* Do not unload pages if not really requested */
        if (count < 0) {
            return;
        }
        Set<Long> pagesNow = new HashSet<>(pages.keySet());
        Set<Long> pagesToUnload = PageSet.selectPagesToUnload(count, pagesNow);

        if (!pagesToUnload.isEmpty()) {
            LOGGER.log(Level.FINER, "table " + table.tablespace + "." + table.name + ","
                + "unloading " + pagesToUnload.size() + " pages (" + pages.size() + " pages actually loaded),"
                + "" + dirtyRecordsPage.data.size() + " dirty records");
            unloadPages(pagesToUnload);
        } else {
            long countDirty = dirtyRecordsPage.size();
            if (countDirty > 0) {
                LOGGER.log(Level.INFO, "table " + table.tablespace + "." + table.name + ", no page to unload, " + countDirty + " dirty records, checkpoint needed");
                requestCheckpoint();
            } else {
                LOGGER.log(Level.FINER, "table " + table.tablespace + "." + table.name + ", no page to unload, " + countDirty + " dirty records, nothing to do");
            }
        }
    }

    private void requestCheckpoint() {
        if (dumpLogSequenceNumber != null) {
            // we are restoring the table, it is better to perform the checkpoint inside the same thread
            this.checkpoint(LogSequenceNumber.START_OF_TIME);
        } else {
            this.tableSpaceManager.requestTableCheckPoint(table.name);
        }
    }

    private void unloadPages(Set<Long> pagesToUnload) {
        LOGGER.log(Level.FINE, "table {0} unloading pages {1}", new Object[]{table.name, pagesToUnload});
        for (Long pageId : pagesToUnload) {
            DataPage removed = pages.remove(pageId);
            if (removed != null) {
                LOGGER.log(Level.FINER, "table {0} removed page {1}, {2}", new Object[]{table.name, pageId, removed.getUsedMemory() / (1024 * 1024) + " MB"});
            }
        };
    }

    private LockHandle lockForWrite(Bytes key, Transaction transaction) {
        if (transaction != null) {
            LockHandle lock = transaction.lookupLock(table.name, key);
            if (lock != null) {
                if (lock.write) {
                    // transaction already locked the key for writes
                    return lock;
                } else {
                    // transaction already locked the key, but we need to upgrade the lock
                    locksManager.releaseLock(lock);
                    transaction.unregisterUpgradedLocksOnTable(table.name, lock);
                    lock = locksManager.acquireWriteLockForKey(key);
                    transaction.registerLockOnTable(this.table.name, lock);
                    return lock;
                }
            } else {
                lock = locksManager.acquireWriteLockForKey(key);
                transaction.registerLockOnTable(this.table.name, lock);
                return lock;
            }
        } else {
            return locksManager.acquireWriteLockForKey(key);
        }
    }

    private LockHandle lockForRead(Bytes key, Transaction transaction) {
        if (transaction != null) {
            LockHandle lock = transaction.lookupLock(table.name, key);
            if (lock != null) {
                // transaction already locked the key
                return lock;
            } else {
                lock = locksManager.acquireReadLockForKey(key);
                transaction.registerLockOnTable(this.table.name, lock);
                return lock;
            }
        } else {
            return locksManager.acquireReadLockForKey(key);
        }
    }

    private StatementExecutionResult executeInsert(InsertStatement insert, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        /*
         an insert can succeed only if the row is valid and the "keys" structure  does not contain the requested key
         the insert will add the row in the 'buffer' without assigning a page to it
         locks: the insert uses global 'insert' lock on the table
         the insert will update the 'maxKey' for auto_increment primary keys
         */
        Bytes key = new Bytes(insert.getKeyFunction().computeNewValue(null, context, tableContext));
        byte[] value = insert.getValuesFunction().computeNewValue(new Record(key, null), context, tableContext);

        LockHandle lock = lockForWrite(key, transaction);
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    // OK, INSERT on a DELETED record inside this transaction
                } else if (transaction.recordInserted(table.name, key) != null) {
                    // ERROR, INSERT on a INSERTED record inside this transaction
                    throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
                } else if (keyToPage.containsKey(key)) {
                    throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
                }
            } else if (keyToPage.containsKey(key)) {
                throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
            }
            LogEntry entry = LogEntryFactory.insert(table, key.data, value, transaction);
            LogSequenceNumber pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);
            return new DMLStatementExecutionResult(entry.transactionId, 1, key, insert.isReturnValues() ? Bytes.from_array(value) : null);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (transaction == null) {
                locksManager.releaseWriteLockForKey(key, lock);
            }
        }
    }

    private static class ExitLoop extends RuntimeException {
    }

    private static class ScanResultOperation {

        public void accept(Record record) throws StatementExecutionException, DataStorageManagerException, LogNotAvailableException {
        }
    }

    private StatementExecutionResult executeUpdate(UpdateStatement update, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        AtomicInteger updateCount = new AtomicInteger();
        Holder<Bytes> lastKey = new Holder<>();
        Holder<byte[]> lastValue = new Holder<>();
        /*
         an update can succeed only if the row is valid, the key is contains in the "keys" structure
         the update will simply override the value of the row, assigning a null page to the row
         the update can have a 'where' predicate which is to be evaluated against the decoded row, the update will be executed only if the predicate returns boolean 'true' value  (CAS operation)
         locks: the update  uses a lock on the the key
         */
        RecordFunction function = update.getFunction();
        long transactionId = transaction != null ? transaction.transactionId : 0;
        Predicate predicate = update.getPredicate();

        ScanStatement scan = new ScanStatement(table.tablespace, table, predicate);
        accessTableData(scan, context, new ScanResultOperation() {
            @Override
            public void accept(Record actual) throws StatementExecutionException, LogNotAvailableException, DataStorageManagerException {
                byte[] newValue = function.computeNewValue(actual, context, tableContext);
                LogEntry entry = LogEntryFactory.update(table, actual.key.data, newValue, transaction);
                LogSequenceNumber pos = log.log(entry, entry.transactionId <= 0);
                apply(pos, entry, false);
                lastKey.value = actual.key;
                lastValue.value = newValue;
                updateCount.incrementAndGet();
            }
        }, transaction, true, true);

        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
            update.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);

    }

    private StatementExecutionResult executeDelete(DeleteStatement delete, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {

        AtomicInteger updateCount = new AtomicInteger();
        Holder<Bytes> lastKey = new Holder<>();
        Holder<byte[]> lastValue = new Holder<>();

        long transactionId = transaction != null ? transaction.transactionId : 0;
        Predicate predicate = delete.getPredicate();

        ScanStatement scan = new ScanStatement(table.tablespace, table, predicate);
        accessTableData(scan, context, new ScanResultOperation() {
            @Override
            public void accept(Record actual) throws StatementExecutionException, LogNotAvailableException, DataStorageManagerException {
                LogEntry entry = LogEntryFactory.delete(table, actual.key.data, transaction);
                LogSequenceNumber pos = log.log(entry, entry.transactionId <= 0);
                apply(pos, entry, false);
                lastKey.value = actual.key;
                lastValue.value = actual.value.data;
                updateCount.incrementAndGet();
            }
        }, transaction, true, true);
        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
            delete.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
    }

    private StatementExecutionResult executeTruncate(TruncateTableStatement truncate, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        if (transaction != null) {
            throw new StatementExecutionException("TRUNCATE TABLE cannot be executed within the context of a Transaction");
        }

        try {
            long estimatedSize = keyToPage.size();
            LogEntry entry = LogEntryFactory.truncate(table, transaction);
            LogSequenceNumber pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);
            return new DMLStatementExecutionResult(0, estimatedSize > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) estimatedSize, null, null);
        } catch (LogNotAvailableException error) {
            throw new StatementExecutionException(error);
        }
    }

    private void applyTruncate() throws DataStorageManagerException {
        if (createdInTransaction > 0) {
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed on an uncommitted table");
        }
        if (checkPointRunning) {
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed during a checkpoint");
        }
        if (tableSpaceManager.isTransactionRunningOnTable(table.name)) {
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed table " + table.name
                + ": at least one transaction is pending on it");
        }
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            for (AbstractIndexManager index : indexes.values()) {
                if (!index.isAvailable()) {
                    throw new DataStorageManagerException("index " + index.getIndexName()
                        + " in not full available. Cannot TRUNCATE table " + table.name);
                }
            }
        }
        pageSet.truncate();
        dirtyRecords.set(0);
        pages.clear();
        pages.put(NEW_PAGE, dirtyRecordsPage);
        dirtyRecordsPage.clear();
        deletedKeys.clear();
        locksManager.clear();
        keyToPage.truncate();
        if (indexes != null) {
            for (AbstractIndexManager index : indexes.values()) {
                index.truncate();
            }
        }
    }

    

    @Override
    public void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException {
        if (transaction == null) {
            throw new DataStorageManagerException("transaction cannot be null");
        }
        boolean forceFlushTableData = false;
        if (createdInTransaction > 0) {
            if (transaction.transactionId != createdInTransaction) {
                throw new DataStorageManagerException("this tableManager is available only on transaction " + createdInTransaction);
            }
            createdInTransaction = 0;
            forceFlushTableData = true;
        }

        Map<Bytes, Record> changedRecords = transaction.changedRecords.get(table.name);
        // transaction is still holding locks on each record, so we can change records        
        Map<Bytes, Record> newRecords = transaction.newRecords.get(table.name);
        if (newRecords != null) {
            for (Record record : newRecords.values()) {                
                applyInsert(record.key, record.value);
            }
        }
        if (changedRecords != null) {
            for (Record r : changedRecords.values()) {                
                applyUpdate(r.key, r.value);
            }
        }
        Set<Bytes> deletedRecords = transaction.deletedRecords.get(table.name);
        if (deletedRecords != null) {
            for (Bytes key : deletedRecords) {                
                applyDelete(key);
            }
        }
        transaction.releaseLocksOnTable(table.name, locksManager);

        if (forceFlushTableData) {
            LOGGER.log(Level.SEVERE, "forcing local checkpoint, table " + table.name + " will be visible to all transactions now");
            checkpoint(log.getLastSequenceNumber());
        }
    }

    @Override
    public void onTransactionRollback(Transaction transaction) {
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    @Override
    public void apply(LogSequenceNumber position, LogEntry entry, boolean recovery) throws DataStorageManagerException {
        if (recovery) {
            if (dumpLogSequenceNumber != null && !position.after(dumpLogSequenceNumber)) {
                // in "restore mode" the 'position" parameter is from the 'old' transaction log
                Transaction transaction = null;
                if (entry.transactionId > 0) {
                    transaction = tableSpaceManager.getTransaction(entry.transactionId);
                }
                if (transaction != null) {
                    LOGGER.log(Level.FINER, "{0}.{1} keep {2} at {3}, table restored from position {4}, it belongs to transaction {5} which was in progress during the dump of the table", new Object[]{table.tablespace, table.name, entry, position, dumpLogSequenceNumber, entry.transactionId});
                } else {
                    LOGGER.log(Level.FINER, "{0}.{1} skip {2} at {3}, table restored from position {4}", new Object[]{table.tablespace, table.name, entry, position, dumpLogSequenceNumber});
                    return;
                }
            } else if (!position.after(bootSequenceNumber)) {
                // recovery mode
                Transaction transaction = null;
                if (entry.transactionId > 0) {
                    transaction = tableSpaceManager.getTransaction(entry.transactionId);
                }
                if (transaction != null) {
                    LOGGER.log(Level.FINER, "{0}.{1} keep {2} at {3}, table booted at {4}, it belongs to transaction {5} which was in progress during the flush of the table", new Object[]{table.tablespace, table.name, entry, position, bootSequenceNumber, entry.transactionId});
                } else {
                    LOGGER.log(Level.FINER, "{0}.{1} skip {2} at {3}, table booted at {4}", new Object[]{table.tablespace, table.name, entry, position, bootSequenceNumber});
                    return;
                }
            }
        }
        switch (entry.type) {
            case LogEntryType.DELETE: {
                // remove the record from the set of existing records
                Bytes key = new Bytes(entry.key);                
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                    }
                    transaction.registerDeleteOnTable(this.table.name, key, position);
                } else {
                    applyDelete(key);
                }
                break;
            }
            case LogEntryType.UPDATE: {
                Bytes key = new Bytes(entry.key);
                Bytes value = new Bytes(entry.value);                
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                    }
                    transaction.registerRecordUpdate(this.table.name, key, value, position);
                } else {
                    applyUpdate(key, value);
                }
                break;
            }
            case LogEntryType.INSERT: {
                Bytes key = new Bytes(entry.key);
                Bytes value = new Bytes(entry.value);                
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                    }
                    transaction.registerInsertOnTable(table.name, key, value, position);
                } else {
                    applyInsert(key, value);
                }
                break;
            }
            case LogEntryType.TRUNCATE_TABLE: {
                applyTruncate();
            }
            ;
            break;
            default:
                throw new IllegalArgumentException("unhandled entry type " + entry.type);
        }
    }

    private void applyDelete(Bytes key) throws DataStorageManagerException {
        Long pageId = keyToPage.remove(key);
        if (pageId == null) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is not present in table " + table.name);
        }
        deletedKeys.add(key);
        Record record;
        DataPage dataPage = pages.get(pageId);
        if (dataPage == null) {
            dataPage = loadPageToMemory(pageId, false);
            if (dataPage == null) {
                throw new IllegalStateException("page " + pageId + " not loaded in memory during delete!");
            }
        }
        if (!NEW_PAGE.equals(pageId)) {
            record = dataPage.get(key);
            pageSet.setPageDirty(pageId);
        } else {
            record = dataPage.remove(key);
        }
        dirtyRecords.incrementAndGet();

        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            if (record == null) {
                throw new RuntimeException("deleted record at " + key + " was not loaded in buffer, cannot update indexes");
            }
            Map<String, Object> values = record.toBean(table);
            for (AbstractIndexManager index : indexes.values()) {
                index.recordDeleted(key, values);
            }
        }
    }

    private void applyUpdate(Bytes key, Bytes value) throws DataStorageManagerException {
        Long pageId = keyToPage.put(key, NEW_PAGE);
        if (pageId == null) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is not present in table " + table.name);
        }
        Record newRecord = new Record(key, value);
        if (!NEW_PAGE.equals(pageId)) {
            pageSet.setPageDirty(pageId);
        }

        dirtyRecords.incrementAndGet();

        // previous record can be among "dirty records" or in a data page
        Record previous = dirtyRecordsPage.put(key, newRecord);

        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            if (previous == null) {
                DataPage oldPage = pages.get(pageId);
                if (oldPage == null) {
                    oldPage = loadPageToMemory(pageId, false);
                }
                if (oldPage == null) {
                    throw new IllegalStateException("page not loaded " + pageId + " while updating record " + key);
                }
                previous = oldPage.get(key);
            }
            if (previous == null) {
                throw new RuntimeException("updated record at " + key + " was not loaded in buffer, cannot update indexes");
            }
            Map<String, Object> prevValues = previous.toBean(table);
            Map<String, Object> newValues = newRecord.toBean(table);
            for (AbstractIndexManager index : indexes.values()) {
                index.recordUpdated(key, prevValues, newValues);
            }
        }
    }

    @Override
    public void dropTableData() throws DataStorageManagerException {
        dataStorageManager.dropTable(tableSpaceUUID, table.name);
    }

    @Override
    public void scanForIndexRebuild(Consumer<Record> records) throws DataStorageManagerException {
        LocalScanPageCache localPageCache = new LocalScanPageCache();
        Consumer<Map.Entry<Bytes, Long>> scanExecutor = (Map.Entry<Bytes, Long> entry) -> {
            Bytes key = entry.getKey();
            LockHandle lock = lockForRead(key, null);
            try {
                Long pageId = entry.getValue();
                if (pageId != null) {
                    Record record = fetchRecord(key, pageId, localPageCache);
                    if (record != null) {
                        records.accept(record);
                    }
                }
            } catch (DataStorageManagerException | StatementExecutionException error) {
                throw new RuntimeException(error);
            } finally {
                locksManager.releaseReadLockForKey(key, lock);
            }
        };
        Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(null, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), tableContext, null);
        scanner.forEach(scanExecutor);

    }

    @Override
    public void dump(FullTableScanConsumer receiver) throws DataStorageManagerException {

        dataStorageManager.fullTableScan(tableSpaceUUID, table.name, receiver);
    }

    public void writeFromDump(List<Record> record) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, table.name + " received " + record.size() + " records");
        checkpointLock.readLock().lock();
        try {            
            for (Record r : record) {
                applyInsert(r.key, r.value);
            }
        } finally {
            checkpointLock.readLock().unlock();
        }
    }

    private void applyInsert(Bytes key, Bytes value) throws DataStorageManagerException {
        if (table.auto_increment) {
            // the next auto_increment value MUST be greater than every other explict value            
            long pk_logical_value;
            if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER) {
                pk_logical_value = key.to_int();
            } else {
                pk_logical_value = key.to_long();
            }
            nextPrimaryKeyValue.accumulateAndGet(pk_logical_value + 1, EnsureLongIncrementAccumulator.INSTANCE);
        }
        Long pageId = keyToPage.put(key, NEW_PAGE);
        if (pageId != null) {
            // very strage but possible inside a transaction which executes DELETE THEN INSERT,
            // we have to track that the previous page is "dirty"
            if (!NEW_PAGE.equals(pageId)) {
                pageSet.setPageDirty(pageId);
            }
            LOGGER.log(Level.SEVERE, "record " + key + " already present in keyToPage?");
        }
        Record record = new Record(key, value);
        dirtyRecordsPage.put(key, record);

        deletedKeys.remove(key);
        dirtyRecords.incrementAndGet();

        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            Map<String, Object> values = record.toBean(table);
            for (AbstractIndexManager index : indexes.values()) {
                index.recordInserted(key, values);
            }
        }

    }

    @Override
    public void flush() throws DataStorageManagerException {
        checkpoint(log.getLastSequenceNumber());
    }

    @Override
    public void close() {
        dataStorageManager.releaseKeyToPageMap(tableSpaceUUID, table.name, keyToPage);
    }

    private StatementExecutionResult executeGet(GetStatement get, Transaction transaction,
        StatementEvaluationContext context)
        throws StatementExecutionException, DataStorageManagerException {
        Bytes key = new Bytes(get.getKey().computeNewValue(null, context, tableContext));
        Predicate predicate = get.getPredicate();
        boolean requireLock = get.isRequireLock();
        long transactionId = transaction != null ? transaction.transactionId : 0;
        LockHandle lock = (transaction != null || requireLock) ? lockForRead(key, transaction) : null;
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    return GetResult.NOT_FOUND(transactionId);
                }
                Record loadedInTransaction = transaction.recordUpdated(table.name, key);
                if (loadedInTransaction != null) {
                    if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                        return GetResult.NOT_FOUND(transactionId);
                    }
                    return new GetResult(transactionId, loadedInTransaction, table);
                }
                loadedInTransaction = transaction.recordInserted(table.name, key);
                if (loadedInTransaction != null) {
                    if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                        return GetResult.NOT_FOUND(transactionId);
                    }
                    return new GetResult(transactionId, loadedInTransaction, table);
                }
            }
            Long pageId = keyToPage.get(key);
            if (pageId == null) {
                return GetResult.NOT_FOUND(transactionId);
            }
            Record loaded = fetchRecord(key, pageId, null);
            if (loaded == null || (predicate != null && !predicate.evaluate(loaded, context))) {
                return GetResult.NOT_FOUND(transactionId);
            }
            return new GetResult(transactionId, loaded, table);

        } finally {
            if (transaction == null && lock != null) {
                locksManager.releaseReadLockForKey(key, lock);
            }
        }
    }

    private DataPage temporaryLoadPageToMemory(Long pageId) throws DataStorageManagerException {
        DataPage result = pages.get(pageId);;
        if (result != null) {
            return result;
        }
        long _start = System.currentTimeMillis();
        List<Record> page = dataStorageManager.readPage(tableSpaceUUID, table.name, pageId);
        long _io = System.currentTimeMillis();
        result = buildDataPage(pageId, page);
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.INFO, "tmp table " + table.name + ","
            + "loaded " + result.size() + " records from page " + pageId
            + " in " + (_stop - _start) + " ms"
            + ", (" + (_io - _start) + " ms read)");
        return result;
    }

    private DataPage loadPageToMemory(Long pageId, boolean recovery) throws DataStorageManagerException {
        DataPage result = pages.get(pageId);;
        if (result != null) {
            return result;
        }
        long _start = System.currentTimeMillis();
        long _ioAndLock = 0;
        long _limitTime = 0;
        AtomicBoolean computed = new AtomicBoolean();
        pagesLock.lock();
        try {
            result = pages.computeIfAbsent(pageId, (id) -> {
                try {
                    computed.set(true);
                    List<Record> page = dataStorageManager.readPage(tableSpaceUUID, table.name, pageId);
                    return buildDataPage(pageId, page);
                } catch (DataStorageManagerException err) {
                    throw new RuntimeException(err);
                }
            });
            if (computed.get()) {
                _ioAndLock = System.currentTimeMillis();
                ensureMemoryLimits();
                _limitTime = System.currentTimeMillis();
            }
        } catch (RuntimeException error) {
            if (error.getCause() != null
                && error.getCause() instanceof DataStorageManagerException) {
                throw (DataStorageManagerException) error.getCause();
            } else {
                throw new DataStorageManagerException(error);
            }
        } finally {
            pagesLock.unlock();
        }
        if (computed.get()) {
            long _stop = System.currentTimeMillis();
            LOGGER.log(Level.FINE,
                "table " + table.name + ","
                + "loaded " + result.size() + " records from page " + pageId
                + " in " + (_stop - _start) + " ms"
                + ", (" + (_ioAndLock - _start) + " ms read + plock"
                + ", " + (_limitTime - _ioAndLock) + " ms lim"
                + ", " + (_stop - _limitTime) + " ms unlock)");
        }
        return result;
    }

    private DataPage buildDataPage(long pageId, List<Record> page) {
        Map<Bytes, Record> newPageMap = new HashMap<>();
        long estimatedPageSize = 0;
        for (Record r : page) {
            newPageMap.put(r.key, r);
            estimatedPageSize += r.key.data.length + r.value.data.length;
        }
        DataPage res = new DataPage(pageId, estimatedPageSize, newPageMap, true);
        return res;
    }

    @Override
    public void ensureMemoryLimits() {
        if (maxTableUsedMemory > 0) {
            long used = (stats.getBuffersUsedMemory() + stats.getKeysUsedMemory());
            long toReclaim = used - maxTableUsedMemory;
            releaseMemory(toReclaim);
        }
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        if (createdInTransaction > 0) {
            LOGGER.log(Level.SEVERE, "checkpoint for table " + table.name + " skipped,"
                + "this table is created on transaction " + createdInTransaction + " which is not committed");
            return Collections.emptyList();
        }
        long start = System.currentTimeMillis();
        long getlock;
        long scanbuffer;
        long createnewpages;
        long tablecheckpoint;
        long unload;
        List<PostCheckpointAction> result = new ArrayList<>();
        checkpointLock.writeLock().lock();
        try {
            pagesLock.lock();
            try {
                getlock = System.currentTimeMillis();
                checkPointRunning = true;

                Set<Long> dirtyPages = pageSet.getDirtyPages();
                Set<Long> tmpLoadedPages = new HashSet<>();

                List<Record> recordsOnDirtyPages = new ArrayList<>();
                Map<Bytes, Record> tmpBuffer = new HashMap<>();

                Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(null,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), tableContext, null);
                scanner.forEach((Map.Entry<Bytes, Long> recordToPage) -> {
                    try {
                        Bytes key = recordToPage.getKey();
                        Long pageId = recordToPage.getValue();
                        if (NEW_PAGE.equals(pageId) || dirtyPages.contains(pageId)) {
                            if (!pages.containsKey(pageId)
                                && !tmpBuffer.containsKey(key)
                                && !NEW_PAGE.equals(pageId)) {
                                if (!tmpLoadedPages.add(pageId)) {
                                    throw new DataStorageManagerException("table " + table.name
                                        + " page " + pageId + " to be loaded twice on tmp buffer duting checkpoint");
                                }
                                List<Record> page = dataStorageManager
                                    .readPage(tableSpaceUUID, table.name, pageId);
                                for (Record r : page) {
                                    tmpBuffer.put(r.key, r);
                                }
                                LOGGER.log(Level.SEVERE, "loaded dirty page " + pageId + " on tmp buffer "
                                    + ": " + page.size() + " records");
                            }
                            DataPage dataPage = pages.get(pageId);

                            Record record = dataPage != null ? dataPage.get(key) : null;
                            if (record == null) {
                                record = tmpBuffer.get(key);
                            }
                            if (record == null) {
                                throw new DataStorageManagerException("table " + table.name
                                    + " found missing record key " + key
                                    + " on page " + pageId + ", dataPage is " + dataPage);
                            }
                            recordsOnDirtyPages.add(record);
                            if (tmpBuffer.size() > 100_000) {
                                LOGGER.log(Level.SEVERE, "need to flush internal tmp buffer");
                                tmpBuffer.clear();
                                tmpLoadedPages.clear();
                            }
                        }
                    } catch (DataStorageManagerException err) {
                        throw new RuntimeException(err);
                    }
                });
                scanbuffer = System.currentTimeMillis();
                LOGGER.log(Level.INFO, "checkpoint {0}, flush dirtyPages, {1} pages, logpos {2}, recordsOnDirtyPages {3}", new Object[]{table.name,
                    dirtyPages.toString(),
                    sequenceNumber,
                    recordsOnDirtyPages.size()});
                List<Record> newPage = new ArrayList<>();
                long newPageSize = 0;
                for (Record toKeep : recordsOnDirtyPages) {
                    newPage.add(toKeep);
                    newPageSize += toKeep.key.data.length + toKeep.value.data.length;
                    if (newPageSize >= maxLogicalPageSize) {
                        createNewPage(newPage, newPageSize);
                        newPageSize = 0;
                        newPage.clear();
                    }
                }
                if (!newPage.isEmpty()) {
                    createNewPage(newPage, newPageSize);
                }
                tmpBuffer.clear();
                createnewpages = System.currentTimeMillis();
                pageSet.checkpointDone(dirtyPages);
                dirtyRecords.set(0);
                for (Long idDirtyPage : dirtyPages) {
                    pages.remove(idDirtyPage);
                }

                TableStatus tableStatus = new TableStatus(table.name, sequenceNumber, Bytes.from_long(nextPrimaryKeyValue.get()).data, newPageId.get(),
                    pageSet.getActivePages());
                List<PostCheckpointAction> actions = dataStorageManager.tableCheckpoint(tableSpaceUUID, table.name, tableStatus);
                tablecheckpoint = System.currentTimeMillis();
                result.addAll(actions);
                LOGGER.log(Level.INFO, "checkpoint {0} finished, now {1}, flushed {2} records", new Object[]{table.name, pageSet + "", recordsOnDirtyPages.size() + " records"});
                dirtyRecordsPage.clear();
                checkPointRunning = false;
                ensureMemoryLimits();
            } finally {
                pagesLock.unlock();
            }
        } finally {
            checkpointLock.writeLock().unlock();
        }
        long end = System.currentTimeMillis();
        long delta = end - start;
        if (delta > 5000) {

            long delta_lock = getlock - start;
            long delta_scanbuffer = scanbuffer - getlock;
            long delta_createnewpages = createnewpages - scanbuffer;
            long delta_tablecheckpoint = tablecheckpoint - createnewpages;
            long delta_unload = end - tablecheckpoint;

            LOGGER.log(Level.INFO, "long checkpoint for {0}, time {1}", new Object[]{table.name,
                delta + " ms (" + delta_lock
                + "+" + delta_scanbuffer
                + "+" + delta_createnewpages
                + "+" + delta_tablecheckpoint
                + "+" + delta_unload + ")"});
        }
        return result;
    }

    private long createNewPage(List<Record> newPage, long newPageSize) throws DataStorageManagerException {

        long pageId = this.newPageId.getAndIncrement();
        DataPage dataPage = buildDataPage(pageId, newPage);

        LOGGER.log(Level.FINER, "createNewPage table {0}, pageId={1} with {2} records, {3} logical page size",
            new Object[]{table.name, pageId, newPage.size(), newPageSize});
        dataStorageManager.writePage(tableSpaceUUID, table.name, pageId, newPage);
        pageSet.pageCreated(pageId);
        pages.put(pageId, dataPage);
        Long _pageId = pageId;
        for (Record record : newPage) {
            keyToPage.put(record.key, _pageId);
        }
        return pageId;
    }

    @Override
    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, Transaction transaction) throws StatementExecutionException {
        boolean sorted = statement.getComparator() != null;
        final Projection projection = statement.getProjection();
        boolean applyProjectionDuringScan = !sorted && projection != null;
        MaterializedRecordSet recordSet;
        if (applyProjectionDuringScan) {
            recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory().createRecordSet(projection.getColumns());
        } else {
            recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory().createRecordSet(table.columns);
        }
        ScanLimits limits = statement.getLimits();
        int maxRows = limits == null ? 0 : limits.computeMaxRows(context);
        boolean sortDone = false;
        if (maxRows > 0) {
            if (sorted) {
                InStreamTupleSorter sorter = new InStreamTupleSorter(limits.getOffset() + maxRows, statement.getComparator());
                accessTableData(statement, context, new ScanResultOperation() {
                    @Override
                    public void accept(Record record) throws StatementExecutionException {
                        Tuple tuple;
                        if (applyProjectionDuringScan) {
                            tuple = projection.map(record, table, context);
                        } else {
                            tuple = new Tuple(record.toBean(table), table.columns);
                        }
                        sorter.collect(tuple);
                    }
                }, transaction, false, false);
                sorter.flushToRecordSet(recordSet);
                sortDone = true;
            } else {
                // if no sort is present the limits can be applying during the scan and perform an early exit
                AtomicInteger remaining = new AtomicInteger(limits.computeMaxRows(context));

                if (limits.getOffset() > 0) {
                    remaining.getAndAdd(limits.getOffset());
                }
                accessTableData(statement, context, new ScanResultOperation() {
                    @Override
                    public void accept(Record record) throws StatementExecutionException {
                        Tuple tuple;
                        if (applyProjectionDuringScan) {
                            tuple = projection.map(record, table, context);
                        } else {
                            tuple = new Tuple(record.toBean(table), table.columns);
                        }
                        recordSet.add(tuple);
                        if (remaining.decrementAndGet() == 0) {
                            throw new ExitLoop();
                        }
                    }
                }, transaction, false, false);
            }
        } else {
            accessTableData(statement, context, new ScanResultOperation() {
                @Override
                public void accept(Record record) throws StatementExecutionException {
                    Tuple tuple;
                    if (applyProjectionDuringScan) {
                        tuple = projection.map(record, table, context);
                    } else {
                        tuple = new Tuple(record.toBean(table), table.columns);
                    }
                    recordSet.add(tuple);
                }
            }, transaction, false, false);
        }

        recordSet.writeFinished();
        if (!sortDone) {
            recordSet.sort(statement.getComparator());
        }
        recordSet.applyLimits(statement.getLimits(), context);
        if (!applyProjectionDuringScan) {
            recordSet.applyProjection(statement.getProjection(), context);
        }
        return new SimpleDataScanner(transaction != null ? transaction.transactionId : 0, recordSet);
    }

    private void accessTableData(ScanStatement statement, StatementEvaluationContext context, ScanResultOperation consumer, Transaction transaction,
        boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        Predicate predicate = statement.getPredicate();
        long _start = System.currentTimeMillis();
        boolean acquireLock = transaction != null || forWrite || lockRequired;

        LocalScanPageCache lastPageRead = acquireLock ? null : new LocalScanPageCache();

        try {

            IndexOperation indexOperation = predicate != null ? predicate.getIndexOperation() : null;
            boolean primaryIndexSeek = indexOperation instanceof PrimaryIndexSeek;
            AbstractIndexManager useIndex = getIndexForTbleAccess(indexOperation);

            BatchOrderedExecutor.Executor<Map.Entry<Bytes, Long>> scanExecutor = (List<Map.Entry<Bytes, Long>> batch) -> {
                for (Map.Entry<Bytes, Long> entry : batch) {
                    Bytes key = entry.getKey();
                    boolean keep_lock = false;
                    boolean already_locked = transaction != null && transaction.lookupLock(table.name, key) != null;
                    LockHandle lock = acquireLock ? (forWrite ? lockForWrite(key, transaction) : lockForRead(key, transaction)) : null;
                    try {
                        if (transaction != null) {
                            if (transaction.recordDeleted(table.name, key)) {
                                // skip this record. inside current transaction it has been deleted
                                continue;
                            }
                            Record record = transaction.recordUpdated(table.name, key);
                            if (record != null) {
                                // use current transaction version of the record
                                if (predicate == null || predicate.evaluate(record, context)) {
                                    consumer.accept(record);
                                    keep_lock = true;
                                }
                                continue;
                            }
                        }
                        Long pageId = entry.getValue();
                        if (pageId != null) {
                            boolean pkFilterCompleteMatch = false;
                            if (!primaryIndexSeek && predicate != null) {
                                Predicate.PrimaryKeyMatchOutcome outcome
                                    = predicate.matchesRawPrimaryKey(key, context);
                                if (outcome == Predicate.PrimaryKeyMatchOutcome.FAILED) {
                                    continue;
                                } else if (outcome == Predicate.PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED) {
                                    pkFilterCompleteMatch = true;
                                }
                            }
                            Record record = fetchRecord(key, pageId, lastPageRead);
                            if (record != null && (pkFilterCompleteMatch || predicate == null || predicate.evaluate(record, context))) {
                                consumer.accept(record);
                                keep_lock = true;
                            }
                        }
                    } finally {
                        // release the lock on the key if it did not match scan criteria
                        if (transaction == null) {
                            if (lock != null) {
                                if (forWrite) {
                                    locksManager.releaseWriteLockForKey(key, lock);
                                } else {
                                    locksManager.releaseReadLockForKey(key, lock);
                                }
                            }
                        } else if (!keep_lock && !already_locked) {
                            transaction.releaseLockOnKey(table.name, key, locksManager);
                        }
                    }
                }
            };
            BatchOrderedExecutor<Map.Entry<Bytes, Long>> executor = new BatchOrderedExecutor<>(SORTED_PAGE_ACCESS_WINDOW_SIZE,
                scanExecutor, SORTED_PAGE_ACCESS_COMPARATOR);
            Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(indexOperation, context, tableContext, useIndex);
            boolean exit = false;
            try {
                scanner.forEach(executor);
                executor.finish();
            } catch (ExitLoop exitLoop) {
                exit = true;
                LOGGER.log(Level.SEVERE, "exit loop during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), exitLoop.toString()});
            } catch (final HerdDBInternalException error) {
                LOGGER.log(Level.SEVERE, "error during scan", error);
                if (error.getCause() instanceof StatementExecutionException) {
                    throw (StatementExecutionException) error.getCause();
                } else if (error.getCause() instanceof DataStorageManagerException) {
                    throw (DataStorageManagerException) error.getCause();
                } else if (error instanceof StatementExecutionException) {
                    throw (StatementExecutionException) error;
                } else if (error instanceof DataStorageManagerException) {
                    throw (DataStorageManagerException) error;
                } else {
                    throw new StatementExecutionException(error);
                }
            }

            if (!exit && transaction != null) {
                for (Record record : transaction.getNewRecordsForTable(table.name)) {
                    if (!transaction.recordDeleted(table.name, record.key)
                        && (predicate == null || predicate.evaluate(record, context))) {
                        consumer.accept(record);
                    }
                }
            }

        } catch (ExitLoop exitLoop) {
            LOGGER.log(Level.SEVERE, "exit loop during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), exitLoop.toString()});
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            LOGGER.log(Level.SEVERE, "error during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), err.toString()});
            throw new StatementExecutionException(err);
        } catch (StatementExecutionException err) {
            LOGGER.log(Level.SEVERE, "error during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), err.toString()});
            throw err;
        }
    }

    private AbstractIndexManager getIndexForTbleAccess(IndexOperation indexOperation) {
        AbstractIndexManager useIndex = null;
        if (indexOperation != null) {
            Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
            if (indexes != null) {
                useIndex = indexes.get(indexOperation.getIndexName());
                if (useIndex != null && !useIndex.isAvailable()) {
                    useIndex = null;
                }
            }
        }
        return useIndex;
    }

    @Override
    public List<Index> getAvailableIndexes() {
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes == null) {
            return Collections.emptyList();
        }
        return indexes.values().stream().filter(AbstractIndexManager::isAvailable).map(AbstractIndexManager::getIndex)
            .collect(Collectors.toList());
    }

    private Record fetchRecord(Bytes key, Long pageId, LocalScanPageCache localScanPageCache) throws StatementExecutionException, DataStorageManagerException {
        int maxTrials = 10_000;
        while (true) {
            Record record;
            if (NEW_PAGE.equals(pageId)) {
                record = dirtyRecordsPage.get(key);
            } else {
                DataPage dataPage = fetchDataPage(pageId, localScanPageCache);
                record = dataPage.get(key);
            }
            if (record != null) {
                return record;
            }
            Long relocatedPageId = keyToPage.get(key);
            LOGGER.log(Level.SEVERE, table.name + " fetchRecord " + key + " failed,"
                + "checkPointRunning:" + checkPointRunning + " pageId:" + pageId + " relocatedPageId:" + relocatedPageId);
            if (relocatedPageId == null) {
                // deleted
                LOGGER.log(Level.SEVERE, "table " + table.name + ", activePages " + pageSet.getActivePages() + ", record " + key + " deleted during data access");
                return null;
            }
            pageId = relocatedPageId;
            if (maxTrials-- == 0) {
                throw new DataStorageManagerException("inconsistency! table " + table.name + " no record in memory for " + key + " page " + pageId + ", activePages " + pageSet.getActivePages() + " after many trials");
            }
        }
    }

    private DataPage fetchDataPage(Long pageId, LocalScanPageCache localScanPageCache) throws DataStorageManagerException {
        DataPage dataPage;
        if (localScanPageCache == null || !ENABLE_LOCAL_SCAN_PAGE_CACHE) {
            dataPage = loadPageToMemory(pageId, false);
        } else {
            if (pageId.equals(localScanPageCache.pageId)) {
                dataPage = localScanPageCache.value;
            } else {
                // TODO: add heuristics and choose whether to load the page in the main buffer
                dataPage = temporaryLoadPageToMemory(pageId);
                localScanPageCache.value = dataPage;
                localScanPageCache.pageId = pageId;
            }
        }
        return dataPage;
    }

    @Override
    public TableManagerStats getStats() {
        return stats;
    }

    @Override
    public long getNextPrimaryKeyValue() {
        return nextPrimaryKeyValue.get();
    }

    @Override
    public boolean isSystemTable() {
        return false;
    }

    @Override
    public void tableAltered(Table table, Transaction transaction) throws DDLException {
        // compute diff, if some column as been dropped we need to remove the value from each record
        List<String> droppedColumns = new ArrayList<>();
        for (Column c : this.table.columns) {
            if (table.getColumn(c.name) == null) {
                droppedColumns.add(c.name);
            }
        }

        this.table = table;
        if (!droppedColumns.isEmpty()) {
            // no lock is necessary
            pages.values().forEach(p -> {
                p.data.values().forEach(r -> r.clearCache());
            });
        }
    }

    @Override
    public long getCreatedInTransaction() {
        return createdInTransaction;
    }

    @Override
    public void tryReleaseMemory(long reclaim) {
        pagesLock.lock();
        try {
            releaseMemory(reclaim);
        } finally {
            pagesLock.unlock();
        }
    }

    private void releaseMemory(long reclaim) {
        if (reclaim <= 0) {
            return;
        }
        int countPages = (int) (reclaim / maxLogicalPageSize);
        if (countPages < UNLOAD_PAGES_MIN_BATCH) {
            countPages = UNLOAD_PAGES_MIN_BATCH;
        }
        int dirtypages = stats.getDirtypages();
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, "Table " + table.tablespace + "." + table.name
                + ": used memory " + (stats.getKeysUsedMemory() / (1024 * 1024)) + "+" + (stats.getBuffersUsedMemory() / (1024 * 1024)) + " MB, "
                + dirtypages + " dirtypages, try to release " + countPages + " pages, to reclaim " + (reclaim / (1024 * 1024)) + " MB ");
        }
        unloadPages(countPages);
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Table " + table.tablespace + "." + table.name
                + " after unload used memory " + (stats.getKeysUsedMemory() / (1024 * 1024)) + "+" + (stats.getBuffersUsedMemory() / (1024 * 1024)) + " MB, "
                + dirtypages + " dirtypages");
        }
    }

    private static final Comparator<Map.Entry<Bytes, Long>> SORTED_PAGE_ACCESS_COMPARATOR = (a, b) -> {
        return a.getValue().compareTo(b.getValue());
    };
}
