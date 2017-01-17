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
import herddb.index.SecondaryIndexSeek;
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
import herddb.model.Record;
import herddb.model.Statement;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.model.DataScanner;
import herddb.model.Projection;
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
import herddb.utils.Bytes;
import herddb.utils.MinDeltaLongIncrementAccumulator;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import herddb.utils.SystemProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.xml.ws.Holder;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 */
public class TableManager implements AbstractTableManager {

    private static final Logger LOGGER = Logger.getLogger(TableManager.class.getName());

    private static final int MAX_LOADED_PAGES = SystemProperties.
        getIntSystemProperty(TableManager.class.getName() + ".maxLoadedPages", 1000);

    private static final int MAX_DIRTY_RECORDS = SystemProperties.
        getIntSystemProperty(TableManager.class.getName() + ".maxDirtyRecords", 10000);

    private static final int UNLOAD_PAGES_MIN_BATCH = SystemProperties.
        getIntSystemProperty(TableManager.class.getName() + ".unloadMinBatch", 10);

    public static final Long NEW_PAGE = Long.valueOf(-1);

    /**
     * a buffer which contains the rows contained into the loaded pages (map<byte[],byte[]>)
     */
    private final Map<Bytes, Record> buffer = new ConcurrentHashMap<>();

    /**
     * keyToPage: a structure which maps each key to the ID of the page (map<byte[], long>) (this can be quite large)
     */
    private final KeyToPageIndex keyToPage;

    /**
     * Keys deleted since the last checkpoint
     */
    private final Set<Bytes> deletedKeys = new ConcurrentSkipListSet<>();

    /**
     * a structure which holds the set of the pages which are loaded in memory (set<long>)
     */
    private final Set<Long> loadedPages = new ConcurrentSkipListSet<>();

    private final Set<Long> dirtyPages = new ConcurrentSkipListSet<>();

    private final Set<Long> activePages = new HashSet<>();

    private final AtomicInteger dirtyRecords = new AtomicInteger();

    private final AtomicLong lastAutoFlush = new AtomicLong();

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

    /**
     * This value is not empty until the transaction who creates the table does not commit
     */
    private long createdInTransaction;

    TableManager(Table table, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID, long maxLogicalPageSize, long createdInTransaction) throws DataStorageManagerException {
        this.table = table;
        this.tableSpaceManager = tableSpaceManager;
        this.log = log;
        this.dataStorageManager = dataStorageManager;
        this.createdInTransaction = createdInTransaction;
        this.tableSpaceUUID = tableSpaceUUID;
        this.tableContext = buildTableContext();
        this.maxLogicalPageSize = maxLogicalPageSize;
        this.keyToPage = dataStorageManager.createKeyToPageMap(tableSpaceUUID, table.name);
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

    @Override
    public void start() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "loading in memory all the keys for table {0}", new Object[]{table.name});
        Set<Long> activePagesAtBoot = new HashSet<>();
        bootSequenceNumber = log.getLastSequenceNumber();

        dataStorageManager.fullTableScan(tableSpaceUUID, table.name,
            new FullTableScanConsumer() {

            long currentPage;

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
                currentPage = -1;
            }
        });
        dataStorageManager.cleanupAfterBoot(tableSpaceUUID, table.name, activePagesAtBoot);

        this.activePages.clear();
        this.activePages.addAll(activePagesAtBoot);
        LOGGER.log(Level.SEVERE, "loaded {0} keys for table {1}, newPageId {2}, nextPrimaryKeyValue {3}, activePages {4}", new Object[]{keyToPage.size(), table.name, newPageId.get(), nextPrimaryKeyValue.get(), activePages + ""});
    }

    @Override
    public StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
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
            if (transaction == null) {
                try {
                    autoFlush();
                } catch (DataStorageManagerException error) {
                    LOGGER.log(Level.SEVERE, "Error during auto-flush: " + error, error);
                    throw new StatementExecutionException(error);
                }
            }
        }
        throw new StatementExecutionException("unsupported statement " + statement);

    }

    private void unloadCleanPages(int count) {

        /* Do not unload pages if not really requested */
        if (count < 0) {
            return;
        }

        List<Long> pagesToUnload = new ArrayList<>();
        for (Long loadedPage : loadedPages) {
            if (!dirtyPages.contains(loadedPage)) {
                pagesToUnload.add(loadedPage);
                if (count-- <= 0) {
                    break;
                }
            }
        }
        if (pagesToUnload.isEmpty()) {
            return;
        }
        LOGGER.log(Level.SEVERE, "table " + table.name + ", unloading " + pagesToUnload + " pages");
        for (Long pageId : pagesToUnload) {
            unloadPage(pageId);
        }
    }

    private void unloadPage(Long pageId) {
        if (!dirtyPages.contains(pageId)) {
            Iterable<Bytes> keys = this.keyToPage.getKeysMappedToPage(pageId);
            LOGGER.log(Level.SEVERE, "table " + table.name + " unloadpage " + pageId);
            for (Bytes key : keys) {
                buffer.remove(key);
            }
            loadedPages.remove(pageId);
        }
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
        }, transaction, true);

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
        }, transaction, true);
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
            flush();
            return new DMLStatementExecutionResult(0, estimatedSize > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) estimatedSize, null, null);
        } catch (LogNotAvailableException error) {
            throw new StatementExecutionException(error);
        }
    }

    private void applyTruncate() throws DataStorageManagerException {
        pagesLock.lock();
        try {
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
            dirtyPages.clear();
            dirtyRecords.set(0);
            activePages.clear();
            buffer.clear();
            deletedKeys.clear();
            locksManager.clear();
            keyToPage.truncate();
            if (indexes != null) {
                for (AbstractIndexManager index : indexes.values()) {
                    index.truncate();
                }
            }
        } finally {
            pagesLock.unlock();
        }
    }

    private void ensurePageLoadedOnApply(Bytes key, boolean recovery) throws DataStorageManagerException {
        Long pageId = keyToPage.get(key);
        if (pageId != null && !Objects.equals(NEW_PAGE, pageId)) {
            loadPageToMemory(pageId, recovery);
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
                ensurePageLoadedOnApply(record.key, recovery);
                applyInsert(record.key, record.value);
            }
        }
        if (changedRecords != null) {
            for (Record r : changedRecords.values()) {
                ensurePageLoadedOnApply(r.key, recovery);
                applyUpdate(r.key, r.value);
            }
        }
        Set<Bytes> deletedRecords = transaction.deletedRecords.get(table.name);
        if (deletedRecords != null) {
            for (Bytes key : deletedRecords) {
                ensurePageLoadedOnApply(key, recovery);
                applyDelete(key);
            }
        }
        transaction.releaseLocksOnTable(table.name, locksManager);

        if (forceFlushTableData) {
            LOGGER.log(Level.SEVERE, "forcing local checkpoint, table " + table.name + " will be visible to all transactions now");
            checkpoint(log.getLastSequenceNumber(), true);
        }
    }

    @Override
    public void onTransactionRollback(Transaction transaction) {
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    @Override
    public void apply(LogSequenceNumber position, LogEntry entry, boolean recovery) throws DataStorageManagerException {
        if (recovery && !position.after(bootSequenceNumber)) {
            Transaction transaction = null;
            if (entry.transactionId > 0) {
                transaction = tableSpaceManager.getTransaction(entry.transactionId);
            }
            if (transaction != null) {
                LOGGER.log(Level.FINER, "{0}.{1} keep{2} at {3}, table booted at {4}, it belongs to transaction {5} which was in progress during the flush of the table", new Object[]{table.tablespace, table.name, entry, position, bootSequenceNumber, entry.transactionId});
            } else {
                LOGGER.log(Level.FINER, "{0}.{1} skip {2} at {3}, table booted at {4}", new Object[]{table.tablespace, table.name, entry, position, bootSequenceNumber});
                return;
            }
        }
        switch (entry.type) {
            case LogEntryType.DELETE: {
                // remove the record from the set of existing records
                Bytes key = new Bytes(entry.key);
                if (recovery) {
                    ensurePageLoadedOnApply(key, recovery);
                }
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
                if (recovery) {
                    ensurePageLoadedOnApply(key, recovery);
                }
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
                if (recovery) {
                    ensurePageLoadedOnApply(key, recovery);
                }
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
        Record record = buffer.remove(key);
        if (!NEW_PAGE.equals(pageId)) {
            dirtyPages.add(pageId);
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
        Record previous = buffer.put(key, newRecord);
        if (!NEW_PAGE.equals(pageId)) {
            dirtyPages.add(pageId);
        }
        dirtyRecords.incrementAndGet();

        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
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
        Consumer<Map.Entry<Bytes, Long>> scanExecutor = (Map.Entry<Bytes, Long> entry) -> {
            Bytes key = entry.getKey();
            LockHandle lock = lockForRead(key, null);
            try {
                Long pageId = entry.getValue();
                if (pageId != null) {
                    Record record = fetchRecord(key, pageId);
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
    public void dump(Consumer<Record> records) throws DataStorageManagerException {

        dataStorageManager.fullTableScan(tableSpaceUUID, table.name, new FullTableScanConsumer() {
            @Override
            public void acceptTableStatus(TableStatus tableStatus) {

            }

            @Override
            public void startPage(long pageId) {
            }

            @Override
            public void acceptRecord(Record record) {
                records.accept(record);
            }

            @Override
            public void endPage() {

            }
        });
    }

    void writeFromDump(List<Record> record) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, table.name + " received " + record.size() + " records");
        for (Record r : record) {
            applyInsert(r.key, r.value);
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
                dirtyPages.add(pageId);
            }
        }
        Record record = new Record(key, value);
        buffer.put(key, record);
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

    private void autoFlush() throws DataStorageManagerException {
        if (checkPointRunning) {
            return;
        }
        int dirtyNow = dirtyRecords.get();
        if (dirtyNow >= MAX_DIRTY_RECORDS) {
            long now = System.currentTimeMillis();
            pagesLock.lock();
            try {
                dirtyNow = dirtyRecords.get();
                if (dirtyNow < MAX_DIRTY_RECORDS) {
                    return;
                }
                LOGGER.log(Level.INFO, "autoflush - dirtyRecords {0}", dirtyNow);
                flush();
            } finally {
                pagesLock.unlock();
            }
            lastAutoFlush.set(now);
        }
    }

    @Override
    public void flush() throws DataStorageManagerException {
        checkpoint(log.getLastSequenceNumber(), true);
    }

    @Override
    public void close() {
        dataStorageManager.releaseKeyToPageMap(tableSpaceUUID, table.name, keyToPage);
    }

    private StatementExecutionResult executeGet(GetStatement get, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        Bytes key = new Bytes(get.getKey().computeNewValue(null, context, tableContext));
        Predicate predicate = get.getPredicate();
        long transactionId = transaction != null ? transaction.transactionId : 0;
        LockHandle lock = lockForRead(key, transaction);
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
            // fastest path first, check if the record is loaded in memory
            Record loaded = buffer.get(key);
            if (loaded != null) {
                if (predicate != null && !predicate.evaluate(loaded, context)) {
                    return GetResult.NOT_FOUND(transactionId);
                }
                return new GetResult(transactionId, loaded, table);
            }
            Long pageId = keyToPage.get(key);
            if (pageId == null) {
                return GetResult.NOT_FOUND(transactionId);
            }
            loaded = fetchRecord(key, pageId);
            if (loaded == null || (predicate != null && !predicate.evaluate(loaded, context))) {
                return GetResult.NOT_FOUND(transactionId);
            }
            return new GetResult(transactionId, loaded, table);

        } finally {
            if (transaction == null) {
                locksManager.releaseReadLockForKey(key, lock);
            }
        }
    }

    private void loadPageToMemory(Long pageId, boolean recovery) throws DataStorageManagerException {
        pagesLock.lock();
        try {
            if (loadedPages.contains(pageId)) {
                return;
            }
            if (dirtyPages.contains(pageId)) {
                throw new DataStorageManagerException("table " + table.tablespace + "." + table.name + ", page " + pageId + " is marked as dirty, it cannot be loaded from disk, dirtyPages " + dirtyPages + ", active " + activePages);
            }
            if (!activePages.contains(pageId)) {
                LOGGER.log(Level.SEVERE, "{0}.{1}: page {2} is no more active. it cannot be loaded from disk", new Object[]{table.tablespace, table.name, pageId});
                return;
            }
            int to_unload = loadedPages.size() - MAX_LOADED_PAGES;
            if (to_unload > 0) {
                unloadCleanPages(to_unload + UNLOAD_PAGES_MIN_BATCH);
            }
            try {
                long _start = System.currentTimeMillis();
                List<Record> page = dataStorageManager.readPage(tableSpaceUUID, table.name, pageId);
                long _stopDisk = System.currentTimeMillis();
                loadedPages.add(pageId);
                for (Record r : page) {
                    Long actualPage = keyToPage.get(r.key);
                    if (actualPage == null || !actualPage.equals(pageId)) {
                        LOGGER.log(Level.SEVERE, "table " + table.name + ", activePages " + activePages + ", dirtyPages " + dirtyPages);
                        throw new DataStorageManagerException("inconsistency at page " + pageId + ": key " + r.key + " is mapped to page " + actualPage + ", not to " + pageId);
                    }
                    buffer.put(r.key, r);
                }
                long _stopBuffer = System.currentTimeMillis();
                LOGGER.log(Level.SEVERE, "table " + table.name + ", loaded " + page.size() + " records from page " + pageId + " in " + (_stopBuffer - _start) + " ms (" + (_stopDisk - _start) + " ms disk, " + (_stopBuffer - _stopDisk) + " ms mem)");
            } catch (DataStorageManagerException error) {
                LOGGER.log(Level.SEVERE, "table " + table.name + ", error loading page " + pageId + ", active pages " + activePages + ", dirtyPages " + dirtyPages, error);
                throw new DataStorageManagerException("table " + table.name + ", error loading page " + pageId + ", active pages " + activePages + ", dirtyPages " + dirtyPages, error);
            }
        } finally {
            pagesLock.unlock();
        }

    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber,
        boolean executeActions) throws DataStorageManagerException {
        if (createdInTransaction > 0) {
            LOGGER.log(Level.SEVERE, "checkpoint for table " + table.name + " skipped, this table is created on transaction " + createdInTransaction + " which is not committed");
            return Collections.emptyList();
        }
        long start = System.currentTimeMillis();
        List<PostCheckpointAction> result = new ArrayList<>();
        pagesLock.lock();
        try {
            checkPointRunning = true;
            /*
                When the size of loaded data in the memory reaches a maximum value the rows on memory are dumped back to disk creating new pages
                for each page:
                if the page is not changed it is only unloaded from memory
                if the page contains even only one single changed row all the rows to the page will be  scheduled in order to create a new page
                rows scheduled to create a new page are arranged in a new set of pages which in turn are dumped to disk
             */
            List<Bytes> recordsOnDirtyPages = new ArrayList<>();
            LOGGER.log(Level.INFO, "checkpoint {0}, flush dirtyPages, {1} pages, logpos {2}", new Object[]{table.name, dirtyPages.toString(), sequenceNumber});
            for (Bytes key : buffer.keySet()) {
                Long pageId = keyToPage.get(key);
                if (dirtyPages.contains(pageId)
                    || Objects.equals(pageId, NEW_PAGE)) {
                    recordsOnDirtyPages.add(key);
                }
            }

//            // this is debug, use only for tests
//            Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(null,
//                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), tableContext, null);
//            scanner.forEach((Map.Entry<Bytes, Long> recordToPage) -> {
//                Long pageId = recordToPage.getValue();
//                if (dirtyPages.contains(pageId)) {
//                    Bytes key = recordToPage.getKey();
//                    if (!buffer.containsKey(key)) {
//                        LOGGER.log(Level.SEVERE, "table " + table.name + " found unloaded record key " + key + " on dirty page " + pageId);
//                    }
//                }
//            });
            LOGGER.log(Level.INFO, "flush {0} recordsOnDirtyPages, {1} records", new Object[]{table.name, recordsOnDirtyPages.size()});
            List<Record> newPage = new ArrayList<>();
            long newPageSize = 0;
            for (Bytes key : recordsOnDirtyPages) {
                Record toKeep = buffer.get(key);
                if (toKeep != null) {
                    newPage.add(toKeep);
                    newPageSize += key.data.length + toKeep.value.data.length;
                    if (newPageSize >= maxLogicalPageSize) {
                        createNewPage(newPage, newPageSize);
                        newPageSize = 0;
                        newPage.clear();
                    }
                }

            }
            if (!newPage.isEmpty()) {
                createNewPage(newPage, newPageSize);
            }

            activePages.removeAll(dirtyPages);
            dirtyPages.clear();
            dirtyRecords.set(0);

            TableStatus tableStatus = new TableStatus(table.name, sequenceNumber, Bytes.from_long(nextPrimaryKeyValue.get()).data, newPageId.get(), activePages);
            List<PostCheckpointAction> actions = dataStorageManager.tableCheckpoint(tableSpaceUUID, table.name, tableStatus);
            result.addAll(actions);
            LOGGER.log(Level.INFO, "checkpoint {0} finished, now activePages {1}, dirty {2}, loaded {3}", new Object[]{table.name, activePages + "", dirtyPages + "", loadedPages + ""});
            if (executeActions) {
                // need to execute actions inline
                for (PostCheckpointAction pa : result) {
                    pa.run();
                }
                result.clear();
            }
            checkPointRunning = false;
            unloadCleanPages(loadedPages.size() - MAX_LOADED_PAGES - 1);
        } finally {
            pagesLock.unlock();
        }
        long end = System.currentTimeMillis();
        long delta = end - start;
        if (delta > 5000) {
            LOGGER.log(Level.INFO, "long checkpoint for {0}, time {1}", new Object[]{table.name, delta + " ms"});
        }
        return result;
    }

    private long createNewPage(List<Record> newPage, long newPageSize) throws DataStorageManagerException {
        long pageId = this.newPageId.getAndIncrement();
        LOGGER.log(Level.SEVERE, "createNewPage table {0}, pageId={1} with {2} records, {3} logical page size", new Object[]{table.name, pageId, newPage.size(), newPageSize});
        dataStorageManager.writePage(tableSpaceUUID, table.name, pageId, newPage);
        activePages.add(pageId);
        loadedPages.add(pageId);
        for (Record record : newPage) {
            keyToPage.put(record.key, pageId);
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
        if (limits != null && limits.getMaxRows() > 0 && !sorted) {
            // if no sort is present the limits can be applying during the scan and perform an early exit
            AtomicInteger remaining = new AtomicInteger(limits.getMaxRows());

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
            }, transaction, false);
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
            }, transaction, false);
        }
        recordSet.writeFinished();
        recordSet.sort(statement.getComparator());
        recordSet.applyLimits(statement.getLimits());
        if (!applyProjectionDuringScan) {
            recordSet.applyProjection(statement.getProjection(), context);
        }
        return new SimpleDataScanner(transaction != null ? transaction.transactionId : 0, recordSet);
    }

    private void accessTableData(ScanStatement statement, StatementEvaluationContext context, ScanResultOperation consumer, Transaction transaction, boolean forWrite) throws StatementExecutionException {
        Predicate predicate = statement.getPredicate();
        long _start = System.currentTimeMillis();

        try {
            IndexOperation indexOperation = predicate != null ? predicate.getIndexOperation() : null;
            Consumer<Map.Entry<Bytes, Long>> scanExecutor = new Consumer<Map.Entry<Bytes, Long>>() {
                @Override
                public void accept(Map.Entry<Bytes, Long> entry) {

                    Bytes key = entry.getKey();
                    boolean keep_lock = false;
                    boolean already_locked = transaction != null && transaction.lookupLock(table.name, key) != null;
                    LockHandle lock = forWrite ? lockForWrite(key, transaction) : lockForRead(key, transaction);
                    try {
                        if (transaction != null) {
                            if (transaction.recordDeleted(table.name, key)) {
                                // skip this record. inside current transaction it has been deleted
                                return;
                            }
                            Record record = transaction.recordUpdated(table.name, key);
                            if (record != null) {
                                // use current transaction version of the record
                                if (predicate == null || predicate.evaluate(record, context)) {
                                    consumer.accept(record);
                                    keep_lock = true;
                                }
                                return;
                            }
                        }
                        Long pageId = entry.getValue();
                        if (pageId != null) {
                            Record record = fetchRecord(key, pageId);
                            if (record != null && (predicate == null || predicate.evaluate(record, context))) {
                                consumer.accept(record);
                                keep_lock = true;
                            }
                        }
                    } catch (DataStorageManagerException | StatementExecutionException | LogNotAvailableException error) {
                        throw new RuntimeException(error);
                    } finally {
                        // release the lock on the key if it did not match scan criteria
                        if (transaction == null) {
                            if (forWrite) {
                                locksManager.releaseWriteLockForKey(key, lock);
                            } else {
                                locksManager.releaseReadLockForKey(key, lock);
                            }
                        } else if (!keep_lock && !already_locked) {
                            transaction.releaseLockOnKey(table.name, key, locksManager);
                        }
                    }
                }
            };

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

            Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(indexOperation, context, tableContext, useIndex);
            boolean exit = false;
            try {
                scanner.forEachOrdered(scanExecutor);
            } catch (ExitLoop exitLoop) {
                exit = true;
                LOGGER.log(Level.SEVERE, "exit loop during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), exitLoop.toString()});
            } catch (final Exception error) {
                LOGGER.log(Level.SEVERE, "error during scan", error);
                if (error.getCause() instanceof StatementExecutionException) {
                    throw (StatementExecutionException) error.getCause();
                } else if (error.getCause() instanceof DataStorageManagerException) {
                    throw (DataStorageManagerException) error.getCause();
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

    private Record fetchRecord(Bytes key, Long pageId) throws StatementExecutionException, DataStorageManagerException {
        Record record = buffer.get(key);
        int maxTrials = 10_000;
        while (record == null) {
            Long relocatedPageId = keyToPage.get(key);
            LOGGER.log(Level.SEVERE, table.name + " fetchRecord " + key + " failed, checkPointRunning:" + checkPointRunning + " pageId:" + pageId + " relocatedPageId:" + relocatedPageId);
            if (relocatedPageId == null) {
                // deleted
                LOGGER.log(Level.SEVERE, "table " + table.name + ", activePages " + activePages + ", record " + key + " deleted during data access");
                return null;
            }
            pageId = relocatedPageId;
            if (!Objects.equals(pageId, NEW_PAGE)) {
                // BEWARE that loading a page into memory can cause other pages to be unloaded
                loadPageToMemory(pageId, false);
            } else {
                throw new DataStorageManagerException("inconsistency! table " + table.name + " no record in memory for " + key + " page " + pageId + ", activePages " + activePages);
            }
            record = buffer.get(key);
            if (maxTrials-- == 0) {
                throw new DataStorageManagerException("inconsistency! table " + table.name + " no record in memory for " + key + " page " + pageId + ", activePages " + activePages + " after many trials");
            }
        }
        return record;
    }

    private final TableManagerStats stats = new TableManagerStats() {
        @Override
        public int getLoadedpages() {
            return loadedPages.size();
        }

        @Override
        public long getTablesize() {
            return keyToPage.size();
        }

        @Override
        public int getMaxloadedpages() {
            return MAX_LOADED_PAGES;
        }

        @Override
        public int getDirtypages() {
            return dirtyPages.size();
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
        public long getLastAutoFlushTs() {
            return lastAutoFlush.longValue();
        }

    };

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
            for (Record record : buffer.values()) {
                // table structure changed
                record.clearCache();
            }
        }
    }

    @Override
    public long getCreatedInTransaction() {
        return createdInTransaction;
    }

}
