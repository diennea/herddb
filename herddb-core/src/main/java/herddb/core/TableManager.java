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

import herddb.core.stats.TableManagerStats;
import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Predicate;
import herddb.model.RecordFunction;
import herddb.model.commands.InsertStatement;
import herddb.model.Record;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.StatementEvaluationContext;
import herddb.model.Tuple;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 */
public class TableManager {

    private static final Logger LOGGER = Logger.getLogger(TableManager.class.getName());

    private static final int MAX_RECORDS_PER_PAGE = 100;

    private static final int MAX_LOADED_PAGES = 100;

    private static final int MAX_DIRTY_RECORDS = 1000;

    private static final int UNLOAD_PAGES_MIN_BATCH = 10;

    public static final Long NO_PAGE = Long.valueOf(-1);

    /**
     * a buffer which contains the rows contained into the loaded pages
     * (map<byte[],byte[]>)
     */
    private final Map<Bytes, Record> buffer = new ConcurrentHashMap<>();

    /**
     * keyToPage: a structure which maps each key to the ID of the page
     * (map<byte[], long>) (this can be quite large)
     */
    private final Map<Bytes, Long> keyToPage = new ConcurrentHashMap<>();

    /**
     * Keys deleted since the last flush
     */
    private final Set<Bytes> deletedKeys = new ConcurrentSkipListSet<>();

    /**
     * a structure which holds the set of the pages which are loaded in memory
     * (set<long>)
     */
    private final Set<Long> loadedPages = new HashSet<>();

    private final Set<Long> dirtyPages = new ConcurrentSkipListSet<>();

    private final AtomicInteger dirtyRecords = new AtomicInteger();

    /**
     * Local locks
     */
    private final LocalLockManager locksManager = new LocalLockManager();

    /**
     * Access to Pages
     */
    private final ReentrantReadWriteLock pagesLock = new ReentrantReadWriteLock(true);

    /**
     * Definition of the table
     */
    private Table table;
    private final CommitLog log;
    private final DataStorageManager dataStorageManager;
    private final TableSpaceManager tableSpaceManager;

    TableManager(Table table, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager) {
        this.table = table;
        this.tableSpaceManager = tableSpaceManager;
        this.log = log;
        this.dataStorageManager = dataStorageManager;
    }

    public Table getTable() {
        return table;
    }

    public void start() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "loading in memory all the keys for table {1}", new Object[]{keyToPage.size(), table.name});
        pagesLock.writeLock().lock();
        try {
            dataStorageManager.loadExistingKeys(table.name, (key, pageId) -> {
                keyToPage.put(key, pageId);
            });
        } finally {
            pagesLock.writeLock().unlock();
        }
        LOGGER.log(Level.SEVERE, "loaded {0} keys for table {1}", new Object[]{keyToPage.size(), table.name});
    }

    StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        pagesLock.readLock().lock();
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
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException("internal data error", err);
        } finally {
            pagesLock.readLock().unlock();
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
        LOGGER.log(Level.SEVERE, "unloading " + pagesToUnload + " pages");
        pagesLock.writeLock().lock();
        for (Long pageId : pagesToUnload) {
            unloadPage(pageId);
        }
    }

    private void unloadPage(Long pageId) {
        List<Bytes> keys = this.keyToPage.entrySet().stream().filter(entry -> pageId.equals(entry.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        LOGGER.log(Level.SEVERE, "unloadpage " + pageId + ", " + keys.size() + " records");
        for (Bytes key : keys) {
            buffer.remove(key);
        }
        loadedPages.remove(pageId);
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
        Bytes key = new Bytes(insert.getKeyFunction().computeNewValue(null, context));
        byte[] value = insert.getValuesFunction().computeNewValue(new Record(key, null), context);

        LockHandle lock = lockForWrite(key, transaction);
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    // OK, INSERT on a DELETED record inside this transaction
                } else if (transaction.recordInserted(table.name, key) != null) {
                    // ERROR, INSERT on a INSERTED record inside this transaction
                    throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
                }
            }
            if (keyToPage.containsKey(key)) {
                throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
            }
            LogEntry entry = LogEntryFactory.insert(table, key.data, value, transaction);
            log.log(entry);
            apply(entry);
            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (transaction == null) {
                locksManager.releaseWriteLockForKey(key, lock);
            }
        }
    }

    private StatementExecutionResult executeUpdate(UpdateStatement update, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        /*
              an update can succeed only if the row is valid, the key is contains in the "keys" structure
              the update will simply override the value of the row, assigning a null page to the row
              the update can have a 'where' predicate which is to be evaluated against the decoded row, the update will be executed only if the predicate returns boolean 'true' value  (CAS operation)
              locks: the update  uses a lock on the the key
         */
        RecordFunction function = update.getFunction();

        Predicate predicate = update.getPredicate();
        Bytes key = new Bytes(update.getKey().computeNewValue(null, context));
        LockHandle lock = lockForWrite(key, transaction);
        try {
            byte[] newValue;
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    // UPDATE on a deleted record
                    return new DMLStatementExecutionResult(0, key);
                }
                // UPDATE on a updated record
                Record actual = transaction.recordUpdated(table.name, key);
                if (actual == null) {
                    // UPDATE on a inserted record
                    actual = transaction.recordInserted(table.name, key);
                }
                if (actual != null) {
                    if (predicate != null && !predicate.evaluate(actual, context)) {
                        // record does not match predicate
                        return new DMLStatementExecutionResult(0, key);
                    }
                    newValue = function.computeNewValue(actual, context);
                } else {
                    // update on a untouched record by this transaction
                    Long pageId = keyToPage.get(key);
                    if (pageId == null) {
                        // no record at that key
                        return new DMLStatementExecutionResult(0, key);
                    }
                    actual = buffer.get(key);
                    if (actual == null) {
                        ensurePageLoaded(pageId);
                        actual = buffer.get(key);
                    }
                    if (predicate != null && !predicate.evaluate(actual, context)) {
                        // record does not match predicate
                        return new DMLStatementExecutionResult(0, key);
                    }
                    newValue = function.computeNewValue(actual, context);
                }
            } else {
                Long pageId = keyToPage.get(key);
                if (pageId == null) {
                    // no record at that key
                    return new DMLStatementExecutionResult(0, key);
                }
                Record actual = buffer.get(key);
                if (actual == null) {
                    ensurePageLoaded(pageId);
                    actual = buffer.get(key);
                }
                if (predicate != null && !predicate.evaluate(actual, context)) {
                    // record does not match predicate
                    return new DMLStatementExecutionResult(0, key);
                }
                newValue = function.computeNewValue(actual, context);
            }
            if (newValue == null) {
                throw new NullPointerException("new value cannot be null");
            }
            LogEntry entry = LogEntryFactory.update(table, key.data, newValue, transaction);
            log.log(entry);

            apply(entry);

            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (transaction == null) {
                locksManager.releaseWriteLockForKey(key, lock);
            }
        }
    }

    private StatementExecutionResult executeDelete(DeleteStatement delete, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        /*
                  a delete can succeed only if the key is contains in the 'keys" structure
                  a delete will remove the key from each of the structures
                  locks: the delete uses a lock on the the key
                  the delete can have a 'where' predicate which is to be evaluated against the decoded row, the delete  will be executed only if the predicate returns boolean 'true' value  (CAS operation)
         */
        Bytes key = new Bytes(delete.getKeyFunction().computeNewValue(null, context));
        LockHandle lock = lockForWrite(key, transaction);
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    // delete on a deleted record inside this transaction
                    return new DMLStatementExecutionResult(0, key);
                }

                // delete on a updated record inside this transaction
                Record actual = transaction.recordUpdated(table.name, key);
                if (actual == null) {
                    // delete on a inserted record inside this transaction
                    actual = transaction.recordInserted(table.name, key);
                }
                if (actual != null) {
                    if (delete.getPredicate() != null && !delete.getPredicate().evaluate(actual, context)) {
                        // record does not match predicate
                        return new DMLStatementExecutionResult(0, key);
                    }
                } else {
                    // matching a record untouched by the transaction till now
                    Long pageId = keyToPage.get(key);
                    if (pageId == null) {
                        // no record at that key
                        return new DMLStatementExecutionResult(0, key);
                    }
                    actual = buffer.get(key);
                    if (actual == null) {
                        // page always need to be loaded because the other records on that page will be rewritten on a new page
                        ensurePageLoaded(pageId);
                        actual = buffer.get(key);
                    }
                    if (delete.getPredicate() != null && !delete.getPredicate().evaluate(actual, context)) {
                        // record does not match predicate
                        return new DMLStatementExecutionResult(0, key);
                    }
                }
            } else {
                Long pageId = keyToPage.get(key);
                if (pageId == null) {
                    // no record at that key
                    return new DMLStatementExecutionResult(0, key);
                }
                Record actual = buffer.get(key);
                if (actual == null) {
                    // page always need to be loaded because the other records on that page will be rewritten on a new page
                    ensurePageLoaded(pageId);
                    actual = buffer.get(key);
                }
                if (delete.getPredicate() != null && !delete.getPredicate().evaluate(actual, context)) {
                    // record does not match predicate
                    return new DMLStatementExecutionResult(0, key);
                }
            }
            LogEntry entry = LogEntryFactory.delete(table, key.data, transaction);
            log.log(entry);
            apply(entry);
            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (transaction == null) {
                locksManager.releaseWriteLockForKey(key, lock);
            }
        }
    }

    void onTransactionCommit(Transaction transaction) throws DataStorageManagerException {
        List<Record> changedRecords = transaction.changedRecords.get(table.name);
        // transaction is still holding locks on each record, so we can change records
        if (changedRecords != null) {
            for (Record r : changedRecords) {
                applyUpdate(r.key, r.value);
            }
        }
        List<Record> newRecords = transaction.newRecords.get(table.name);
        if (newRecords != null) {
            for (Record record : newRecords) {
                applyInsert(record.key, record.value);
            }
        }
        List<Bytes> deletedRecords = transaction.deletedRecords.get(table.name);
        if (deletedRecords != null) {
            for (Bytes key : deletedRecords) {
                applyDelete(key);
            }
        }
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    void onTransactionRollback(Transaction transaction) {
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    void apply(LogEntry entry) throws DataStorageManagerException {
        switch (entry.type) {
            case LogEntryType.DELETE: {
                // remove the record from the set of existing records
                Bytes key = new Bytes(entry.key);
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    transaction.registerDeleteOnTable(this.table.name, key);
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
                    transaction.registerRecoredUpdate(this.table.name, key, value);
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
                    transaction.registerInsertOnTable(table.name, key, value);
                } else {
                    applyInsert(key, value);
                }
                break;
            }
            default:
                throw new IllegalArgumentException("unhandled entry type " + entry.type);
        }
    }

    private void applyDelete(Bytes key) {
        Long pageId = keyToPage.remove(key);
        deletedKeys.add(key);
        dirtyPages.add(pageId);
        buffer.remove(key);
        dirtyRecords.incrementAndGet();
    }

    private void applyUpdate(Bytes key, Bytes value) {
        Long pageId = keyToPage.put(key, NO_PAGE);
        buffer.put(key, new Record(key, value));
        dirtyPages.add(pageId);
        dirtyRecords.incrementAndGet();
    }

    private void applyInsert(Bytes key, Bytes value) {
        keyToPage.put(key, NO_PAGE);
        buffer.put(key, new Record(key, value));
        deletedKeys.remove(key);
        dirtyRecords.incrementAndGet();
    }

    private void autoFlush() throws DataStorageManagerException {
        if (dirtyRecords.get() >= MAX_DIRTY_RECORDS) {
            LOGGER.log(Level.SEVERE, "autoflush");
            flush();
        }
    }

    void close() {
        // TODO
    }

    private StatementExecutionResult executeGet(GetStatement get, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        Bytes key = new Bytes(get.getKey().computeNewValue(null, context));
        Predicate predicate = get.getPredicate();
        LockHandle lock = lockForRead(key, transaction);
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    return GetResult.NOT_FOUND;
                }
                Record loadedInTransaction = transaction.recordUpdated(table.name, key);
                if (loadedInTransaction != null) {
                    if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                        return GetResult.NOT_FOUND;
                    }
                    return new GetResult(loadedInTransaction, table);
                }
                loadedInTransaction = transaction.recordInserted(table.name, key);
                if (loadedInTransaction != null) {
                    if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                        return GetResult.NOT_FOUND;
                    }
                    return new GetResult(loadedInTransaction, table);
                }
            }
            // fastest path first, check if the record is loaded in memory
            Record loaded = buffer.get(key);
            if (loaded != null) {
                if (predicate != null && !predicate.evaluate(loaded, context)) {
                    return GetResult.NOT_FOUND;
                }
                return new GetResult(loaded, table);
            }
            Long pageId = keyToPage.get(key);
            if (pageId != null) {
                ensurePageLoaded(pageId);
            } else {
                return GetResult.NOT_FOUND;
            }
            loaded = buffer.get(key);
            if (loaded == null) {
                throw new StatementExecutionException("corrupted data, missing record " + key);
            }
            if (predicate != null && !predicate.evaluate(loaded, context)) {
                return GetResult.NOT_FOUND;
            }
            return new GetResult(loaded, table);

        } finally {
            if (transaction == null) {
                locksManager.releaseReadLockForKey(key, lock);
            }
        }
    }

    private void ensurePageLoaded(Long pageId) throws DataStorageManagerException {
        if (loadedPages.contains(pageId)) {
            return;
        }
        pagesLock.readLock().unlock();
        pagesLock.writeLock().lock();
        try {
            if (loadedPages.contains(pageId)) {
                return;
            }
            int to_unload = loadedPages.size() - MAX_LOADED_PAGES;
            if (to_unload > 0) {
                unloadCleanPages(to_unload + UNLOAD_PAGES_MIN_BATCH);
            }
            List<Record> page = dataStorageManager.loadPage(table.name, pageId);
            loadedPages.add(pageId);
            for (Record r : page) {
                buffer.put(r.key, r);
            }
        } finally {
            pagesLock.writeLock().unlock();
            pagesLock.readLock().lock();
        }
    }

    void flush() throws DataStorageManagerException {
        pagesLock.writeLock().lock();
        LogSequenceNumber sequenceNumber = log.getActualSequenceNumber();
        try {
            /*
                When the size of loaded data in the memory reaches a maximum value the rows on memory are dumped back to disk creating new pages
                for each page:
                if the page is not changed it is only unloaded from memory
                if the page contains even only one single changed row all the rows to the page will be  scheduled in order to create a new page
                rows scheduled to create a new page are arranged in a new set of pages which in turn are dumped to disk
             */
            List<Bytes> recordsOnDirtyPages = new ArrayList<>();
            LOGGER.log(Level.SEVERE, "flush dirtyPages, {0} pages", new Object[]{dirtyPages.toString()});
            for (Bytes key : buffer.keySet()) {
                Long pageId = keyToPage.get(key);
                if (dirtyPages.contains(pageId)
                        || Objects.equals(pageId, NO_PAGE)) {
                    recordsOnDirtyPages.add(key);
                }
            }
            LOGGER.log(Level.SEVERE, "flush recordsOnDirtyPages, {0} records", new Object[]{recordsOnDirtyPages.size()});
            List<Record> newPage = new ArrayList<>();
            int count = 0;
            for (Bytes key : recordsOnDirtyPages) {
                Record toKeep = buffer.get(key);
                if (toKeep != null) {
                    newPage.add(toKeep);
                    if (++count == MAX_RECORDS_PER_PAGE) {
                        createNewPage(sequenceNumber, newPage);
                        newPage.clear();
                        count = 0;
                    }
                }

            }
            if (!newPage.isEmpty()) {
                createNewPage(sequenceNumber, newPage);
            }
            buffer.clear();
            loadedPages.clear();
            dirtyPages.clear();
            dirtyRecords.set(0);
        } finally {
            pagesLock.writeLock().unlock();
        }

    }

    private void createNewPage(LogSequenceNumber sequenceNumber, List<Record> newPage) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "createNewPage at " + sequenceNumber + " with " + newPage.size() + " records");
        Long newPageId = dataStorageManager.writePage(table.name, sequenceNumber, newPage);
        for (Record record : newPage) {
            keyToPage.put(record.key, newPageId);
        }
    }

    private static class RecordSet {

        private List<Record> records = new ArrayList<>();
    }

    private class SimpleDataScanner extends DataScanner {

        final RecordSet keys;
        final Predicate predicate;
        final Iterator<Record> iterator;
        Tuple next;
        boolean finished;

        public SimpleDataScanner(Table table, RecordSet keys, ScanStatement statement) {
            super(table, statement);
            this.keys = keys;
            this.predicate = statement.getPredicate();
            this.iterator = keys.records.iterator();
        }

        @Override
        public void close() throws DataScannerException {
            finished = true;
        }

        @Override
        public boolean hasNext() throws DataScannerException {
            if (finished) {
                return false;
            }
            return ensureNext();
        }

        private boolean ensureNext() throws DataScannerException {
            if (next != null) {
                return true;
            }
            try {
                while (true) {
                    if (!iterator.hasNext()) {
                        finished = true;
                        return false;
                    }
                    Record record = iterator.next();
                    Tuple tuple = new Tuple(record.toBean(table));
                    next = scan.getProjection().map(tuple);
                    return true;
                    // RECORD does not match, iterate again
                }
            } catch (StatementExecutionException err) {
                throw new DataScannerException(err);
            }
        }

        @Override
        public Tuple next() throws DataScannerException {
            if (finished) {
                throw new DataScannerException("Scanner is exhausted");
            }
            Tuple _next = next;
            next = null;
            return _next;
        }

    }

    DataScanner scan(ScanStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        // TODO, support transactions
        Transaction transaction = null;
        Predicate predicate = statement.getPredicate();

        RecordSet recordSet = new RecordSet();
        // TODO: swap on disk the resultset
        try {
            if (predicate != null && predicate instanceof PrimaryKeyIndexSeekPredicate) {
                PrimaryKeyIndexSeekPredicate pred = (PrimaryKeyIndexSeekPredicate) predicate;
                GetResult getResult = (GetResult) executeGet(new GetStatement(table.tablespace, table.name, pred.key, null), transaction, context);
                if (getResult.found()) {
                    recordSet.records.add(getResult.getRecord());
                }
            } else {
                pagesLock.readLock().lock();
                try {
                    // TODO: swap on disk the resultset
                    for (Map.Entry<Bytes, Long> entry : keyToPage.entrySet()) {
                        Bytes key = entry.getKey();
                        LockHandle lock = lockForRead(key, transaction);
                        try {
                            Long pageId = entry.getValue();
                            if (pageId != null) { // record disappeared during the scan
                                Record record;
                                if (Objects.equals(pageId, NO_PAGE)) {
                                    record = buffer.get(key);
                                } else {
                                    ensurePageLoaded(pageId);
                                    record = buffer.get(key);
                                }
                                if (record == null) {
                                    throw new RuntimeException("inconsistency! no record in memory for " + entry.getKey() + " page " + pageId);
                                }
                                if (predicate == null || predicate.evaluate(record, context)) {
                                    recordSet.records.add(record);
                                }
                            }
                        } finally {
                            if (transaction == null) {
                                locksManager.releaseReadLockForKey(key, lock);
                            }
                        }
                    }
                } finally {
                    pagesLock.readLock().unlock();
                }
            }
            return new SimpleDataScanner(table, recordSet, statement);
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        }
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
        public int getMaxrecordsperpage() {
            return MAX_RECORDS_PER_PAGE;
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

    };

    public TableManagerStats getStats() {
        return stats;
    }

}
