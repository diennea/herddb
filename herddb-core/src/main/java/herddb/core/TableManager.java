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
import herddb.model.commands.UpdateStatement;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 */
public class TableManager {

    private static final Logger LOGGER = Logger.getLogger(TableManager.class.getName());

    private static final int MAX_RECORDS_PER_PAGE = 100;

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

    /**
     * Local locks
     */
    private final LocalLockManager locksManager = new LocalLockManager();

    /**
     * Access to Pages
     */
    private ReentrantReadWriteLock pagesLock = new ReentrantReadWriteLock(true);

    /**
     * Definition of the table
     */
    private Table table;
    private final CommitLog log;
    private final DataStorageManager dataStorageManager;

    TableManager(Table table, CommitLog log, DataStorageManager dataStorageManager) {
        this.table = table;
        this.log = log;
        this.dataStorageManager = dataStorageManager;
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

    StatementExecutionResult executeStatement(Statement statement, Transaction transaction) throws StatementExecutionException {
        pagesLock.readLock().lock();
        try {
            if (statement instanceof UpdateStatement) {
                UpdateStatement update = (UpdateStatement) statement;
                return executeUpdate(update, transaction);
            }
            if (statement instanceof InsertStatement) {
                InsertStatement insert = (InsertStatement) statement;
                return executeInsert(insert, transaction);
            }
            if (statement instanceof GetStatement) {
                GetStatement get = (GetStatement) statement;
                return executeGet(get, transaction);
            }
            if (statement instanceof DeleteStatement) {
                DeleteStatement delete = (DeleteStatement) statement;
                return executeDelete(delete, transaction);
            }
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException("internal data error", err);
        } finally {
            pagesLock.readLock().unlock();
        }
        throw new StatementExecutionException("unsupported statement " + statement);
    }

    private StatementExecutionResult executeInsert(InsertStatement insert, Transaction transaction) throws StatementExecutionException {
        /*
            an insert can succeed only if the row is valid and the "keys" structure  does not contain the requested key
            the insert will add the row in the 'buffer' without assigning a page to it
            locks: the insert uses global 'insert' lock on the table
            the insert will update the 'maxKey' for auto_increment primary keys
         */
        Record record = insert.getRecord();
        Bytes key = record.key;
        LockHandle lock = locksManager.acquireWriteLockForKey(key);
        if (transaction != null) {
            transaction.registerLockOnTable(this.table.name, lock);
        }
        try {
            if (keyToPage.containsKey(key)) {
                throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
            }
            LogEntry entry = LogEntryFactory.insert(table, record.key.data, record.value.data, transaction);
            log.log(entry);
            apply(entry);

            if (transaction != null) {
                transaction.registerInsertOnTable(table.name, key);
            }

            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (transaction == null) {
                locksManager.releaseWriteLockForKey(key, lock);
            }
        }
    }

    private StatementExecutionResult executeUpdate(UpdateStatement update, Transaction transaction) throws StatementExecutionException, DataStorageManagerException {
        /*
              an update can succeed only if the row is valid, the key is contains in the "keys" structure
              the update will simply override the value of the row, assigning a null page to the row
              the update can have a 'where' predicate which is to be evaluated against the decoded row, the update will be executed only if the predicate returns boolean 'true' value  (CAS operation)
              locks: the update  uses a lock on the the key
         */
        RecordFunction function = update.getFunction();

        Predicate predicate = update.getPredicate();
        Bytes key = update.getKey();
        LockHandle lock = locksManager.acquireWriteLockForKey(key);
        if (transaction != null) {
            transaction.registerLockOnTable(this.table.name, lock);
        }
        try {
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
            if (predicate != null && !predicate.evaluate(actual)) {
                // record does not match predicate
                return new DMLStatementExecutionResult(0, key);
            }
            if (transaction != null) {
                transaction.registerChangedOnTable(this.table.name, actual);
            }

            byte[] newValue = function.computeNewValue(actual);
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

    private StatementExecutionResult executeDelete(DeleteStatement delete, Transaction transaction) throws StatementExecutionException, DataStorageManagerException {
        /*
                  a delete can succeed only if the key is contains in the 'keys" structure
                  a delete will remove the key from each of the structures
                  locks: the delete uses a lock on the the key
                  the delete can have a 'where' predicate which is to be evaluated against the decoded row, the delete  will be executed only if the predicate returns boolean 'true' value  (CAS operation)
         */
        Bytes key = delete.getKey();
        LockHandle lock = locksManager.acquireWriteLockForKey(key);
        if (transaction != null) {
            transaction.registerLockOnTable(this.table.name, lock);
        }
        try {
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
            } else if (transaction != null) {
                transaction.registerChangedOnTable(this.table.name, actual);
            }
            if (delete.getPredicate() != null && !delete.getPredicate().evaluate(actual)) {
                // record does not match predicate
                return new DMLStatementExecutionResult(0, key);
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

    void onTransactionCommit(Transaction transaction) {
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    void onTransactionRollback(Transaction transaction) {
        List<Record> changedRecords = transaction.getChangedRecordsForTable(table.name);
        // transaction is still holding locks on each record, so we can change records
        if (changedRecords != null) {
            for (Record r : changedRecords) {
                buffer.put(r.key, r);
                Long pageId = keyToPage.put(r.key, NO_PAGE);
                if (pageId != null) {
                    dirtyPages.add(pageId);
                }
            }
        }
        List<Bytes> newRecords = transaction.getNewRecordsForTable(table.name);
        if (newRecords != null) {
            for (Bytes key : newRecords) {
                buffer.remove(key);
                keyToPage.remove(key);
            }
        }
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    private void apply(LogEntry entry) {
        switch (entry.type) {
            case LogEntryType.DELETE: {
                // remove the record from the set of existing records
                Bytes key = new Bytes(entry.key);
                Long pageId = keyToPage.remove(key);
                deletedKeys.add(key);
                buffer.remove(key);
                dirtyPages.add(pageId);
                break;
            }
            case LogEntryType.UPDATE: {
                // mark record as dirty
                Bytes key = new Bytes(entry.key);
                Bytes value = new Bytes(entry.value);
                Long pageId = keyToPage.put(key, NO_PAGE);
                buffer.put(key, new Record(key, value));
                dirtyPages.add(pageId);
                break;
            }
            case LogEntryType.INSERT: {
                Bytes key = new Bytes(entry.key);
                Bytes value = new Bytes(entry.value);
                keyToPage.put(key, NO_PAGE);
                buffer.put(key, new Record(key, value));
                deletedKeys.remove(key);
                break;
            }
            default:
                throw new IllegalArgumentException("unhandled entry type " + entry.type);
        }
    }

    void close() {
        // TODO
    }

    private StatementExecutionResult executeGet(GetStatement get, Transaction transaction) throws StatementExecutionException, DataStorageManagerException {
        Bytes key = get.getKey();
        Predicate predicate = get.getPredicate();
        LockHandle lock = locksManager.acquireReadLockForKey(key);
        if (transaction != null) {
            transaction.registerLockOnTable(this.table.name, lock);
        }
        try {
            // fastest path first, check if the record is loaded in memory
            Record loaded = buffer.get(key);
            if (loaded != null) {
                if (predicate != null && !predicate.evaluate(loaded)) {
                    return GetResult.NOT_FOUND;
                }
                return new GetResult(loaded);
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
            if (predicate != null && !predicate.evaluate(loaded)) {
                return GetResult.NOT_FOUND;
            }
            return new GetResult(loaded);
        } finally {
            if (transaction == null) {
                locksManager.releaseReadLockForKey(key, lock);
            }
        }
    }

    private void ensurePageLoaded(Long pageId) throws DataStorageManagerException {
        pagesLock.readLock().unlock();
        pagesLock.writeLock().lock();
        try {
            if (loadedPages.contains(pageId)) {
                throw new RuntimeException("corrupted state, page " + pageId + " should already be loaded in memory");
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
            LOGGER.log(Level.SEVERE, "flush dirtyPages {0}", new Object[]{dirtyPages.toString()});
            for (Bytes key : buffer.keySet()) {
                Long pageId = keyToPage.get(key);
                if (dirtyPages.contains(pageId)
                        || pageId == NO_PAGE // using the '==' because we really use the reference
                        ) {
                    recordsOnDirtyPages.add(key);
                }
            }
            LOGGER.log(Level.SEVERE, "flush recordsOnDirtyPages {0}", new Object[]{recordsOnDirtyPages.toString()});
            List<Record> newPage = new ArrayList<>();
            int count = 0;
            for (Bytes key : recordsOnDirtyPages) {
                Record toKeep = buffer.get(key);
                if (toKeep != null) {
                    newPage.add(toKeep);
                    if (count++ == MAX_RECORDS_PER_PAGE) {
                        createNewPage(sequenceNumber, newPage);
                        newPage.clear();
                    }
                }

            }
            if (!newPage.isEmpty()) {
                createNewPage(sequenceNumber, newPage);
            }
            buffer.clear();
            loadedPages.clear();
            dirtyPages.clear();
        } finally {
            pagesLock.writeLock().unlock();
        }

    }

    private void createNewPage(LogSequenceNumber sequenceNumber, List<Record> newPage) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "createNewPage at " + sequenceNumber + " with " + newPage);
        Long newPageId = dataStorageManager.writePage(table.name, sequenceNumber, newPage);
        for (Record record : newPage) {
            keyToPage.put(record.key, newPageId);
        }
    }

}
