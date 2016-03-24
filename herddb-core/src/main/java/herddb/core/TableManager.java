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
import herddb.log.LogNotAvailableException;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Predicate;
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
import herddb.utils.Bytes;
import herddb.utils.LocalLockManager;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 */
public class TableManager {

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
     * a structure which holds the set of the pages which are loaded in memory
     * (set<long>)
     */
    private final Set<Long> loadedPages = new HashSet<>();

    /**
     * Local locks
     */
    private final LocalLockManager locksManager = new LocalLockManager();

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

    public void start() {

    }

    StatementExecutionResult executeStatement(Statement statement, Transaction transaction) throws StatementExecutionException {
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
        ReentrantReadWriteLock lock = locksManager.acquireWriteLockForKey(key);
        try {
            if (keyToPage.containsKey(key)) {
                throw new DuplicatePrimaryKeyException(key, "key " + key + " already exists in table " + table.name);
            }
            LogEntry entry = LogEntryFactory.insert(table, record, transaction);
            log.log(entry);
            keyToPage.put(key, NO_PAGE);
            buffer.put(key, record);
            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            locksManager.releaseWriteLockForKey(key, lock);
        }
    }

    private StatementExecutionResult executeUpdate(UpdateStatement update, Transaction transaction) throws StatementExecutionException {
        /*
              an update can succeed only if the row is valid, the key is contains in the "keys" structure
              the update will simply override the value of the row, assigning a null page to the row
              the update can have a 'where' predicate which is to be evaluated against the decoded row, the update will be executed only if the predicate returns boolean 'true' value  (CAS operation)
              locks: the update  uses a lock on the the key
         */
        Record record = update.getRecord();
        Bytes key = record.key;
        ReentrantReadWriteLock lock = locksManager.acquireWriteLockForKey(key);
        try {
            if (!keyToPage.containsKey(key)) {
                // no record at that key
                return new DMLStatementExecutionResult(0, key);
            }
            if (update.getPredicate() != null) {
                Record actual = buffer.get(key);
                if (actual == null) {
                    ensureRecordLoaded(key);
                    actual = buffer.get(key);
                }
                if (!update.getPredicate().evaluate(actual)) {
                    // record does not match predicate
                    return new DMLStatementExecutionResult(0, key);
                }
            }

            LogEntry entry = LogEntryFactory.update(table, record, transaction);
            log.log(entry);

            // mark record as dirty
            keyToPage.put(key, NO_PAGE);
            buffer.put(key, record);
            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            locksManager.releaseWriteLockForKey(key, lock);
        }
    }

    private StatementExecutionResult executeDelete(DeleteStatement delete, Transaction transaction) throws StatementExecutionException {
        /*
                  a delete can succeed only if the key is contains in the 'keys" structure
                  a delete will remove the key from each of the structures
                  locks: the delete uses a lock on the the key
                  the delete can have a 'where' predicate which is to be evaluated against the decoded row, the delete  will be executed only if the predicate returns boolean 'true' value  (CAS operation)
         */
        Bytes key = delete.getKey();
        ReentrantReadWriteLock lock = locksManager.acquireWriteLockForKey(key);
        try {
            if (!keyToPage.containsKey(key)) {
                // no record at that key
                return new DMLStatementExecutionResult(0, key);
            }
            if (delete.getPredicate() != null) {
                Record actual = buffer.get(key);
                if (actual == null) {
                    ensureRecordLoaded(key);
                    actual = buffer.get(key);
                }
                if (!delete.getPredicate().evaluate(actual)) {
                    // record does not match predicate
                    return new DMLStatementExecutionResult(0, key);
                }
            }

            LogEntry entry = LogEntryFactory.delete(table, key, transaction);
            log.log(entry);

            // remove the record from the set of existing records
            keyToPage.remove(key);
            buffer.remove(key);
            return new DMLStatementExecutionResult(1, key);
        } catch (LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            locksManager.releaseWriteLockForKey(key, lock);
        }
    }

    private void ensureRecordLoaded(Bytes key) {

    }

    void close() {
        // TODO
    }

    private StatementExecutionResult executeGet(GetStatement get, Transaction transaction) throws StatementExecutionException {
        Bytes key = get.getKey();
        Predicate predicate = get.getPredicate();
        ReentrantReadWriteLock lock = locksManager.acquireReadLockForKey(key);
        try {
             // fastest path first, check if the record is loaded in memory
            Record loaded = buffer.get(key);
            if (loaded != null) {
                if (predicate != null && !predicate.evaluate(loaded)) {
                    return GetResult.NOT_FOUND;
                }
                return new GetResult(loaded);
            }
            if (keyToPage.containsKey(key)) {
                ensureRecordLoaded(key);
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
            locksManager.releaseReadLockForKey(key, lock);
        }

    }

}
