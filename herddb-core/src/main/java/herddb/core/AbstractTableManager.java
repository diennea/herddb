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
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import herddb.model.DDLException;
import herddb.model.DataScanner;
import herddb.model.Record;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.commands.ScanStatement;
import herddb.storage.DataStorageManagerException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Abstract of Table
 *
 * @author enrico.olivelli
 */
public interface AbstractTableManager extends AutoCloseable {

    StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException;

    TableManagerStats getStats();

    Table getTable();

    DataScanner scan(ScanStatement statement, StatementEvaluationContext context, Transaction transaction) throws StatementExecutionException;

    void start() throws DataStorageManagerException;

    void close();

    void dropTableData() throws DataStorageManagerException;

    void onTransactionRollback(Transaction transaction) throws DataStorageManagerException;

    void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException;

    void apply(LogSequenceNumber pos, LogEntry entry, boolean recovery) throws DataStorageManagerException;

    void dump(Consumer<Record> records) throws DataStorageManagerException;

    List<PostCheckpointAction> checkpoint(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException;
    
    void scanForIndexRebuild(Consumer<Record> records) throws DataStorageManagerException;

    void flush() throws DataStorageManagerException;
    
    long getNextPrimaryKeyValue();

    boolean isSystemTable();

    public void tableAltered(Table table, Transaction transaction) throws DDLException;

    long getCreatedInTransaction();

    public void tryReleaseMemory(long reclaim);
    
}
