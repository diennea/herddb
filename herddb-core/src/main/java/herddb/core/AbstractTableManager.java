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
import herddb.index.KeyToPageIndex;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import herddb.model.DDLException;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.commands.ScanStatement;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Abstract of Table
 *
 * @author enrico.olivelli
 */
public interface AbstractTableManager extends AutoCloseable {

    TableManagerStats getStats();

    Table getTable();

    long getCreatedInTransaction();

    List<Index> getAvailableIndexes();

    KeyToPageIndex getKeyToPageIndex();

    LogSequenceNumber getBootSequenceNumber();

    long getNextPrimaryKeyValue();

    boolean isSystemTable();

    /**
     * Check if the table manage has been fully started
     */
    boolean isStarted();

    void start() throws DataStorageManagerException;

    @Override
    void close();

    void flush() throws DataStorageManagerException;

    void dump(LogSequenceNumber sequenceNumber, FullTableScanConsumer dataReceiver) throws DataStorageManagerException;

    /**
     * Perform a faster checkpoint
     */
    TableCheckpoint checkpoint(boolean pin) throws DataStorageManagerException;

    /**
     * Performs a full deep checkpoint cleaning as much space as possible.
     * <p>
     * It's an hint for table manager for perform a more aggressive checkpoint.
     * Table manager implementations can resolve to perform a normal checkpoint
     * if they need for internal logic.
     * </p>
     */
    TableCheckpoint fullCheckpoint(boolean pin) throws DataStorageManagerException;

    /**
     * Unpin a previously pinned checkpont (see {@link #checkpoint(boolean)})
     *
     * @throws DataStorageManagerException
     */
    void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    void dropTableData() throws DataStorageManagerException;

    void tableAltered(Table table, Transaction transaction) throws DDLException;

    void onTransactionRollback(Transaction transaction) throws DataStorageManagerException;

    void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException;

    void apply(CommitLogResult pos, LogEntry entry, boolean recovery) throws DataStorageManagerException;

    default StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        CompletableFuture<StatementExecutionResult> res = executeStatementAsync(statement, transaction, context);
        try {
            return res.get();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new StatementExecutionException(err);
        } catch (ExecutionException err) {
            Throwable cause = err.getCause();
            if (cause instanceof HerdDBInternalException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof StatementExecutionException) {
                throw (StatementExecutionException) cause;
            } else {
                throw new StatementExecutionException(cause);
            }
        }
    }

    CompletableFuture<StatementExecutionResult> executeStatementAsync(Statement statement, Transaction transaction, StatementEvaluationContext context);

    DataScanner scan(
            ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException;

    void scanForIndexRebuild(Consumer<Record> records) throws DataStorageManagerException;

    final class TableCheckpoint {

        final String tableName;
        final LogSequenceNumber sequenceNumber;
        final List<PostCheckpointAction> actions;

        public TableCheckpoint(String tableName, LogSequenceNumber sequenceNumber, List<PostCheckpointAction> actions) {
            super();
            this.tableName = tableName;
            this.sequenceNumber = sequenceNumber;
            this.actions = actions;
        }
    }

}
