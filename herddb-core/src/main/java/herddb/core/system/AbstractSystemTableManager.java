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
package herddb.core.system;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import herddb.core.AbstractTableManager;
import herddb.core.MaterializedRecordSet;
import herddb.core.PostCheckpointAction;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.core.stats.TableManagerStats;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import herddb.model.DDLException;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.InvalidTableException;
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.model.Tuple;
import herddb.model.commands.ScanStatement;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;

/**
 * System tables
 *
 * @author enrico.olivelli
 */
public abstract class AbstractSystemTableManager implements AbstractTableManager {

    protected TableSpaceManager tableSpaceManager;
    protected final Table table;

    public AbstractSystemTableManager(TableSpaceManager tableSpaceManager, Table table) {
        this.tableSpaceManager = tableSpaceManager;
        this.table = table;
    }

    @Override
    public StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        throw new StatementExecutionException("not supported on system tables");
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public TableManagerStats getStats() {
        return new TableManagerStats() {

            @Override
            public int getLoadedpages() {
                return 0;
            }

            @Override
            public long getLoadedPagesCount() {
                return 0;
            }

            @Override
            public long getUnloadedPagesCount() {
                return 0;
            }

            @Override
            public long getTablesize() {
                return 0;
            }

            @Override
            public int getDirtypages() {
                return 0;
            }

            @Override
            public int getDirtyrecords() {
                return 0;
            }

            @Override
            public long getDirtyUsedMemory() {
                return 0;
            }

            @Override
            public long getMaxLogicalPageSize() {
                return 0;
            }

            @Override
            public long getBuffersUsedMemory() {
                return 0;
            }

            @Override
            public long getKeysUsedMemory() {
                return 0;
            }

        };
    }

    @Override
    public void start() throws DataStorageManagerException {
    }

    @Override
    public void close() {
    }

    @Override
    public void dropTableData() throws DataStorageManagerException {
        throw new DataStorageManagerException("no supported on system tables");
    }

    @Override
    public void onTransactionRollback(Transaction transaction) throws DataStorageManagerException {
    }

    @Override
    public void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException {
    }

    @Override
    public void apply(LogSequenceNumber pos, LogEntry entry, boolean recovery) throws DataStorageManagerException {
    }

    @Override
    public void dump(FullTableScanConsumer dataReceiver) throws DataStorageManagerException {
        throw new DataStorageManagerException("you cannot dump a system table!");
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
        return Collections.emptyList();
    }

    @Override
    public void flush() throws DataStorageManagerException {
    }

    @Override
    public void scanForIndexRebuild(Consumer<Record> records) throws DataStorageManagerException {
    }

    @Override
    public long getNextPrimaryKeyValue() {
        return -1;
    }

    @Override
    public boolean isSystemTable() {
        return true;
    }

    @Override
    public void tableAltered(Table table, Transaction transaction) throws DDLException {
        throw new InvalidTableException("cannot alter system tables");
    }

    protected abstract Iterable<Record> buildVirtualRecordList() throws StatementExecutionException;

    @Override
    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, Transaction transaction) throws StatementExecutionException {
        Predicate predicate = statement.getPredicate();
        MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
            .createRecordSet(table.columnNames, table.columns);
        for (Record record : buildVirtualRecordList()) {
            if (predicate == null || predicate.evaluate(record, context)) {
                recordSet.add(new Tuple(record.toBean(table)));
            }
        }
        recordSet.writeFinished();
        recordSet.sort(statement.getComparator());
        recordSet.applyLimits(statement.getLimits(), context);
        recordSet.applyProjection(statement.getProjection(), context);
        return new SimpleDataScanner(transaction != null ? transaction.transactionId : 0, recordSet);
    }

    @Override
    public long getCreatedInTransaction() {
        return 0;
    }

    @Override
    public List<Index> getAvailableIndexes() {
        return Collections.emptyList();
    }

}
