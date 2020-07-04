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

import herddb.core.AbstractTableManager;
import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.core.stats.TableManagerStats;
import herddb.index.KeyToPageIndex;
import herddb.log.CommitLogResult;
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
import herddb.model.commands.ScanStatement;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

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
    public CompletableFuture<StatementExecutionResult> executeStatementAsync(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        return FutureUtils.exception(new StatementExecutionException("not supported on system tables"));
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
    public void start(boolean created) throws DataStorageManagerException {
    }

    @Override
    public void close() {
    }

    @Override
    public LogSequenceNumber getBootSequenceNumber() {
        return LogSequenceNumber.START_OF_TIME;
    }

    @Override
    public void dropTableData() throws DataStorageManagerException {
        throw new DataStorageManagerException("no supported on system tables");
    }

    @Override
    public void validateAlterTable(Table table, StatementEvaluationContext context) throws StatementExecutionException {
        throw new StatementExecutionException("Cannot alter system table " + table.name);
    }

    @Override
    public void onTransactionRollback(Transaction transaction) throws DataStorageManagerException {
    }

    @Override
    public void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException {
    }

    @Override
    public void apply(CommitLogResult pos, LogEntry entry, boolean recovery) throws DataStorageManagerException {
    }

    @Override
    public void dump(LogSequenceNumber sequenceNumber, FullTableScanConsumer receiver) throws DataStorageManagerException {
        throw new DataStorageManagerException("you cannot dump a system table!");
    }

    @Override
    public TableCheckpoint checkpoint(boolean pin) throws DataStorageManagerException {
        throw new DataStorageManagerException("you cannot checkpoint a system table!");
    }

    @Override
    public TableCheckpoint fullCheckpoint(boolean pin) throws DataStorageManagerException {
        throw new DataStorageManagerException("you cannot checkpoint a system table!");
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        throw new DataStorageManagerException("you cannot checkpoint a system table!");
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
    public boolean isStarted() {
        return true;
    }

    @Override
    public void tableAltered(Table table, Transaction transaction) throws DDLException {
        throw new InvalidTableException("cannot alter system tables");
    }

    protected abstract Iterable<Record> buildVirtualRecordList() throws StatementExecutionException;

    private final Comparator<Record> sortByPk = new Comparator<Record>() {
        @Override
        public int compare(Record o1, Record o2) {
            Map<String, Object> ac1 = o1.toBean(table);
            Map<String, Object> ac2 = o2.toBean(table);

            for (String pk : table.primaryKey) {
                Object value1 = ac1.get(pk);
                Object value2 = ac2.get(pk);
                int cmp = SQLRecordPredicateFunctions.compare(value1, value2);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    };

    @Override

    public DataScanner scan(
            ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        Predicate predicate = statement.getPredicate();
        MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                .createRecordSet(table.columnNames, table.columns);
        Iterable<Record> data = buildVirtualRecordList();
        StreamSupport
                .stream(data.spliterator(), false)
                .filter(record -> {
                    return (predicate == null || predicate.evaluate(record, context));
                })
                .sorted(sortByPk) // enforce sort by PK
                .map(r -> r.getDataAccessor(table))
                .forEach(recordSet::add);

        recordSet.writeFinished();
        recordSet.sort(statement.getComparator());
        recordSet.applyLimits(statement.getLimits(), context);
        recordSet.applyProjection(statement.getProjection(), context);
        return new SimpleDataScanner(transaction, recordSet);
    }

    @Override
    public long getCreatedInTransaction() {
        return 0;
    }

    @Override
    public List<Index> getAvailableIndexes() {
        return Collections.emptyList();
    }

    @Override
    public KeyToPageIndex getKeyToPageIndex() {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName()).append(" [table=").append(table).append("]");
        return builder.toString();
    }
}
