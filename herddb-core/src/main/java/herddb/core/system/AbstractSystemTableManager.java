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
import herddb.log.LogEntry;
import herddb.model.ColumnTypes;
import herddb.model.DDLException;
import herddb.model.DataScanner;
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
import java.util.function.Consumer;

/**
 * System tables
 *
 * @author enrico.olivelli
 */
public abstract class AbstractSystemTableManager implements AbstractTableManager {

    protected TableSpaceManager parent;
    protected final Table table;

    public AbstractSystemTableManager(TableSpaceManager parent, Table table) {
        this.parent = parent;
        this.table = table;
    }

    @Override
    public StatementExecutionResult executeStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        throw new StatementExecutionException("no supported on system tables");
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
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getMaxrecordsperpage() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getMaxloadedpages() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public long getTablesize() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getDirtypages() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public int getDirtyrecords() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    public void onTransactionCommit(Transaction transaction) throws DataStorageManagerException {
    }

    @Override
    public void apply(LogEntry entry) throws DataStorageManagerException {
    }

    @Override
    public void dump(Consumer<Record> records) throws DataStorageManagerException {
    }

    @Override
    public void checkpoint() throws DataStorageManagerException {

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
    public void tableAltered(Table table) throws DDLException {
        throw new InvalidTableException("cannot alter system tables");
    }

    protected abstract Iterable<Record> buildVirtualRecordList();

    @Override
    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, Transaction transaction) throws StatementExecutionException {
        Predicate predicate = statement.getPredicate();
        MaterializedRecordSet recordSet = new MaterializedRecordSet(table.columns);
        for (Record record : buildVirtualRecordList()) {
            if (predicate == null || predicate.evaluate(record, context)) {
                recordSet.records.add(new Tuple(record.toBean(table)));
            }
        }
        recordSet.sort(statement.getComparator());
        recordSet.applyLimits(statement.getLimits());
        recordSet = recordSet.select(statement.getProjection());
        return new SimpleDataScanner(recordSet);
    }

}
