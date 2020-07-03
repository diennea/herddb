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

package herddb.sql.functions;

import static org.junit.Assert.assertTrue;
import herddb.core.AbstractTableManager;
import herddb.core.stats.TableManagerStats;
import herddb.index.KeyToPageIndex;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.Test;


/**
 * @author amitchavan
 */
public class ShowCreateTableCalculatorTest {


    public static class MockTableManager implements AbstractTableManager {

        private final Table t;
        private final List<Index> indexes;

        public MockTableManager(Table t, List<Index> idxs) {
            this.t = t;
            this.indexes = idxs;
        }

        @Override
        public TableManagerStats getStats() {
            return null;
        }

        @Override
        public Table getTable() {
            return t;
        }

        @Override
        public long getCreatedInTransaction() {
            return 0;
        }

        @Override
        public List<Index> getAvailableIndexes() {
            return indexes;
        }

        @Override
        public KeyToPageIndex getKeyToPageIndex() {
            return null;
        }

        @Override
        public LogSequenceNumber getBootSequenceNumber() {
            return null;
        }

        @Override
        public long getNextPrimaryKeyValue() {
            return 0;
        }

        @Override
        public boolean isSystemTable() {
            return false;
        }

        @Override
        public boolean isStarted() {
            return false;
        }

        @Override
        public void start() throws DataStorageManagerException {

        }

        @Override
        public void close() {

        }

        @Override
        public void flush() throws DataStorageManagerException {

        }

        @Override
        public void dump(LogSequenceNumber sequenceNumber, FullTableScanConsumer dataReceiver) throws DataStorageManagerException {

        }

        @Override
        public TableCheckpoint checkpoint(boolean pin) throws DataStorageManagerException {
            return null;
        }

        @Override
        public TableCheckpoint fullCheckpoint(boolean pin) throws DataStorageManagerException {
            return null;
        }

        @Override
        public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

        }

        @Override
        public void dropTableData() throws DataStorageManagerException {

        }

        @Override
        public void validateAlterTable(Table table, StatementEvaluationContext context) {
        }

        @Override
        public void tableAltered(Table table, Transaction transaction) throws DDLException {

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
        public CompletableFuture<StatementExecutionResult> executeStatementAsync(Statement statement, Transaction transaction, StatementEvaluationContext context) {
            return null;
        }

        @Override
        public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, Transaction transaction, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
            return null;
        }

        @Override
        public void scanForIndexRebuild(Consumer<Record> records) throws DataStorageManagerException {

        }
    }

    @Test
    public void test() {
        Table t = Table.builder()
                .uuid("1234")
                .column("k1", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.STRING)
                .primaryKey("k1")
                .tablespace("ts1")
                .name("test3")
                .build();

        AbstractTableManager tm = new MockTableManager(t, new ArrayList<>());
        assertTrue(ShowCreateTableCalculator.calculate(false, "test3", "ts1", tm).equals("CREATE TABLE ts1.test3(k1 integer,s1 string,PRIMARY KEY(k1))"));

        t = Table.builder()
                .uuid("1234")
                .column("k1", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.STRING)
                .column("s2", ColumnTypes.NOTNULL_STRING)
                .primaryKey("k1", true)
                .primaryKey("s2")
                .tablespace("ts1")
                .name("test3")
                .build();

        tm = new MockTableManager(t, new ArrayList<>());
        assertTrue(ShowCreateTableCalculator.calculate(false, "test3", "ts1", tm).equals("CREATE TABLE ts1.test3(k1 integer auto_increment,s1 string,s2 string not null,PRIMARY KEY(k1,s2))"));

        t = Table.builder()
                .uuid("1234")
                .column("k1", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.NOTNULL_STRING)
                .column("l1", ColumnTypes.NOTNULL_LONG)
                .column("i1", ColumnTypes.NOTNULL_INTEGER)
                .primaryKey("k1", true)
                .tablespace("ts1")
                .name("test3")
                .build();

        tm = new MockTableManager(t, new ArrayList<>());
        assertTrue(ShowCreateTableCalculator.calculate(false, "test3", "ts1", tm).equals("CREATE TABLE ts1.test3(k1 integer auto_increment,s1 string not null,l1 long not null,i1 integer not null,PRIMARY KEY(k1))"));

        t = Table.builder()
                .uuid("1234")
                .column("ID", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.NOTNULL_STRING)
                .column("l1", ColumnTypes.NOTNULL_LONG)
                .column("i1", ColumnTypes.NOTNULL_INTEGER)
                .primaryKey("ID", true)
                .tablespace("ts1")
                .name("test4")
                .build();

        tm = new MockTableManager(t, new ArrayList<>());
        assertTrue(ShowCreateTableCalculator.calculate(false, "test4", "ts1", tm).equals("CREATE TABLE ts1.test4(id integer auto_increment,s1 string not null,l1 long not null,i1 integer not null,PRIMARY KEY(id))"));
    }
}
