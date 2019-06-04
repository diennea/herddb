package herddb.sql.functions;

import herddb.core.AbstractTableManager;
import herddb.core.stats.TableManagerStats;
import herddb.index.KeyToPageIndex;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogSequenceNumber;
import herddb.model.*;
import herddb.model.commands.ScanStatement;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;



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
                .column("k1", ColumnTypes.INTEGER, 0)
                .column("s1", ColumnTypes.STRING, 1)
                .primaryKey("k1")
                .tablespace("ts1")
                .name("test3")
                .build();

        AbstractTableManager tm = new MockTableManager(t, new ArrayList<>());
        assertTrue(ShowCreateTableCalculator.calculate(false, "test3", "ts1", tm).equals("CREATE TABLE ts1.test3(k1 integer,s1 string,PRIMARY KEY(k1))"));


        t = Table.builder()
                .uuid("1234")
                .column("k1", ColumnTypes.INTEGER, 0)
                .column("s1", ColumnTypes.STRING, 1)
                .primaryKey("k1")
                .tablespace("ts1")
                .name("test3")
                .build();

        tm = new MockTableManager(t, new ArrayList<>());
        assertTrue(ShowCreateTableCalculator.calculate(false, "test3", "ts1", tm).equals("CREATE TABLE ts1.test3(k1 integer,s1 string,PRIMARY KEY(k1))"));


    }
}
