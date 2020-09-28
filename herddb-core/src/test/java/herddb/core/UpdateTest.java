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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * @author diego.salvi
 */
public class UpdateTest {

    @Test
    public void updateWithJDBC() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1,s1))", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey4", "a", Integer.valueOf(1234))).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = n1 + ? WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = n1 - ? WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = ? + n1 WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = ? + n1 WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = ? + n1 WHERE k1 = ? AND s1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey", "a")).getUpdateCount());
        }
    }

    /**
     * Check update then select (#320, not every row update really was run).
     *
     * @author diego.salvi
     */
    @Test
    public void updateThenSelect() throws Exception {

        int runs = 50;
        int inserts = 100;

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            for (int run = 0; run < runs; ++run) {

                execute(manager, "CREATE TABLE tblspace1.tsql (k1 int, n1 int, primary key(k1))", Collections.emptyList());

                for (int i = 0; i < inserts; ++i) {
                    assertEquals(1, executeUpdate(manager,
                            "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)",
                            Arrays.asList(Integer.valueOf(i), Integer.valueOf(1))).getUpdateCount());
                }

                long tx = beginTransaction(manager, "tblspace1");

                TransactionContext ctx = new TransactionContext(tx);

                assertEquals(inserts, executeUpdate(manager,
                        "UPDATE tblspace1.tsql set n1 = 100",
                        Collections.emptyList(), ctx).getUpdateCount());

                for (int i = 0; i < inserts; ++i) {
                    assertEquals(1, scan(manager,
                            "SELECT k1, n1 FROM tblspace1.tsql WHERE k1 = ? AND n1 = 100",
                            Arrays.asList(Integer.valueOf(i)), ctx).consumeAndClose().size());
                }

                for (int i = 0; i < inserts; ++i) {
                    try {
                        assertEquals(1, scan(manager,
                                "SELECT k1, n1 FROM tblspace1.tsql WHERE k1 = ? AND n1 = 100",
                                Arrays.asList(Integer.valueOf(i)), ctx).consumeAndClose().size());
                    } catch (AssertionError e) {

                        throw e;
                    }
                }

                commitTransaction(manager, "tblspace1", tx);

                for (int i = 0; i < inserts; ++i) {
                    assertEquals(1, scan(manager,
                            "SELECT k1, n1 FROM tblspace1.tsql WHERE k1 = ? AND n1 = 100",
                            Arrays.asList(Integer.valueOf(i), ctx)).consumeAndClose().size());
                }

                execute(manager, "DROP TABLE tblspace1.tsql", Collections.emptyList());

            }

        }
    }

    @Test
    public void updateMultiRowsWithValidationError() throws Exception {

        final int inserts = 10;

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 int, n1 int not null, primary key(k1))", Collections.emptyList());

            for (int i = 0; i < inserts; i++) {
                assertEquals(1, executeUpdate(manager,
                        "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)",
                        Arrays.asList(Integer.valueOf(i), Integer.valueOf(1))).getUpdateCount());
            }

            long tx = beginTransaction(manager, "tblspace1");

            TransactionContext ctx = new TransactionContext(tx);

            // single record failed update
            StatementExecutionException error =
                    herddb.utils.TestUtils.expectThrows(StatementExecutionException.class, () -> {
                        executeUpdate(manager,
                                "UPDATE tblspace1.tsql set n1 = null WHERE n1=1",
                                Collections.emptyList(), ctx);
                    });
            assertEquals("error on column n1 (integer not null):Cannot have null value in non-NULL type integer", error.getMessage());

            // multi record failed update
            StatementExecutionException errors =
                    herddb.utils.TestUtils.expectThrows(StatementExecutionException.class, () -> {
                        executeUpdate(manager,
                                "UPDATE tblspace1.tsql set n1 = null",
                                Collections.emptyList(), ctx);
                    });
            assertEquals("error on column n1 (integer not null):Cannot have null value in non-NULL type integer", errors.getMessage());

            commitTransaction(manager, "tblspace1", tx);

        }

    }

    @Test
    public void upsertTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT n1 from tblspace1.tsql where k1=?", Arrays.asList("mykey"))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(1234, recordSet.get(0).get(0));
            }
            assertEquals(1, executeUpdate(manager, "UPSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1235))).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT n1 from tblspace1.tsql where k1=?", Arrays.asList("mykey"))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(1235, recordSet.get(0).get(0));
            }
            assertEquals(4, executeUpdate(manager, "UPSERT INTO tblspace1.tsql(k1,n1)"
                    + "values(?,?),(?,?),(?,?),(?,?)", Arrays.asList(
                            "mykey", Integer.valueOf(1235),
                            "mykey", Integer.valueOf(1236),
                            "mykey", Integer.valueOf(1237),
                            "mykey", Integer.valueOf(1238)
                            )).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT n1 from tblspace1.tsql where k1=?", Arrays.asList("mykey"))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(1238, recordSet.get(0).get(0));
            }

            assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql", Collections.emptyList()).getUpdateCount());
            execute(manager, "ALTER TABLE tblspace1.tsql MODIFY s1 string not null", Collections.emptyList());

            // assert that UPSERT fails
            StatementExecutionException error =
                    herddb.utils.TestUtils.expectThrows(StatementExecutionException.class, () ->  {
                        executeUpdate(manager, "UPSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1235)));
                    });
            assertThat(error.getMessage(), containsString("Column 's1' has no default value and does not allow NULLs"));

            // insert a value, n1 has a value
            assertEquals(1, executeUpdate(manager, "UPSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,'non-empty')", Arrays.asList("mykey", Integer.valueOf(1235))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT n1 from tblspace1.tsql where k1=?", Arrays.asList("mykey"))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(1235, recordSet.get(0).get(0));
            }

            // upsert, making n1 null now, because it has not been named in the INSERT clause
            // use non uppercase casing in UPSERT keyword
            assertEquals(1, executeUpdate(manager, "UPsert INTO tblspace1.tsql(k1,s1) values(?,'non-empty')", Arrays.asList("mykey")).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT n1 from tblspace1.tsql where k1=?", Arrays.asList("mykey"))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertNull(recordSet.get(0).get(0));
            }
        }
    }


    @Test
    public void simpleManualSequence() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.myseq (k1 int, k2 int, current_value int, primary key(k1, k2))", Collections.emptyList());

            DMLStatementExecutionResult insertSequence1 =
                    executeUpdate(manager, "INSERT INTO tblspace1.myseq(k1,k2,current_value) values(?,?,?)",
                            Arrays.asList(Integer.valueOf(78), Integer.valueOf(0), Integer.valueOf(1)), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            assertTrue(insertSequence1.transactionId > 0);

            try (DataScanner scan = scan(manager, "SELECT current_value from tblspace1.myseq where k1=?", Arrays.asList(78), new TransactionContext(insertSequence1.transactionId))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(1, recordSet.get(0).get(0));
            }

            // set current_value = 2 if current_value = 1
            DMLStatementExecutionResult updateSequence1 =
                    executeUpdate(manager, "UPDATE tblspace1.myseq set current_value=? where k1=78 and k2=0 and current_value=?",
                            Arrays.asList(Integer.valueOf(2), Integer.valueOf(1)), new TransactionContext(insertSequence1.transactionId));
            assertEquals(insertSequence1.transactionId, updateSequence1.transactionId);
            assertEquals(1, updateSequence1.getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT current_value from tblspace1.myseq where k1=?", Arrays.asList(78), new TransactionContext(updateSequence1.transactionId))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(2, recordSet.get(0).get(0));
            }

            DMLStatementExecutionResult updateSequence2 =
                    executeUpdate(manager, "UPDATE tblspace1.myseq set current_value=? where k1=78 and k2=0 and current_value=?",
                            Arrays.asList(Integer.valueOf(3), Integer.valueOf(2)), new TransactionContext(updateSequence1.transactionId));
            assertEquals(updateSequence2.transactionId, updateSequence1.transactionId);
            assertEquals(1, updateSequence2.getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT current_value from tblspace1.myseq where k1=?", Arrays.asList(78), new TransactionContext(updateSequence1.transactionId))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(1, recordSet.size());
                assertEquals(3, recordSet.get(0).get(0));
            }

            DMLStatementExecutionResult deleteSequence2 =
                    executeUpdate(manager, "DELETE FROM tblspace1.myseq where k1=78 and k2=0 ",
                            Arrays.asList(), new TransactionContext(updateSequence1.transactionId));
            assertEquals(deleteSequence2.transactionId, updateSequence1.transactionId);
            assertEquals(1, deleteSequence2.getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT current_value from tblspace1.myseq where k1=?", Arrays.asList(78), new TransactionContext(updateSequence1.transactionId))) {
                List<DataAccessor> recordSet = scan.consumeAndClose();
                assertEquals(0, recordSet.size());
            }

        }
    }

}
