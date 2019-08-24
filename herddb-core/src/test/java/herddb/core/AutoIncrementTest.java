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

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Tests on table creation. the full record is the PK
 *
 * @author enrico.olivelli
 */
public class AutoIncrementTest {

    @Test
    public void testAutoIncrementInt() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (n1 int primary key auto_increment, s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());

            List<DataAccessor> rows = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
            assertEquals(6, rows.size());
            for (int i = 1; i <= 6; i++) {
                int _i = i;
                assertTrue(rows.stream().filter(t -> t.get("n1").equals(Integer.valueOf(_i))).findAny().isPresent());
            }

            DMLStatementExecutionResult result = executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"));
            assertEquals(1, result.getUpdateCount());
            assertEquals(Bytes.from_int(7), result.getKey());
        }
    }

    @Test
    public void testAutoIncrementLong() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (n1 long primary key auto_increment, s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());

            List<DataAccessor> rows = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
            assertEquals(6, rows.size());
            for (int i = 1; i <= 6; i++) {
                int _i = i;
                assertTrue(rows.stream().filter(t -> t.get("n1").equals(Long.valueOf(_i))).findAny().isPresent());
            }

            DMLStatementExecutionResult result = executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"));
            assertEquals(1, result.getUpdateCount());
            assertEquals(Bytes.from_long(7), result.getKey());

        }
    }

    @Test
    public void testAutoIncrement_other_sintax() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (n1 int auto_increment, s1 string, primary key (n1))", Collections.emptyList());
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa")).getUpdateCount());
                assertEquals(2, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?),(?)", Arrays.asList("aa", "aa")).getUpdateCount());

                List<DataAccessor> rows = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
                assertEquals(6, rows.size());
                for (int i = 1; i <= 6; i++) {
                    int _i = i;
                    assertTrue(rows.stream().filter(t -> t.get("n1").equals(Integer.valueOf(_i))).findAny().isPresent());
                }

                DMLStatementExecutionResult result = executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"));
                assertEquals(1, result.getUpdateCount());
                assertEquals(Bytes.from_int(7), result.getKey());
                assertEquals(7, executeUpdate(manager, "delete from tblspace1.tsql", Collections.emptyList()).getUpdateCount());
            }
            // the same but in transaction
            {
                DMLStatementExecutionResult res = executeUpdate(manager,
                        "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), TransactionContext.AUTOTRANSACTION_TRANSACTION);
                assertEquals(1, res.getUpdateCount());
                long tx = res.transactionId;
                assertTrue(tx > 0);
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx)).getUpdateCount());
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx)).getUpdateCount());
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx)).getUpdateCount());
                assertEquals(2, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?),(?)", Arrays.asList("aa", "aa"), new TransactionContext(tx)).getUpdateCount());

                List<DataAccessor> rows = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose();
                assertEquals(6, rows.size());
                for (int i = 8; i <= 13; i++) {
                    int _i = i;
                    assertTrue("can't find " + i, rows.stream().filter(t -> t.get("n1").equals(Integer.valueOf(_i))).findAny().isPresent());
                }

                DMLStatementExecutionResult result = executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx));
                assertEquals(1, result.getUpdateCount());
                assertEquals(Bytes.from_int(14), result.getKey());
                assertEquals(tx, res.transactionId);
                // transaction not committed
            }
            // the same but in transaction, multi values as first
            {
                DMLStatementExecutionResult res = executeUpdate(manager,
                        "INSERT INTO tblspace1.tsql(s1) values(?),(?)", Arrays.asList("aa", "aa"), TransactionContext.AUTOTRANSACTION_TRANSACTION);
                assertEquals(2, res.getUpdateCount());
                long tx = res.transactionId;
                assertTrue(tx > 0);
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx)).getUpdateCount());
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx)).getUpdateCount());
                assertEquals(2, executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?),(?)", Arrays.asList("aa", "aa"), new TransactionContext(tx)).getUpdateCount());

                List<DataAccessor> rows = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose();
                assertEquals(6, rows.size());
                for (int i = 15; i <= 20; i++) {
                    int _i = i;
                    assertTrue(rows.stream().filter(t -> t.get("n1").equals(Integer.valueOf(_i))).findAny().isPresent());
                }

                DMLStatementExecutionResult result = executeUpdate(manager, "INSERT INTO tblspace1.tsql(s1) values(?)", Arrays.asList("aa"), new TransactionContext(tx));
                assertEquals(1, result.getUpdateCount());
                assertEquals(Bytes.from_int(21), result.getKey());
                assertEquals(tx, res.transactionId);
                assertEquals(7, executeUpdate(manager, "delete from tblspace1.tsql", Collections.emptyList(), new TransactionContext(tx)).getUpdateCount());
            }
        }
    }

}
