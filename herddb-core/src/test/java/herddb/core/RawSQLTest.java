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

import herddb.codec.RecordSerializer;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import herddb.index.PrimaryIndexSeek;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.IndexAlreadyExistsException;
import herddb.model.IndexDoesNotExistException;
import herddb.model.MissingJDBCParameterException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.planner.PlannerOp;
import herddb.sql.CalcitePlanner;
import herddb.sql.SQLPlanner;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.MapUtils;
import herddb.utils.RawString;
import java.sql.Timestamp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class RawSQLTest {

    @Test
    public void cacheStatement() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            ScanStatement scanFirst = null;
            for (int i = 0; i < 100; i++) {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexSeek);
                if (scanFirst == null) {
                    scanFirst = scan;
                } else {
                    assertTrue(scan == scanFirst);
                }
            }

        }
    }

    @Test
    public void jdbcWrongParameterCountTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            try {
                scan(manager, "SELECT * FROM tblspace1.tsql where k1=?", Collections.emptyList());
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(1, ok.getIndex());
            }

            try {
                scan(manager, "SELECT * FROM tblspace1.tsql where k1=1 and n1=?", Collections.emptyList());
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(1, ok.getIndex());
            }

            try {
                scan(manager, "SELECT * FROM tblspace1.tsql where k1=1 or n1=?", Collections.emptyList());
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(1, ok.getIndex());
            }

            try {
                scan(manager, "SELECT * FROM tblspace1.tsql order by k1 limit ?", Collections.emptyList());
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(1, ok.getIndex());
            }

            if (manager.getPlanner() instanceof SQLPlanner) {
                try {
                    scan(manager, "SELECT * FROM tblspace1.tsql where n1 = 1234 and k1 in "
                            + "(SELECT k1 FROM tblspace1.tsql order by k1 limit ?) and n1 = ?", Arrays.asList(1));
                    fail();
                } catch (MissingJDBCParameterException ok) {
                    assertEquals(2, ok.getIndex());
                }

                scan(manager, "SELECT * FROM tblspace1.tsql where n1 = ? and k1 in "
                        + "(SELECT k1 FROM tblspace1.tsql order by k1 limit ?)", Arrays.asList(1));

                try {
                    scan(manager, "SELECT * FROM tblspace1.tsql where k1 in "
                            + "(SELECT k1 FROM tblspace1.tsql order by k1 limit ?)", Collections.emptyList());
                    fail();
                } catch (MissingJDBCParameterException ok) {
                    assertEquals(1, ok.getIndex());
                }

                try {
                    scan(manager, "SELECT * FROM tblspace1.tsql where k1 in (SELECT k1+? FROM tblspace1.tsql)", Collections.emptyList());
                    fail();
                } catch (MissingJDBCParameterException ok) {
                    assertEquals(1, ok.getIndex());
                }
            }
            try {
                scan(manager, "SELECT * FROM tblspace1.tsql where k1 in (SELECT k1 FROM tblspace1.tsql where n1=?)", Collections.emptyList());
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(1, ok.getIndex());
            }

            try {
                scan(manager, "SELECT * FROM tblspace1.tsql where n1=? and k1 in (SELECT k1 FROM tblspace1.tsql where n1=?)",
                        Arrays.asList(1));
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(2, ok.getIndex());
            }

            try {
                scan(manager, "SELECT * FROM tblspace1.tsql where k1=1 and n1=? and n1=?", Arrays.asList(1));
                fail();
            } catch (MissingJDBCParameterException ok) {
                assertEquals(2, ok.getIndex());
            }

            try {
                scan(manager, "SELECT n1+? FROM tblspace1.tsql", Collections.emptyList());
            } catch (MissingJDBCParameterException ok) {
                assertEquals(1, ok.getIndex());
            }

            if (manager.getPlanner() instanceof SQLPlanner) {
                try {
                    scan(manager, "SELECT sum(n1), sum(?) FROM tblspace1.tsql", Collections.emptyList());
                } catch (MissingJDBCParameterException ok) {
                    assertEquals(1, ok.getIndex());
                }
            }

        }
    }

    @Test
    public void createNumericTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,"
                    + "n1 decimal(18,0),"
                    + "n2 numeric(10,0))", Collections.emptyList());

        }
    }

    @Test
    public void escapedStringTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,"
                    + "s1 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.tsql (k1 ,"
                    + "s1) values('test','test ''escaped')", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT k1,s1 FROM tblspace1.tsql where s1='test ''escaped'", Collections.emptyList());) {
                assertEquals(1, scan.consume().size());
            }

        }
    }

    @Test
    public void currentTimestampTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string,n1 int,s1 string,t1 timestamp, primary key (t1) )", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,CURRENT_TIMESTAMP)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            Thread.sleep(500);
            assertEquals(1234, scan(manager, "SELECT n1 FROM tblspace1.tsql WHERE t1<CURRENT_TIMESTAMP", Collections.emptyList()).consume().get(0).get("n1"));

            java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
            if (manager.getPlanner() instanceof SQLPlanner) {
                // non standard syntax, needs a decoding
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,'" + RecordSerializer.getUTCTimestampFormatter()
                        .format(now.toInstant()) + "')", Arrays.asList("mykey2", Integer.valueOf(1234))).getUpdateCount());
            }
            java.sql.Timestamp now2 = new java.sql.Timestamp(now.getTime() + 1000);
            // standard syntax, but timezone dependant
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,{ts '" + now2 + "'})", Arrays.asList("mykey3", Integer.valueOf(1234))).getUpdateCount());
        }
    }

    @Test
    public void caseWhenTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string,t1 timestamp)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,CURRENT_TIMESTAMP)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,CURRENT_TIMESTAMP)", Arrays.asList("mykey2", Integer.valueOf(1235))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,CURRENT_TIMESTAMP)", Arrays.asList("mykey3", Integer.valueOf(1236))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT k1, "
                    + "CASE "
                    + "WHEN k1='mykey'  THEN 'a' "
                    + "WHEN k1='mykey2' THEN 'b' "
                    + "ELSE 'c'  "
                    + "END as mycase "
                    + "FROM tblspace1.tsql "
                    + "ORDER BY k1", Collections.emptyList())) {
                List<DataAccessor> res = scan.consume();
                for (DataAccessor t : res) {
                    System.out.println("t:" + t);
                }
                assertEquals(3, res.size());
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey", "mycase", "a"
                ))));
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey2", "mycase", "b"
                ))));
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey3", "mycase", "c"
                ))));
            }
            try (DataScanner scan = scan(manager, "SELECT k1, "
                    + "CASE "
                    + "WHEN k1='mykey'  THEN 'a' "
                    + "WHEN k1='mykey2' THEN 'b' "
                    + "END as mycase "
                    + "FROM tblspace1.tsql "
                    + "ORDER BY k1", Collections.emptyList())) {
                List<DataAccessor> res = scan.consume();
                for (DataAccessor t : res) {
                    System.out.println("t:" + t);
                }
                assertEquals(3, res.size());
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey", "mycase", "a"
                ))));
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey2", "mycase", "b"
                ))));
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey3", "mycase", null
                ))));
            }
            try (DataScanner scan = scan(manager, "SELECT k1, "
                    + "SUM(CASE "
                    + "WHEN k1='mykey'  THEN 1 "
                    + "WHEN k1='mykey2' THEN 2 "
                    + "ELSE 3  "
                    + "END) as mysum "
                    + "FROM tblspace1.tsql "
                    + "GROUP BY k1",
                    Collections.emptyList())) {
                List<DataAccessor> res = scan.consume();
                for (DataAccessor t : res) {
                    System.out.println("t2:" + t);
                }
                assertEquals(3, res.size());
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey", "mysum", 1L
                ))));
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey2", "mysum", 2L
                ))));
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "mykey3", "mysum", 3L
                ))));
            }
            try (DataScanner scan = scan(manager, "SELECT "
                    + "SUM(CASE "
                    + "WHEN k1='mykey'  THEN 1 "
                    + "WHEN k1='mykey2' THEN 2 "
                    + "ELSE 3  "
                    + "END) as mysum "
                    + "FROM tblspace1.tsql "
                    + "",
                    Collections.emptyList())) {
                List<DataAccessor> res = scan.consume();
                for (DataAccessor t : res) {
                    System.out.println("t:" + t);
                }
                assertEquals(1, res.size());
                assertTrue(
                        res.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "mysum", 6L
                ))));

            }
        }
    }

    @Test
    public void insertFromSelect() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string,t1 timestamp)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.tsql2 (k2 string primary key,n2 int,s2 string,t2 timestamp)", Collections.emptyList());

            java.sql.Timestamp tt1 = new java.sql.Timestamp(System.currentTimeMillis());
            java.sql.Timestamp tt2 = new java.sql.Timestamp(System.currentTimeMillis() + 60000);
            java.sql.Timestamp tt3 = new java.sql.Timestamp(System.currentTimeMillis() + 120000);

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1234), tt1)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,?)", Arrays.asList("mykey2", Integer.valueOf(1235), tt2)).getUpdateCount());
            assertEquals(2, executeUpdate(manager, "INSERT INTO tblspace1.tsql2(k2,t2,n2)"
                    + "(select k1,t1,n1 from tblspace1.tsql)", Collections.emptyList()).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT k2,n2,t2 FROM tblspace1.tsql2 ORDER BY n2 desc", Collections.emptyList());) {
                List<DataAccessor> res = scan.consume();
                assertEquals(RawString.of("mykey2"), res.get(0).get("k2"));
                assertEquals(RawString.of("mykey"), res.get(1).get("k2"));
                assertEquals(Integer.valueOf(1235), res.get(0).get("n2"));
                assertEquals(Integer.valueOf(1234), res.get(1).get("n2"));
                assertEquals(tt2, res.get(0).get("t2"));
                assertEquals(tt1, res.get(1).get("t2"));
            }

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,?)", Arrays.asList("mykey3", Integer.valueOf(1236), tt1)).getUpdateCount());
            DMLStatementExecutionResult executeUpdateInTransaction = executeUpdate(manager, "INSERT INTO tblspace1.tsql2(k2,t2,n2)"
                    + "(select k1,t1,n1 from tblspace1.tsql where n1=?)", Arrays.asList(1236), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            assertEquals(1, executeUpdateInTransaction.getUpdateCount());
            assertTrue(executeUpdateInTransaction.transactionId > 0);
            try (DataScanner scan = scan(manager, "SELECT k2,n2,t2 FROM tblspace1.tsql2 ORDER BY n2 desc", Collections.emptyList(), new TransactionContext(executeUpdateInTransaction.transactionId));) {
                assertEquals(3, scan.consume().size());
            }
            manager.executeStatement(new RollbackTransactionStatement("tblspace1", executeUpdateInTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = scan(manager, "SELECT k2,n2,t2 FROM tblspace1.tsql2 ORDER BY n2 desc", Collections.emptyList());) {
                assertEquals(2, scan.consume().size());
            }

            DMLStatementExecutionResult executeUpdateInTransaction2 = executeUpdate(manager, "INSERT INTO tblspace1.tsql2(k2,t2,n2)"
                    + "(select k1,t1,n1 from tblspace1.tsql where n1=?)", Arrays.asList(1236), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            assertEquals(1, executeUpdateInTransaction2.getUpdateCount());
            assertTrue(executeUpdateInTransaction2.transactionId > 0);
            manager.executeStatement(new CommitTransactionStatement("tblspace1", executeUpdateInTransaction2.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = scan(manager, "SELECT k2,n2,t2 FROM tblspace1.tsql2 ORDER BY n2 desc", Collections.emptyList());) {
                assertEquals(3, scan.consume().size());
            }
            if (manager.getPlanner() instanceof SQLPlanner) {
                DMLStatementExecutionResult executeUpdateWithParameters = executeUpdate(manager, "INSERT INTO tblspace1.tsql2(k2,t2,n2)"
                        + "(select ?,?,n1 from tblspace1.tsql where n1=?)", Arrays.asList("mykey5", tt3, 1236), TransactionContext.NO_TRANSACTION);
                assertEquals(1, executeUpdateWithParameters.getUpdateCount());
                assertTrue(executeUpdateWithParameters.transactionId == 0);

                try (DataScanner scan = scan(manager, "SELECT k2,n2,t2 "
                        + "FROM tblspace1.tsql2 "
                        + "WHERE t2 = ?", Arrays.asList(tt3));) {
                    List<DataAccessor> all = scan.consume();
                    assertEquals(1, all.size());
                    assertEquals(Integer.valueOf(1236), all.get(0).get("n2"));
                    assertEquals(tt3, all.get(0).get("t2"));
                    assertEquals(RawString.of("mykey5"), all.get(0).get("k2"));
                }
            }
        }
    }

    @Test
    public void multiInsert() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string,t1 timestamp)", Collections.emptyList());

            java.sql.Timestamp tt1 = new java.sql.Timestamp(System.currentTimeMillis());
            java.sql.Timestamp tt2 = new java.sql.Timestamp(System.currentTimeMillis() + 60000);

            assertEquals(2,
                    executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,?),(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1234), tt1,
                            "mykey2", Integer.valueOf(1235), tt2)).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT k1,n1,t1 FROM tblspace1.tsql ORDER BY n1 desc", Collections.emptyList());) {
                List<DataAccessor> res = scan.consume();
                assertEquals(RawString.of("mykey2"), res.get(0).get("k1"));
                assertEquals(RawString.of("mykey"), res.get(1).get("k1"));
                assertEquals(Integer.valueOf(1235), res.get(0).get("n1"));
                assertEquals(Integer.valueOf(1234), res.get(1).get("n1"));
                assertEquals(tt2, res.get(0).get("t1"));
                assertEquals(tt1, res.get(1).get("t1"));
            }

            execute(manager, "CREATE TABLE tblspace1.tsql2 (a1 integer auto_increment primary key, k1 string ,n1 int,s1 string,t1 timestamp)", Collections.emptyList());
            assertEquals(2,
                    executeUpdate(manager, "INSERT INTO tblspace1.tsql2(k1,n1,t1) values(?,?,?),(?,?,?)",
                            Arrays.asList("mykey", Integer.valueOf(1234), tt1,
                                    "mykey2", Integer.valueOf(1235), tt2)).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT a1,k1,n1,t1 FROM tblspace1.tsql2 ORDER BY n1 desc", Collections.emptyList());) {
                List<DataAccessor> res = scan.consume();
                assertEquals(RawString.of("mykey2"), res.get(0).get("k1"));
                assertEquals(RawString.of("mykey"), res.get(1).get("k1"));
                assertEquals(Integer.valueOf(1235), res.get(0).get("n1"));
                assertEquals(Integer.valueOf(1234), res.get(1).get("n1"));
                assertEquals(tt2, res.get(0).get("t1"));
                assertEquals(tt1, res.get(1).get("t1"));
                assertEquals(2, res.get(0).get("a1"));
                assertEquals(1, res.get(1).get("a1"));
            }

            // auto-transaction
            execute(manager, "CREATE TABLE tblspace1.tsql3 (a1 integer auto_increment primary key, k1 string ,n1 int,s1 string,t1 timestamp)", Collections.emptyList());
            DMLStatementExecutionResult resInsert = executeUpdate(manager, "INSERT INTO tblspace1.tsql3(k1,n1,t1) values(?,?,?),(?,?,?)",
                    Arrays.asList("mykey", Integer.valueOf(1234), tt1,
                            "mykey2", Integer.valueOf(1235), tt2), TransactionContext.AUTOTRANSACTION_TRANSACTION
            );
            assertEquals(2,
                    resInsert.getUpdateCount());
            assertTrue(resInsert.transactionId > 0);
            try (DataScanner scan = scan(manager, "SELECT a1,k1,n1,t1 FROM tblspace1.tsql3 ORDER BY n1 desc", Collections.emptyList(), 0,
                    new TransactionContext(resInsert.transactionId));) {
                List<DataAccessor> res = scan.consume();
                assertEquals(RawString.of("mykey2"), res.get(0).get("k1"));
                assertEquals(RawString.of("mykey"), res.get(1).get("k1"));
                assertEquals(Integer.valueOf(1235), res.get(0).get("n1"));
                assertEquals(Integer.valueOf(1234), res.get(1).get("n1"));
                assertEquals(tt2, res.get(0).get("t1"));
                assertEquals(tt1, res.get(1).get("t1"));
                assertEquals(2, res.get(0).get("a1"));
                assertEquals(1, res.get(1).get("a1"));
            }

        }
    }

    @Test
    public void atomicCounterTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1234, scan(manager, "SELECT n1 FROM tblspace1.tsql", Collections.emptyList()).consume().get(0).get("n1"));
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=n1+1 where k1=?", Arrays.asList("mykey")).getUpdateCount());
            assertEquals(1235, scan(manager, "SELECT n1 FROM tblspace1.tsql", Collections.emptyList()).consume().get(0).get("n1"));
            assertEquals(Long.valueOf(1236), scan(manager, "SELECT n1+1 FROM tblspace1.tsql", Collections.emptyList()).consume().get(0).get(0));
            assertEquals(Long.valueOf(1234), scan(manager, "SELECT n1-1 FROM tblspace1.tsql", Collections.emptyList()).consume().get(0).get(0));
            assertEquals(1235, scan(manager, "SELECT n1 FROM tblspace1.tsql WHERE n1+1=1236", Collections.emptyList()).consume().get(0).get(0));
            assertEquals(1235, scan(manager, "SELECT n1 FROM tblspace1.tsql WHERE n1+n1=2470", Collections.emptyList()).consume().get(0).get(0));
        }
    }

    @Test
    public void selectWithParameters() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            assumeTrue(manager.getPlanner() instanceof SQLPlanner);
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT ? as foo, k1, n1 FROM tblspace1.tsql", Arrays.asList("test"));) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
                assertEquals(RawString.of("test"), all.get(0).get("foo"));
                assertEquals(RawString.of("mykey"), all.get(0).get("k1"));
                assertEquals(Integer.valueOf(1234), all.get(0).get("n1"));
            }

            Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
            try (DataScanner scan = scan(manager, "SELECT ? as foo, ? as bar  FROM tblspace1.tsql", Arrays.asList(Long.valueOf(1), timestamp));) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
                assertEquals(Long.valueOf(1), all.get(0).get("foo"));
                assertEquals(timestamp, all.get(0).get("bar"));
            }

            try (DataScanner scan = scan(manager, "SELECT MAX(?) as foo, MIN(?) as bar  FROM tblspace1.tsql", Arrays.asList(Long.valueOf(1), timestamp));) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
                assertEquals(Long.valueOf(1), all.get(0).get("foo"));
                assertEquals(timestamp, all.get(0).get("bar"));
            }

            executeUpdate(manager, "DELETE FROM tblspace1.tsql", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT ? as foo, ? as bar  FROM tblspace1.tsql", Arrays.asList(Long.valueOf(1), timestamp));) {
                List<DataAccessor> all = scan.consume();
                assertEquals(0, all.size());
            }

            try (DataScanner scan = scan(manager, "SELECT MAX(?) as foo, MIN(?) as bar  FROM tblspace1.tsql", Arrays.asList(Long.valueOf(1), timestamp));) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
                assertNull(all.get(0).get("foo"));
                assertNull(all.get(0).get("bar"));
            }
        }
    }

    @Test
    public void insertTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql values(?,?,?)", Arrays.asList("mykey2", Integer.valueOf(1234), "value")).getUpdateCount());

            assertEquals(2, scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume().size());

        }
    }

    @Test
    public void updateTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1234), "value1")).getUpdateCount());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where s1='value1'", Collections.emptyList()).consume().size());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set s1=k1  where k1=?", Arrays.asList("mykey")).getUpdateCount());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where s1='mykey'", Collections.emptyList()).consume().size());
        }
    }

    @Test
    public void insertJdbcParametersTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            }
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(n1,k1) values(?,?)", Arrays.asList(Integer.valueOf(1234), "mykey2")).getUpdateCount());
            }
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(n1,k1,s1) values(?,?,?)", Arrays.asList(Integer.valueOf(1234), "mykey3", "string2")).getUpdateCount());
            }
        }
    }

    @Test
    public void limitsTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            // scan performed at "scan time"
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 1", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
                assertEquals(RawString.of("mykey"), result.get(0).get("k1"));
            }
            if (manager.getPlanner() instanceof SQLPlanner) {
                try (DataScanner scan1 = scan(manager, "SELECT TOP 1 * FROM tblspace1.tsql ORDER BY k1", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get("k1"));
                }
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 1,1", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
                assertEquals(RawString.of("mykey2"), result.get(0).get("k1"));
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 1,2", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(2, result.size());
                assertEquals(RawString.of("mykey2"), result.get(0).get("k1"));
                assertEquals(RawString.of("mykey3"), result.get(1).get("k1"));
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(4, result.size());
                assertEquals(RawString.of("mykey"), result.get(0).get("k1"));
                assertEquals(RawString.of("mykey2"), result.get(1).get("k1"));
                assertEquals(RawString.of("mykey3"), result.get(2).get("k1"));
                assertEquals(RawString.of("mykey4"), result.get(3).get("k1"));
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 10,10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(0, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 4,10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(0, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql ORDER BY k1 LIMIT 3,10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
                assertEquals(RawString.of("mykey4"), result.get(0).get("k1"));
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT 3", Collections.emptyList(), 2, TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(2, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT ?", Arrays.asList(3), TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT 1,2", Collections.emptyList(), TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(2, result.size());
            }
            if (manager.getPlanner() instanceof CalcitePlanner) {
                try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT ?,?", Arrays.asList(1, 2), TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(2, result.size());
                }
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql "
                    + "WHERE k1 <> ? LIMIT ?", Arrays.asList("aaa", 3), TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql "
                    + "WHERE k1 <> ? ORDER BY k1 LIMIT ?", Arrays.asList("aaa", 3), TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT k1, count(*) FROM tblspace1.tsql "
                    + "WHERE k1 <> ? GROUP BY k1 ORDER BY k1 LIMIT ?", Arrays.asList("aaa", 3), TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT ?", Arrays.asList(0), TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(4, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT ?", Collections.emptyList(), TransactionContext.NO_TRANSACTION);) {
                fail();
            } catch (MissingJDBCParameterException err) {
                assertEquals(1, err.getIndex());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql "
                    + "ORDER BY k1 LIMIT 3", Collections.emptyList(), 5, TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql "
                    + "ORDER BY k1 ", Collections.emptyList(), 2, TransactionContext.NO_TRANSACTION);) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(2, result.size());
            }
        }
    }

    @Test
    public void simpleExitLoopTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {

                try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT 1", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                }
                try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT 2", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(2, result.size());
                }
                try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql LIMIT 10", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(4, result.size());
                }
            }
        }
    }

    @Test
    public void orderByAliasTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql as tt ORDER BY tt.N1", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
                assertEquals(RawString.of("mykey"), result.get(0).get("k1"));
                assertEquals(RawString.of("mykey2"), result.get(1).get("k1"));
                assertEquals(RawString.of("mykey3"), result.get(2).get("k1"));
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql as tt ORDER BY tt.N1 desc", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
                assertEquals(RawString.of("mykey3"), result.get(0).get("k1"));
                assertEquals(RawString.of("mykey2"), result.get(1).get("k1"));
                assertEquals(RawString.of("mykey"), result.get(2).get("k1"));

            }
        }
    }

    @Test
    public void orderByAlias2Test() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string primary key,N1 int,S1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql as tt ORDER BY tt.n1", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
                assertEquals(RawString.of("mykey"), result.get(0).get("k1"));
                assertEquals(RawString.of("mykey2"), result.get(1).get("k1"));
                assertEquals(RawString.of("mykey3"), result.get(2).get("k1"));
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql as tt ORDER BY tt.n1 desc", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
                assertEquals(RawString.of("mykey3"), result.get(0).get("k1"));
                assertEquals(RawString.of("mykey2"), result.get(1).get("k1"));
                assertEquals(RawString.of("mykey"), result.get(2).get("k1"));

            }
        }
    }

    @Test
    public void limitsAggregatesTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            // scan performed after aggregation
            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,k1 "
                    + "FROM tblspace1.tsql "
                    + "GROUP BY k1 "
                    + "ORDER BY k1 DESC", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(4, result.size());
                assertEquals(RawString.of("mykey4"), result.get(0).get("k1"));
                assertEquals(Long.valueOf(1), result.get(0).get("cc"));
                assertEquals(RawString.of("mykey3"), result.get(1).get("k1"));
                assertEquals(Long.valueOf(1), result.get(1).get("cc"));
                assertEquals(RawString.of("mykey2"), result.get(2).get("k1"));
                assertEquals(Long.valueOf(1), result.get(2).get("cc"));
                assertEquals(RawString.of("mykey"), result.get(3).get("k1"));
                assertEquals(Long.valueOf(1), result.get(3).get("cc"));
            }

            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,k1 FROM tblspace1.tsql GROUP BY k1 ORDER BY k1 DESC LIMIT 1,1", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
                assertEquals(RawString.of("mykey3"), result.get(0).get("k1"));
                assertEquals(Long.valueOf(1), result.get(0).get("cc"));
            }

            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as alias,k1 FROM tblspace1.tsql GROUP BY k1 ORDER BY k1 DESC LIMIT 1,2", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(2, result.size());
                assertEquals(RawString.of("mykey3"), result.get(0).get("k1"));
                assertEquals(Long.valueOf(1), result.get(0).get("alias"));
                assertEquals(RawString.of("mykey2"), result.get(1).get("k1"));
                assertEquals(Long.valueOf(1), result.get(1).get("alias"));
            }
            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,k1 FROM tblspace1.tsql GROUP BY k1 ORDER BY k1 DESC LIMIT 10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(4, result.size());
                assertEquals(RawString.of("mykey4"), result.get(0).get("k1"));
                assertEquals(Long.valueOf(1), result.get(0).get("cc"));
                assertEquals(RawString.of("mykey3"), result.get(1).get("k1"));
                assertEquals(Long.valueOf(1), result.get(1).get("cc"));
                assertEquals(RawString.of("mykey2"), result.get(2).get("k1"));
                assertEquals(Long.valueOf(1), result.get(2).get("cc"));
                assertEquals(RawString.of("mykey"), result.get(3).get("k1"));
                assertEquals(Long.valueOf(1), result.get(3).get("cc"));
            }

            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*),k1 FROM tblspace1.tsql GROUP BY k1 ORDER BY k1 DESC LIMIT 10,10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(0, result.size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*),k1 FROM tblspace1.tsql GROUP BY k1 ORDER BY k1 DESC LIMIT 4,10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(0, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,k1 FROM tblspace1.tsql GROUP BY k1 ORDER BY k1 DESC LIMIT 3,10", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
                assertEquals(RawString.of("mykey"), result.get(0).get("k1"));
                assertEquals(Long.valueOf(1), result.get(0).get("cc"));
            }
            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,n1 FROM tblspace1.tsql GROUP BY n1 ORDER BY cc", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(3, result.size());
                assertNull(result.get(0).get("n1"));
                assertEquals(Long.valueOf(1), result.get(0).get("cc"));

                assertEquals(Integer.valueOf(1), result.get(1).get("n1"));
                assertEquals(Long.valueOf(1), result.get(1).get("cc"));

                assertEquals(Integer.valueOf(2), result.get(2).get("n1"));
                assertEquals(Long.valueOf(2), result.get(2).get("cc"));
            }

            try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,n1"
                    + " FROM tblspace1.tsql"
                    + " WHERE n1 is not null"
                    + " GROUP BY n1"
                    + " ORDER BY cc", Collections.emptyList());) {
                List<DataAccessor> result = scan1.consume();
                for (DataAccessor ac : result) {
                    System.out.println("ac:" + ac.toMap());
                }

                assertEquals(Integer.valueOf(1), result.get(0).get("n1"));
                assertEquals(Long.valueOf(1), result.get(0).get("cc"));

                assertEquals(Integer.valueOf(2), result.get(1).get("n1"));
                assertEquals(Long.valueOf(2), result.get(1).get("cc"));
                assertEquals(2, result.size());

            }
        }
    }

    @Test
    public void simpleCountTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {

                try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(Long.valueOf(4), result.get(0).get(0));
                    assertEquals(Long.valueOf(4), result.get(0).get("cc"));
                }
            }
            if (manager.getPlanner() instanceof SQLPlanner) {
                try (DataScanner scan1 = scan(manager, "SELECT COUNT(*)  FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(Long.valueOf(4), result.get(0).get(0));
                    assertEquals(Long.valueOf(4), result.get(0).get("count(*)"));
                }
            }
            {

                try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc FROM tblspace1.tsql WHERE k1='mykey3'", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(Long.valueOf(1), result.get(0).get(0));
                    assertEquals(Long.valueOf(1), result.get(0).get("cc"));
                }
            }
            {

                try (DataScanner scan1 = scan(manager, "SELECT COUNT(*),k1 FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    Assert.fail();
                } catch (StatementExecutionException error) {
                    assertTrue("field k1 MUST appear in GROUP BY clause".equals(error.getMessage())
                            || error.getMessage().equals("From line 1, column 17 to line 1, column 18: Expression 'K1' is not being grouped"));
                }
            }
            {
                try (DataScanner scan1 = scan(manager, "SELECT COUNT(*) as cc,k1 FROM tblspace1.tsql GROUP BY k1", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(4, result.size());
                    for (DataAccessor t : result) {
                        assertEquals(Long.valueOf(1), t.get("cc"));
                        switch (t.get("k1") + "") {
                            case "mykey":
                            case "mykey2":
                            case "mykey3":
                            case "mykey4":
                                break;
                            default:
                                fail();
                        }
                    }

                }
            }

        }
    }

    @Test
    public void simpleSumTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {

                try (DataScanner scan1 = scan(manager, "SELECT SUM(n1) as cc FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(Long.valueOf(8), result.get(0).get(0));
                    assertEquals(Long.valueOf(8), result.get(0).get("cc"));
                }
            }

            {

                try (DataScanner scan1 = scan(manager, "SELECT SUM(n1) as cc,s1 FROM tblspace1.tsql GROUP BY s1 ORDER BY s1", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(3, result.size());
                    assertEquals(Long.valueOf(3), result.get(0).get(0));
                    assertEquals(Long.valueOf(3), result.get(0).get("cc"));
                    assertEquals(RawString.of("a"), result.get(0).get(1));
                    assertEquals(RawString.of("a"), result.get(0).get("s1"));

                    assertEquals(Long.valueOf(5), result.get(1).get(0));
                    assertEquals(Long.valueOf(5), result.get(1).get("cc"));
                    assertEquals(RawString.of("b"), result.get(1).get(1));
                    assertEquals(RawString.of("b"), result.get(1).get("s1"));

                    assertEquals(Long.valueOf(0), result.get(2).get(0));
                    assertEquals(Long.valueOf(0), result.get(2).get("cc"));
                    assertEquals(null, result.get(2).get(1));
                    assertEquals(null, result.get(2).get("s1"));
                }
            }

            {

                try (DataScanner scan1 = scan(manager, "SELECT SUM(n1) as asum,s1 FROM tblspace1.tsql GROUP BY s1 ORDER BY s1", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(3, result.size());
                    assertEquals(Long.valueOf(3), result.get(0).get(0));
                    assertEquals(Long.valueOf(3), result.get(0).get("asum"));
                    assertEquals(RawString.of("a"), result.get(0).get(1));
                    assertEquals(RawString.of("a"), result.get(0).get("s1"));

                    assertEquals(Long.valueOf(5), result.get(1).get(0));
                    assertEquals(Long.valueOf(5), result.get(1).get("asum"));
                    assertEquals(RawString.of("b"), result.get(1).get(1));
                    assertEquals(RawString.of("b"), result.get(1).get("s1"));

                    assertEquals(Long.valueOf(0), result.get(2).get(0));
                    assertEquals(Long.valueOf(0), result.get(2).get("asum"));
                    assertEquals(null, result.get(2).get(1));
                    assertEquals(null, result.get(2).get("s1"));
                }
            }

            {

                try (DataScanner scan1 = scan(manager, "SELECT SUM(n1) as asum,s1 FROM tblspace1.tsql GROUP BY s1 ORDER BY asum", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(3, result.size());

                    assertEquals(Long.valueOf(0), result.get(0).get(0));
                    assertEquals(Long.valueOf(0), result.get(0).get("asum"));
                    assertEquals(null, result.get(0).get(1));
                    assertEquals(null, result.get(0).get("s1"));

                    assertEquals(Long.valueOf(3), result.get(1).get(0));
                    assertEquals(Long.valueOf(3), result.get(1).get("asum"));
                    assertEquals(RawString.of("a"), result.get(1).get(1));
                    assertEquals(RawString.of("a"), result.get(1).get("s1"));

                    assertEquals(Long.valueOf(5), result.get(2).get(0));
                    assertEquals(Long.valueOf(5), result.get(2).get("asum"));
                    assertEquals(RawString.of("b"), result.get(2).get(1));
                    assertEquals(RawString.of("b"), result.get(2).get("s1"));

                }
            }

            {
                try (DataScanner scan1 = scan(manager, "SELECT SUM(1) as cc FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(Long.valueOf(4), result.get(0).get(0));
                    assertEquals(Long.valueOf(4), result.get(0).get("cc"));
                }
            }

        }
    }

    @Test
    public void simpleMinMaxTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {

                try (DataScanner scan1 = scan(manager, "SELECT MIN(n1) as mi, MAX(n1) as ma FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(Integer.valueOf(1), result.get(0).get(0));
                    assertEquals(Integer.valueOf(1), result.get(0).get("mi"));

                    assertEquals(Integer.valueOf(5), result.get(0).get(1));
                    assertEquals(Integer.valueOf(5), result.get(0).get("ma"));
                }
            }

            {

                try (DataScanner scan1 = scan(manager, "SELECT MIN(n1) as mi, MAX(n1) as ma FROM tblspace1.tsql WHERE k1='mykey4'", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertNull(result.get(0).get(0));
                    assertNull(result.get(0).get("mi"));

                    assertNull(result.get(0).get(1));
                    assertNull(result.get(0).get("ma"));
                }
            }

            {

                try (DataScanner scan1 = scan(manager, "SELECT MIN(n1) as mi, MAX(n1) as ma FROM tblspace1.tsql WHERE k1='no_results'", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertNull(result.get(0).get(0));
                    assertNull(result.get(0).get("mi"));

                    assertNull(result.get(0).get(1));
                    assertNull(result.get(0).get("ma"));
                }
            }

            {

                try (DataScanner scan1 = scan(manager, "SELECT MIN(n1) as mi, MAX(n1) as ma FROM tblspace1.tsql WHERE k1='no_results' GROUP BY k1", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(0, result.size());
                }
            }

        }
    }

    @Test
    public void simpleMinMaxTimestampTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string, t1 timestamp)", Collections.emptyList());

            java.sql.Timestamp time1 = new java.sql.Timestamp(System.currentTimeMillis());
            java.sql.Timestamp time2 = new java.sql.Timestamp(time1.getTime() + 60000);

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,t1) values(?,?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a", time1)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,t1) values(?,?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a", time2)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,t1) values(?,?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b", time1)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {

                try (DataScanner scan1 = scan(manager, "SELECT MIN(t1) as mi, MAX(t1) as ma FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(time1, result.get(0).get(0));
                    assertEquals(time1, result.get(0).get("mi"));

                    assertEquals(time2, result.get(0).get(1));
                    assertEquals(time2, result.get(0).get("ma"));
                }
            }

        }
    }

    @Test
    public void simpleComparatorTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey3", Integer.valueOf(3), "a")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey4", Integer.valueOf(-1), "a")).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=1", Collections.emptyList());) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1>1", Collections.emptyList());) {
                assertEquals(2, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1>=1", Collections.emptyList());) {
                assertEquals(3, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1<1", Collections.emptyList());) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1<-7", Collections.emptyList());) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1<=-1", Collections.emptyList());) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1<>1", Collections.emptyList());) {
                assertEquals(3, scan1.consume().size());
            }

        }
    }

    @Test
    public void scalarFunctionTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());

            {

                try (DataScanner scan1 = scan(manager, "SELECT lower(k1) as low, upper(k1) as up, k1 FROM tblspace1.tsql", Collections.emptyList());) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(1, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get(0));
                    assertEquals(RawString.of("MYKEY"), result.get(0).get(1));
                    assertEquals(RawString.of("mykey"), result.get(0).get(2));

                }
            }
        }
    }

    @Test
    public void orderByTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(4, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get(0));
                    assertEquals(RawString.of("mykey2"), result.get(1).get(0));
                    assertEquals(RawString.of("mykey3"), result.get(2).get(0));
                    assertEquals(RawString.of("mykey4"), result.get(3).get(0)); // NULLS LAST
                }
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1 desc", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(4, result.size());
                    assertEquals(RawString.of("mykey"), result.get(3).get(0));
                    assertEquals(RawString.of("mykey2"), result.get(2).get(0));
                    assertEquals(RawString.of("mykey3"), result.get(1).get(0));
                    assertEquals(RawString.of("mykey4"), result.get(0).get(0));
                }
            }
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey5", Integer.valueOf(3))).getUpdateCount());
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1, k1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(5, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get(0));
                    assertEquals(RawString.of("mykey2"), result.get(1).get(0));
                    assertEquals(RawString.of("mykey3"), result.get(2).get(0));
                    assertEquals(RawString.of("mykey5"), result.get(3).get(0));
                    assertEquals(RawString.of("mykey4"), result.get(4).get(0)); // NULLS LAST
                }
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1, k1 desc", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(5, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get(0));
                    assertEquals(RawString.of("mykey2"), result.get(1).get(0));
                    assertEquals(RawString.of("mykey5"), result.get(2).get(0));
                    assertEquals(RawString.of("mykey3"), result.get(3).get(0));
                    assertEquals(RawString.of("mykey4"), result.get(4).get(0)); // NULLS LAST
                }
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1, k1 desc limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(2, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get(0));
                    assertEquals(RawString.of("mykey2"), result.get(1).get(0));
                }
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1 asc limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(2, result.size());
                    assertEquals(RawString.of("mykey"), result.get(0).get(0));
                    assertEquals(RawString.of("mykey2"), result.get(1).get(0));
                }
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql order by n1 desc limit 3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> result = scan1.consume();
                    assertEquals(3, result.size());
                    assertEquals(RawString.of("mykey4"), result.get(0).get(0));
                    assertEquals(RawString.of("mykey5"), result.get(1).get(0));
                    assertEquals(RawString.of("mykey3"), result.get(2).get(0));
                }
            }

        }
    }

    @Test
    public void indexSeek() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                        " SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant"
                        + " FROM tblspace1.tsql"
                        + " where k1 ='mykey'", Collections.emptyList(), true, true, false, -1);

                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexSeek);
                try (DataScanner scan1
                        = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner;) {
                    List<DataAccessor> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(3, records.get(0).getFieldNames().length);
                    assertEquals(3, records.get(0).toMap().size());
                    assertEquals("thekey", records.get(0).getFieldNames()[0].toLowerCase());
                    assertEquals(RawString.of("mykey"), records.get(0).get("theKey"));
                    assertEquals("thestringconstant", records.get(0).getFieldNames()[1].toLowerCase());
                    assertEquals(RawString.of("one"), records.get(0).get("theStringConstant"));
                    assertEquals("longconstant", records.get(0).getFieldNames()[2].toLowerCase());
                    assertEquals(3L, ((Number) records.get(0).get("LongConstant")).longValue());
                }
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 = 'mykey_no'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    assertTrue(scan1.consume().isEmpty());
                }
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 = 'mykey' and n1<>1234", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    assertTrue(scan1.consume().isEmpty());
                }
            }
        }
    }

    @Test
    public void aliasTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 theKey FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                PlannerOp plannerOp = translate1.plan.mainStatement.unwrap(PlannerOp.class);
                System.out.println("plannerOp:" + plannerOp);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> records = scan1.consume();
                    assertEquals(0, records.size());
                    assertEquals(1, scan1.getFieldNames().length);
                    assertEquals("thekey", scan1.getFieldNames()[0].toLowerCase());
                }
            }
        }
    }

    @Test
    public void basicTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = result.getRecord().toBean(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals(RawString.of("mykey"), finalRecord.get("k1"));
                assertEquals(Integer.valueOf(1234), finalRecord.get("n1"));
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ?", Arrays.asList(Integer.valueOf(999), "mykey")).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and n1 = ?", Arrays.asList(Integer.valueOf(100), "mykey", Integer.valueOf(999))).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 = ? or n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(100), Integer.valueOf(1000))).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 <> ?)", Arrays.asList(Integer.valueOf(34), "mykey", Integer.valueOf(15))).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and not (n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(34))).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = result.getRecord().toBean(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals(RawString.of("mykey"), finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey"), false, true, false, -1);
                GetStatement st_get = (GetStatement) translate1.plan.mainStatement;
                GetResult result = manager.get(st_get, translate1.context, TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = result.getRecord().toBean(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals(RawString.of("mykey"), finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 999), false, true, false, -1);
                GetStatement st_get_with_condition = (GetStatement) translate1.plan.mainStatement;
                GetResult result = manager.get(st_get_with_condition, translate1.context, TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = result.getRecord().toBean(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals(RawString.of("mykey"), finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 9992), false, true, false, -1);
                GetStatement st_get_with_wrong_condition = (GetStatement) translate1.plan.mainStatement;
                GetResult result = manager.get(st_get_with_wrong_condition, translate1.context, TransactionContext.NO_TRANSACTION);
                assertFalse(result.found());
            }
            {
                assertEquals(0, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 = ? and n1 = ?", Arrays.asList("mykey", 123)).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey")).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertFalse(result.found());
            }
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
            }

            {
                assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 1234)).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertFalse(result.found());
            }

            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values('mykey2',1234)", Collections.emptyList()).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2135 where k1 = 'mykey2'", Collections.emptyList()).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='foo' where k1 = 'mykey2' and s1 is null", Collections.emptyList()).getUpdateCount());
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(1, scan1.consume().size());
                }

            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> records = scan1.consume();
                    assertEquals(1, records.size());
                    System.out.println("records:" + records);
                    assertEquals(1, records.get(0).getFieldNames().length);
                    assertEquals(1, records.get(0).toMap().size());
                    assertEquals("k1", records.get(0).getFieldNames()[0]);
                    assertEquals(RawString.of("mykey2"), records.get(0).get(0));
                }
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 theKey FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                PlannerOp plannerOp = translate1.plan.mainStatement.unwrap(PlannerOp.class);
                System.out.println("plannerOp:" + plannerOp);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    List<DataAccessor> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(1, records.get(0).getFieldNames().length);
                    assertEquals(1, records.get(0).toMap().size());
                    assertEquals("thekey", records.get(0).getFieldNames()[0].toLowerCase());
                    System.out.println("type: " + records.get(0).getClass());
                    assertEquals(RawString.of("mykey2"), records.get(0).get(0));
                }
            }
            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true, false, -1);
                try (DataScanner scan1 = ((ScanResult) manager.executePlan(translate1.plan,
                        translate1.context, TransactionContext.NO_TRANSACTION)).dataScanner;) {
                    List<DataAccessor> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(3, records.get(0).getFieldNames().length);
                    assertEquals(3, records.get(0).toMap().size());
                    assertEquals("thekey", records.get(0).getFieldNames()[0].toLowerCase());
                    assertEquals(RawString.of("mykey2"), records.get(0).get(0));
                    assertEquals("thestringconstant", records.get(0).getFieldNames()[1].toLowerCase());
                    assertEquals(RawString.of("one"), records.get(0).get(1));
                    assertEquals("longconstant", records.get(0).getFieldNames()[2].toLowerCase());
                    assertEquals(3, ((Number) records.get(0).get(2)).longValue());
                }
            }

            {
                TranslatedQuery translate1 = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.tsql where k1 ='mykey2' and s1 is not null", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translate1.plan.mainStatement.unwrap(ScanStatement.class);
                try (DataScanner scan1 = manager.scan(scan, translate1.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(1, scan1.consume().size());
                }

            }
            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is not null", Collections.emptyList()).getUpdateCount());
            }
            {
                assertEquals(0, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is null", Collections.emptyList()).getUpdateCount());
            }
            {
                assertEquals(0, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and not (s1 is not null)", Collections.emptyList()).getUpdateCount());
            }
            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and not (s1 is null)", Collections.emptyList()).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "DELETE FROM  tblspace1.tsql where k1 = 'mykey2' and s1 is not null", Collections.emptyList()).getUpdateCount());
                assertEquals(0, executeUpdate(manager, "DELETE FROM  tblspace1.tsql where k1 = 'mykey2' and s1 is not null", Collections.emptyList()).getUpdateCount());
            }
            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey2"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertFalse(result.found());
            }

            {
                TransactionResult result = (TransactionResult) execute(manager, "EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());
                long tx = result.getTransactionId();
                execute(manager, "EXECUTE COMMITTRANSACTION 'tblspace1'," + tx, Collections.emptyList());;
            }
            {
                TransactionResult result = (TransactionResult) execute(manager, "EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());
                long tx = result.getTransactionId();
                execute(manager, "EXECUTE ROLLBACKTRANSACTION 'tblspace1'," + tx, Collections.emptyList());
            }
        }

    }

    @Test
    public void multipleColumnPrimaryKeyTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string,"
                    + "n1 int,"
                    + "s1 string, "
                    + "primary key (k1,n1)"
                    + ")", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList());) {
                assertEquals(1, scan1.consume().size());
            }

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1235))).getUpdateCount());
            try (DataScanner scan1 = scan(manager, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList());) {
                assertEquals(2, scan1.consume().size());
            }
            try {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1235))).getUpdateCount());
                fail();
            } catch (DuplicatePrimaryKeyException err) {
            }
            try (DataScanner scan1 = scan(manager, "SELECT k1,n1  FROM tblspace1.tsql where k1 ='mykey' order by n1", Collections.emptyList());) {
                List<DataAccessor> rows = scan1.consume();
                assertEquals(2, rows.size());
                assertEquals(1234, rows.get(0).get("n1"));
                assertEquals(1235, rows.get(1).get("n1"));

            }
            try (DataScanner scan1 = scan(manager, "SELECT k1,n1 FROM tblspace1.tsql where k1 ='mykey' and n1=1234", Collections.emptyList());) {
                assertEquals(1, scan1.consume().size());
            }
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(n1,k1) values(?,?)", Arrays.asList(Integer.valueOf(1236), "mykey")).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT k1,n1  FROM tblspace1.tsql where k1 ='mykey' order by n1 desc", Collections.emptyList());) {
                List<DataAccessor> rows = scan1.consume();
                assertEquals(3, rows.size());
                assertEquals(1236, rows.get(0).get("n1"));
                assertEquals(1235, rows.get(1).get("n1"));
                assertEquals(1234, rows.get(2).get("n1"));

            }

            try (DataScanner scan1 = scan(manager, "SELECT k1,n1 FROM tblspace1.tsql where k1 ='mykey' and n1=1234", Collections.emptyList());) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT k1,n1 FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList());) {
                assertEquals(3, scan1.consume().size());
            }

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1 =? and n1=?", Arrays.asList("newvalue", "mykey", Integer.valueOf(1236))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT k1,n1,s1 FROM tblspace1.tsql where k1 ='mykey' and n1=1236", Collections.emptyList());) {
                List<DataAccessor> rows = scan1.consume();
                assertEquals(1, rows.size());
                assertEquals(RawString.of("newvalue"), rows.get(0).get("s1"));

            }

            assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 =? and n1=?", Arrays.asList("mykey", Integer.valueOf(1236))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT k1,n1 FROM tblspace1.tsql where k1 ='mykey' and n1=1236", Collections.emptyList());) {
                assertEquals(0, scan1.consume().size());
            }
        }
    }

    @Test
    public void updateSingleRowWithPredicate() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.q1_MESSAGE (MSG_ID bigint primary key,status int, lastbouncecategory tinyint)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_MESSAGE(msg_id,status, lastbouncecategory) values(?,?,?)", Arrays.asList(2, 1, null)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_MESSAGE(msg_id,status, lastbouncecategory) values(?,?,?)", Arrays.asList(3, 1, null)).getUpdateCount());

            try (DataScanner resultSet = scan(manager, "SELECT * FROM tblspace1.q1_MESSAGE", Collections.emptyList());) {
                List<DataAccessor> consume = resultSet.consume();
                for (DataAccessor tuple : consume) {
                    System.out.println("tuple1:" + tuple.toMap());
                }
            }

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.q1_MESSAGE set status=?, lastbouncecategory=null where MSG_ID=? and (status = 1 or status=5)", Arrays.asList(4, 2)).getUpdateCount());

            try (DataScanner resultSet = scan(manager, "SELECT * FROM tblspace1.q1_MESSAGE", Collections.emptyList());) {
                List<DataAccessor> consume = resultSet.consume();
                for (DataAccessor tuple : consume) {
                    System.out.println("tuple2:" + tuple.toMap());
                }
            }
        }
    }

    @Test
    public void betweenTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,ts timestamp,l1 bigint)", Collections.emptyList());

            long now = System.currentTimeMillis();

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,ts,l1) values(?,?,?,?)", Arrays.asList("mykey", Integer.valueOf(1234), new java.sql.Timestamp(now), Long.valueOf(2234))).getUpdateCount());

            // integer
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where n1 between 1234 and 1234", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where n1 between 1234 and 1235", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where n1 between 1233 and 1234", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where n1 between 1200 and 1239", Collections.emptyList()).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql where n1 between 0 and -1", Collections.emptyList()).consume().size());

            // long
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where l1 between 2234 and 2234", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where l1 between 2234 and 2235", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where l1 between 2233 and 2234", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where l1 between 2200 and 2239", Collections.emptyList()).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql where l1 between 0 and -1", Collections.emptyList()).consume().size());

            // string
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where k1 between 'mykey' and 'mykey'", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where k1 between 'mykey' and 'mykfy'", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where k1 between 'mykdy' and 'mykey'", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where k1 between 'mykay' and 'mykqy'", Collections.emptyList()).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql where k1 between 'mykfy' and 'mykgy'", Collections.emptyList()).consume().size());

            // timestamp
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where ts between ? and ?", Arrays.asList(new java.sql.Timestamp(now), new java.sql.Timestamp(now))).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where ts between ? and ?", Arrays.asList(new java.sql.Timestamp(now), new java.sql.Timestamp(now + 60000))).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where ts between ? and ?", Arrays.asList(new java.sql.Timestamp(now - 1000), new java.sql.Timestamp(now))).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where ts between ? and ?", Arrays.asList(new java.sql.Timestamp(now - 1000), new java.sql.Timestamp(now + 60000))).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql where ts between ? and ?", Arrays.asList(new java.sql.Timestamp(0), new java.sql.Timestamp(1000))).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql where ts between ? and ?", Arrays.asList(new java.sql.Timestamp(now + 1000), new java.sql.Timestamp(now - 1000))).consume().size());

            if (manager.getPlanner() instanceof SQLPlanner) {
                System.out.println("now:" + new java.sql.Timestamp(now));
                assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where ts >= {ts '" + new java.sql.Timestamp(now) + "'}", Collections.emptyList()).consume().size());

                // timestamp with jdbc literals
                assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql where ts between {ts '" + new java.sql.Timestamp(now) + "'} and {ts '" + new java.sql.Timestamp(now) + "'}", Collections.emptyList()).consume().size());

            } else {
                // Calcite interprets JDBC syntax as in UTC Timezone

            }

        }
    }

    @Test
    public void andOrTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string, t1 timestamp)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a")).getUpdateCount());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=2 and n1=1", Collections.emptyList()).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=1 and n1=2", Collections.emptyList()).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=3 and n1=2", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=1 and n1=1", Collections.emptyList()).consume().size());

            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=2 or n1=1", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=1 or n1=2", Collections.emptyList()).consume().size());
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=3 or n1=2", Collections.emptyList()).consume().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql WHERE n1=1 or n1=1", Collections.emptyList()).consume().size());

            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql WHERE not (n1=2) or n1=3", Collections.emptyList()).consume().size());
        }
    }

    @Test
    public void createDropIndexTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            execute(manager, "CREATE INDEX ix1 ON tblspace1.tsql(n1)", Collections.emptyList());
            try {
                execute(manager, "CREATE INDEX ix1 ON tblspace1.tsql(n1)", Collections.emptyList());
                fail();
            } catch (IndexAlreadyExistsException ok) {
            }
            execute(manager, "DROP INDEX tblspace1.ix1", Collections.emptyList());

            execute(manager, "CREATE INDEX ix1 ON tblspace1.tsql(n1)", Collections.emptyList());

            execute(manager, "CREATE HASH INDEX ix_hash ON tblspace1.tsql(n1)", Collections.emptyList());

            try {
                execute(manager, "CREATE BADTYPE INDEX ix_bad ON tblspace1.tsql(n1)", Collections.emptyList());
                fail();
            } catch (StatementExecutionException ok) {
                assertTrue(ok.getMessage().contains("badtype"));
            }

            try {
                execute(manager, "DROP INDEX tblspace1.ix2", Collections.emptyList());
                fail();
            } catch (IndexDoesNotExistException ok) {
            }
            try {
                execute(manager, "DROP INDEX ix1", Collections.emptyList());
                fail();
            } catch (IndexDoesNotExistException ok) {
            }
            try {
                execute(manager, "CREATE INDEX ix2 ON tsql(n1)", Collections.emptyList());
                fail();
            } catch (TableDoesNotExistException ok) {
            }

            try {
                execute(manager, "CREATE INDEX duplicatecolumn ON tblspace1.tsql(n1,n1)", Collections.emptyList());
                fail();
            } catch (StatementExecutionException ok) {
            }

        }
    }

    @Test
    public void createIndexOnTableTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string,"
                    + "INDEX ix1 (n1,s1))", Collections.emptyList());

            execute(manager, "DROP INDEX tblspace1.ix1", Collections.emptyList());

        }
    }
}
