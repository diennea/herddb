/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache Licensename, Version 2.0 (the
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

package herddb.sql;

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;
import herddb.codec.DataAccessorForFullRecord;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.Projection;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.planner.BindableTableScanOp;
import herddb.model.planner.DeleteOp;
import herddb.model.planner.InsertOp;
import herddb.model.planner.LimitedSortedBindableTableScanOp;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp.IdentityProjection;
import herddb.model.planner.ProjectOp.ZeroCopyProjection;
import herddb.model.planner.ProjectOp.ZeroCopyProjection.RuntimeProjectedDataAccessor;
import herddb.model.planner.SimpleDeleteOp;
import herddb.model.planner.SimpleInsertOp;
import herddb.model.planner.SimpleUpdateOp;
import herddb.model.planner.SortOp;
import herddb.model.planner.SortedBindableTableScanOp;
import herddb.model.planner.TableScanOp;
import herddb.model.planner.UpdateOp;
import herddb.server.ServerSideScannerPeer;
import herddb.sql.expressions.AccessCurrentRowExpression;
import herddb.sql.expressions.CompiledEqualsExpression;
import herddb.sql.expressions.ConstantExpression;
import herddb.utils.DataAccessor;
import herddb.utils.MapUtils;
import herddb.utils.RawString;
import herddb.utils.TuplesList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class CalcitePlannerTest {

    @Test
    public void simplePlansTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeThat(manager.getPlanner(), instanceOf(CalcitePlanner.class));

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)", Arrays.asList("mykey", 1234), TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql where k1='mykey'", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }

            { // test ReduceExpressionsRule.FILTER_INSTANCE
                // "n1 = 1 and n1 is not null" -> this must be simplified to "n1 = 1"
                BindableTableScanOp plan = assertInstanceOf(plan(manager, "select * from tblspace1.tsql where n1 = 1 and n1 is not null"), BindableTableScanOp.class);
                ScanStatement statement = (ScanStatement) plan.getStatement();
                Projection projection = statement.getProjection();
                assertThat(projection, instanceOf(IdentityProjection.class));
                SQLRecordPredicate predicate = (SQLRecordPredicate) statement.getPredicate();
                assertThat(predicate.getWhere(), instanceOf(CompiledEqualsExpression.class));
                CompiledEqualsExpression equals = (CompiledEqualsExpression) predicate.getWhere();
                assertThat(equals.getLeft(), instanceOf(AccessCurrentRowExpression.class));
                assertThat(equals.getRight(), instanceOf(ConstantExpression.class));
            }

            assertInstanceOf(plan(manager, "select * from tblspace1.tsql"), TableScanOp.class);
            assertInstanceOf(plan(manager, "select * from tblspace1.tsql where n1=1"), BindableTableScanOp.class);
            assertInstanceOf(plan(manager, "select n1 from tblspace1.tsql"), BindableTableScanOp.class);
            assertInstanceOf(plan(manager, "update tblspace1.tsql set n1=? where k1=?"), SimpleUpdateOp.class);
            assertInstanceOf(plan(manager, "update tblspace1.tsql set n1=? where k1=? and n1=?"), SimpleUpdateOp.class);
            assertInstanceOf(plan(manager, "update tblspace1.tsql set n1=?"
                    + " where n1 in (select b.n1*2 from tblspace1.tsql b)"), UpdateOp.class);
            assertInstanceOf(plan(manager, "delete from tblspace1.tsql where k1=?"), SimpleDeleteOp.class);
            assertInstanceOf(plan(manager, "delete from tblspace1.tsql where k1=? and n1=?"), SimpleDeleteOp.class);
            assertInstanceOf(plan(manager, "delete from tblspace1.tsql where n1 in (select b.n1*2 from tblspace1.tsql b)"), DeleteOp.class);
            assertInstanceOf(plan(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)"), SimpleInsertOp.class);
            assertInstanceOf(plan(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?),(?,?)"), InsertOp.class);

            assertInstanceOf(plan(manager, "select * from tblspace1.tsql order by k1"), SortedBindableTableScanOp.class);
            assertInstanceOf(plan(manager, "select k1 from tblspace1.tsql order by k1"), SortedBindableTableScanOp.class);
            assertInstanceOf(plan(manager, "select k1 from tblspace1.tsql order by k1 limit 10"), LimitedSortedBindableTableScanOp.class);
            {
                BindableTableScanOp plan = assertInstanceOf(plan(manager, "select * from tblspace1.tsql where k1=?"), BindableTableScanOp.class);
                Projection projection = plan.getStatement().getProjection();
                System.out.println("projection:" + projection);
                assertThat(projection, instanceOf(IdentityProjection.class));
                assertNull(plan.getStatement().getComparator());
            }

            {
                SortedBindableTableScanOp plan = assertInstanceOf(plan(manager, "select n1,k1 from tblspace1.tsql order by n1"), SortedBindableTableScanOp.class);
                Projection projection = plan.getStatement().getProjection();
                System.out.println("projection:" + projection);
                assertThat(projection, instanceOf(ZeroCopyProjection.class));
                ZeroCopyProjection zero = (ZeroCopyProjection) projection;
                assertArrayEquals(new String[]{"n1", "k1"}, zero.getFieldNames());
                assertEquals("n1", zero.getColumns()[0].name);
                assertEquals("k1", zero.getColumns()[1].name);
                TupleComparator comparator = plan.getStatement().getComparator();
                System.out.println("comparator:" + comparator);
                assertThat(comparator, instanceOf(SortOp.class));
                SortOp sortOp = (SortOp) comparator;
                assertFalse(sortOp.isOnlyPrimaryKeyAndAscending());
            }

            {
                SortedBindableTableScanOp plan = assertInstanceOf(plan(manager, "select n1,k1 from tblspace1.tsql order by k1"), SortedBindableTableScanOp.class);
                Projection projection = plan.getStatement().getProjection();
                System.out.println("projection:" + projection);
                assertThat(projection, instanceOf(ZeroCopyProjection.class));
                ZeroCopyProjection zero = (ZeroCopyProjection) projection;
                assertArrayEquals(new String[]{"n1", "k1"}, zero.getFieldNames());
                assertEquals("n1", zero.getColumns()[0].name);
                assertEquals("k1", zero.getColumns()[1].name);
                TupleComparator comparator = plan.getStatement().getComparator();
                System.out.println("comparator:" + comparator);
                assertThat(comparator, instanceOf(SortOp.class));
                SortOp sortOp = (SortOp) comparator;
                assertTrue(sortOp.isOnlyPrimaryKeyAndAscending());
            }
            {
                SortedBindableTableScanOp plan = assertInstanceOf(plan(manager, "select * from tblspace1.tsql order by n1"), SortedBindableTableScanOp.class);
                Projection projection = plan.getStatement().getProjection();
                System.out.println("projection:" + projection);
                assertThat(projection, instanceOf(IdentityProjection.class));
                IdentityProjection zero = (IdentityProjection) projection;
                assertArrayEquals(new String[]{"k1", "n1", "s1"}, zero.getFieldNames());
                assertEquals("k1", zero.getColumns()[0].name);
                assertEquals("n1", zero.getColumns()[1].name);
                assertEquals("s1", zero.getColumns()[2].name);
                TupleComparator comparator = plan.getStatement().getComparator();
                System.out.println("comparator:" + comparator);
                assertThat(comparator, instanceOf(SortOp.class));
                SortOp sortOp = (SortOp) comparator;
                assertFalse(sortOp.isOnlyPrimaryKeyAndAscending());
            }
            {
                SortedBindableTableScanOp plan = assertInstanceOf(plan(manager, "select * from tblspace1.tsql order by k1"), SortedBindableTableScanOp.class);
                Projection projection = plan.getStatement().getProjection();
                System.out.println("projection:" + projection);
                assertThat(projection, instanceOf(IdentityProjection.class));
                IdentityProjection zero = (IdentityProjection) projection;
                assertArrayEquals(new String[]{"k1", "n1", "s1"}, zero.getFieldNames());
                assertEquals("k1", zero.getColumns()[0].name);
                assertEquals("n1", zero.getColumns()[1].name);
                assertEquals("s1", zero.getColumns()[2].name);
                TupleComparator comparator = plan.getStatement().getComparator();
                System.out.println("comparator:" + comparator);
                assertThat(comparator, instanceOf(SortOp.class));
                SortOp sortOp = (SortOp) comparator;
                assertTrue(sortOp.isOnlyPrimaryKeyAndAscending());
            }

        }
    }

    @Test
    public void dirtySQLPlansTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeThat(manager.getPlanner(), instanceOf(CalcitePlanner.class));

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "\n\nCREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "\n\nINSERT INTO tblspace1.tsql (k1,n1) values(?,?)", Arrays.asList("mykey", 1234), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql where k1='mykey'", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }

            assertInstanceOf(plan(manager, "-- comment\nselect * from tblspace1.tsql"), TableScanOp.class);
            assertInstanceOf(plan(manager, "/* multiline\ncomment */\nselect * from tblspace1.tsql where n1=1"), BindableTableScanOp.class);
            assertInstanceOf(plan(manager, "\n\nselect n1 from tblspace1.tsql"), BindableTableScanOp.class);
            assertInstanceOf(plan(manager, "-- comment\nupdate tblspace1.tsql set n1=? where k1=?"), SimpleUpdateOp.class);
            assertInstanceOf(plan(manager, "/* multiline\ncomment */\nupdate tblspace1.tsql set n1=? where k1=? and n1=?"), SimpleUpdateOp.class);
            assertInstanceOf(plan(manager, "update tblspace1.tsql set n1=?"
                    + " where n1 in (select b.n1*2 from tblspace1.tsql b)"), UpdateOp.class);
            assertInstanceOf(plan(manager, "-- comment\ndelete from tblspace1.tsql where k1=?"), SimpleDeleteOp.class);
            assertInstanceOf(plan(manager, "/* multiline\ncomment */\ndelete from tblspace1.tsql where k1=? and n1=?"), SimpleDeleteOp.class);
            assertInstanceOf(plan(manager, "\n\ndelete from tblspace1.tsql where n1 in (select b.n1*2 from tblspace1.tsql b)"), DeleteOp.class);
            assertInstanceOf(plan(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)"), SimpleInsertOp.class);
            assertInstanceOf(plan(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?),(?,?)"), InsertOp.class);
            assertInstanceOf(plan(manager, "select k1 from tblspace1.tsql order by k1"), SortedBindableTableScanOp.class);
            assertInstanceOf(plan(manager, "select k1 from tblspace1.tsql order by k1 limit 10"), LimitedSortedBindableTableScanOp.class);
            BindableTableScanOp plan = assertInstanceOf(plan(manager, "select * from tblspace1.tsql where k1=?"), BindableTableScanOp.class);
            Projection projection = plan.getStatement().getProjection();
            System.out.println("projection:" + projection);
            assertThat(projection, instanceOf(IdentityProjection.class));
            assertNull(plan.getStatement().getComparator());

        }
    }

    @Test
    public void joinsPlansTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeThat(manager.getPlanner(), instanceOf(CalcitePlanner.class));
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.tsql2 (k2 string primary key,n2 int,s2 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)", Arrays.asList("mykey", 1234), TransactionContext.NO_TRANSACTION);
            execute(manager, "INSERT INTO tblspace1.tsql2 (k2,n2) values(?,?)", Arrays.asList("mykey", 1234), TransactionContext.NO_TRANSACTION);
            execute(manager, "INSERT INTO tblspace1.tsql2 (k2,n2) values(?,?)", Arrays.asList("mykey2", 1234), TransactionContext.NO_TRANSACTION);
            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2,n1 "
                        + "  from tblspace1.tsql2 join tblspace1.tsql on k2<>k1 "
                        + "  where n2 > 1 "
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                    assertEquals("n1", t.getFieldNames()[2]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234, "n1", 1234
                        ))));

            }
            {
                // theta join
                List<DataAccessor> tuples = scan(manager, "select k2,n2,n1 "
                        + "  from tblspace1.tsql2"
                        + "  left join tblspace1.tsql on n2 < n1-1000"
                        + "  "
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                    assertEquals("n1", t.getFieldNames()[2]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234, "n1", null
                        ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey", "n2", 1234, "n1", null
                        ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2,round(n1/2) as halfn1"
                        + "  from tblspace1.tsql2"
                        + "  left join tblspace1.tsql on n2 < n1-?"
                        + "  "
                        + " ", Arrays.asList(-1000)).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                    assertEquals("halfn1", t.getFieldNames()[2]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234, "halfn1", 617.0
                        ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey", "n2", 1234, "halfn1", 617.0
                        ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2 from tblspace1.tsql2"
                        + "                     where k2 not in (select k1 from tblspace1.tsql)"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234
                        ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2 from tblspace1.tsql2"
                        + "                     where n2 in (select n1 from tblspace1.tsql"
                        + "                                    )"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234
                        ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey", "n2", 1234
                        ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2 from tblspace1.tsql2"
                        + "                     where exists (select * from tblspace1.tsql"
                        + "                                     where n1=n2)"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234
                        ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey", "n2", 1234
                        ))));

            }

            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)", Arrays.asList("mykey6", 1236), TransactionContext.NO_TRANSACTION);
            execute(manager, "INSERT INTO tblspace1.tsql2 (k2,n2) values(?,?)", Arrays.asList("mykey6a", 1236), TransactionContext.NO_TRANSACTION);

            // sort not as top level node
            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2 from tblspace1.tsql2"
                        + "                     where n2 in (select n1 from tblspace1.tsql"
                        + "                                     order by n1 desc limit 1)"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().allMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey6a", "n2", 1236
                        ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "select k2,n2 from tblspace1.tsql2"
                        + "                     where n2 in (select n1 from tblspace1.tsql"
                        + "                                     order by n1 asc limit 1)"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey", "n2", 1234
                        ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                                "k2", "mykey2", "n2", 1234
                        ))));

            }

        }
    }

    @Test
    public void explainPlanTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,"
                    + "s1 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.tsql (k1 ,"
                    + "s1) values('testk1','tests1')", Collections.emptyList());

            try (DataScanner scan = scan(manager, "EXPLAIN SELECT k1 FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> consume = scan.consume();
                assertEquals(4, consume.size());
                consume.get(0).forEach((k, b) -> {
                    System.out.println("k:" + k + " -> " + b);
                });
                consume.get(1).forEach((k, b) -> {
                    System.out.println("k:" + k + " -> " + b);
                });
                consume.get(2).forEach((k, b) -> {
                    System.out.println("k:" + k + " -> " + b);
                });
                consume.get(3).forEach((k, b) -> {
                    System.out.println("k:" + k + " -> " + b);
                });
            }
        }
    }

    @Test
    public void showCreateTableTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.test (k1 string primary key,"
                    + "s1 string not null)", Collections.emptyList());

            execute(manager, "CREATE TABLE tblspace1.test22 (k1 string not null ,"
                    + "s1 string not null, n1 int, primary key(k1,n1))", Collections.emptyList());


            TranslatedQuery translatedQuery = manager.getPlanner().translate("tblspace1", "SHOW CREATE TABLE tblspace1.test", Collections.emptyList(),
                    true, false, true, -1);
            if (translatedQuery.plan.mainStatement instanceof SQLPlannedOperationStatement
                    || translatedQuery.plan.mainStatement instanceof ScanStatement) {
                ScanResult scanResult = (ScanResult) manager.executePlan(translatedQuery.plan, translatedQuery.context, TransactionContext.NO_TRANSACTION);
                DataScanner dataScanner = scanResult.dataScanner;

                ServerSideScannerPeer scanner = new ServerSideScannerPeer(dataScanner);

                String[] columns = dataScanner.getFieldNames();
                List<DataAccessor> records = dataScanner.consume(2);
                TuplesList tuplesList = new TuplesList(columns, records);
                assertTrue(tuplesList.columnNames[0].equalsIgnoreCase("tabledef"));
                Tuple values = (Tuple) records.get(0);
                assertTrue("CREATE TABLE tblspace1.test(k1 string,s1 string not null,PRIMARY KEY(k1))".equalsIgnoreCase(values.get("tabledef").toString()));
            }

            translatedQuery = manager.getPlanner().translate("tblspace1", "SHOW CREATE TABLE tblspace1.test22", Collections.emptyList(),
                    true, false, true, -1);
            if (translatedQuery.plan.mainStatement instanceof SQLPlannedOperationStatement
                    || translatedQuery.plan.mainStatement instanceof ScanStatement) {
                ScanResult scanResult = (ScanResult) manager.executePlan(translatedQuery.plan, translatedQuery.context, TransactionContext.NO_TRANSACTION);
                DataScanner dataScanner = scanResult.dataScanner;

                ServerSideScannerPeer scanner = new ServerSideScannerPeer(dataScanner);

                String[] columns = dataScanner.getFieldNames();
                List<DataAccessor> records = dataScanner.consume(2);
                TuplesList tuplesList = new TuplesList(columns, records);
                assertTrue(tuplesList.columnNames[0].equalsIgnoreCase("tabledef"));
                Tuple values = (Tuple) records.get(0);
                assertTrue("CREATE TABLE tblspace1.test22(k1 string not null,s1 string not null,n1 integer,Primary key(k1,n1))".equalsIgnoreCase(values.get("tabledef").toString()));
            }

        }
    }

    @Test
    public void showCreateTableTest_with_Indexes() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);


            execute(manager, "CREATE TABLE tblspace1.test23 (k1 string not null ,"
                    + "s1 string not null, n1 int, primary key(k1,n1))", Collections.emptyList());

            execute(manager, "CREATE INDEX ixn1s1 on tblspace1.test23(n1,s1)", Collections.emptyList());

            TranslatedQuery translatedQuery = manager.getPlanner().translate("tblspace1", "SHOW CREATE TABLE tblspace1.test23 WITH INDEXES", Collections.emptyList(),
                    true, false, true, -1);
            if (translatedQuery.plan.mainStatement instanceof SQLPlannedOperationStatement
                    || translatedQuery.plan.mainStatement instanceof ScanStatement) {
                ScanResult scanResult = (ScanResult) manager.executePlan(translatedQuery.plan, translatedQuery.context, TransactionContext.NO_TRANSACTION);
                DataScanner dataScanner = scanResult.dataScanner;

                ServerSideScannerPeer scanner = new ServerSideScannerPeer(dataScanner);

                String[] columns = dataScanner.getFieldNames();
                List<DataAccessor> records = dataScanner.consume(2);
                TuplesList tuplesList = new TuplesList(columns, records);
                assertTrue(tuplesList.columnNames[0].equalsIgnoreCase("tabledef"));
                Tuple values = (Tuple) records.get(0);
                assertTrue("CREATE TABLE tblspace1.test23(k1 string not null,s1 string not null,n1 integer,Primary KEY(k1,n1),INDEX ixn1s1(n1,s1))".equalsIgnoreCase(values.get("tabledef").toString()));

                // Drop the table and indexes and recreate them again.
                String showCreateCommandOutput = values.get("tabledef").toString();

                //drop the table
                execute(manager, "DROP TABLE tblspace1.test23", Collections.emptyList());

                //ensure table has been dropped
                try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='test23'", Collections.emptyList())) {
                    assertTrue(scan.consume().isEmpty());
                }

                //ensure index has been dropped
                try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.sysindexes where index_name='ixn1s1'", Collections.emptyList())) {
                    assertTrue(scan.consume().isEmpty());
                }

                execute(manager, showCreateCommandOutput, Collections.emptyList());
                // Ensure the table is getting created
                try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='test23'", Collections.emptyList())) {
                    assertFalse(scan.consume().isEmpty());
                }
                // Ensure index got created as well.
                try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.sysindexes where index_name='ixn1s1'", Collections.emptyList())) {
                    assertFalse(scan.consume().isEmpty());
                }
            }
        }
    }

    @Test
    public void showCreateTable_with_recreating_table_from_command_output() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.test23 (k1 int auto_increment ,"
                    + "s1 string not null, n1 int, primary key(k1))", Collections.emptyList());

            TranslatedQuery translatedQuery = manager.getPlanner().translate("tblspace1", "SHOW CREATE TABLE tblspace1.test23", Collections.emptyList(),
                    true, false, true, -1);
            if (translatedQuery.plan.mainStatement instanceof SQLPlannedOperationStatement
                    || translatedQuery.plan.mainStatement instanceof ScanStatement) {
                ScanResult scanResult = (ScanResult) manager.executePlan(translatedQuery.plan, translatedQuery.context, TransactionContext.NO_TRANSACTION);
                DataScanner dataScanner = scanResult.dataScanner;

                ServerSideScannerPeer scanner = new ServerSideScannerPeer(dataScanner);

                String[] columns = dataScanner.getFieldNames();
                List<DataAccessor> records = dataScanner.consume(2);
                TuplesList tuplesList = new TuplesList(columns, records);
                assertTrue(tuplesList.columnNames[0].equalsIgnoreCase("tabledef"));
                Tuple values = (Tuple) records.get(0);
                assertTrue("CREATE TABLE tblspace1.test23(k1 integer auto_increment,s1 string not null,n1 integer,PRIMARY KEY(k1))".equalsIgnoreCase(values.get("tabledef").toString()));

                //drop the table
                execute(manager, "DROP TABLE tblspace1.test23", Collections.emptyList());

                //ensure table has been dropped
                try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='test23'", Collections.emptyList())) {
                    assertTrue(scan.consume().isEmpty());
                }

                //recreate table
                String showCreateTableResult = values.get("tabledef").toString();
                execute(manager, showCreateTableResult, Collections.emptyList());
                // Ensure the table is getting created
                try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='test23'", Collections.emptyList())) {
                    assertFalse(scan.consume().isEmpty());
                }

            }
        }
    }

    @Test(expected = TableDoesNotExistException.class)
    public void showCreateTable_Non_Existent_Table() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            TranslatedQuery translatedQuery = manager.getPlanner().translate("herd", "SHOW CREATE TABLE tblspace1.test223", Collections.emptyList(),
                    true, false, true, -1);
        }
    }

    @Test(expected = TableSpaceDoesNotExistException.class)
    public void showCreateTable_Non_Existent_TableSpace() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();

            TranslatedQuery translatedQuery = manager.getPlanner().translate("herd", "SHOW CREATE TABLE tblspace1.test223", Collections.emptyList(),
                    true, false, true, -1);
        }
    }

    @Test(expected = TableDoesNotExistException.class)
    public void showCreateTable_Within_TransactionContext() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            long tx = beginTransaction(manager, "tblspace1");

            execute(manager, "CREATE TABLE tblspace1.test (k1 string primary key,"
                    + "s1 string not null)", Collections.emptyList(), new TransactionContext(tx));

            TranslatedQuery translatedQuery = manager.getPlanner().translate("herd", "SHOW CREATE TABLE tblspace1.test", Collections.emptyList(),
                    true, false, true, -1);
        }
    }


    @Test
    public void zeroCopyProjectionTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeTrue(manager.getPlanner() instanceof CalcitePlanner);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,"
                    + "s1 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.tsql (k1 ,"
                    + "s1) values('testk1','tests1')", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> consume = scan.consume();
                assertEquals(1, consume.size());
                DataAccessor record = consume.get(0);
                assertThat(record, instanceOf(DataAccessorForFullRecord.class));
                assertEquals("k1", record.getFieldNames()[0]);
                assertEquals("s1", record.getFieldNames()[1]);
                assertEquals(RawString.of("testk1"), record.get(0));
                assertEquals(RawString.of("tests1"), record.get(1));
                assertEquals(RawString.of("testk1"), record.get("k1"));
                assertEquals(RawString.of("tests1"), record.get("s1"));

            }
            try (DataScanner scan = scan(manager, "SELECT k1,s1 FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> consume = scan.consume();
                assertEquals(1, consume.size());
                DataAccessor record = consume.get(0);
                assertThat(record, instanceOf(DataAccessorForFullRecord.class));
                assertEquals("k1", record.getFieldNames()[0]);
                assertEquals("s1", record.getFieldNames()[1]);
                assertEquals(RawString.of("testk1"), record.get(0));
                assertEquals(RawString.of("tests1"), record.get(1));
                assertEquals(RawString.of("testk1"), record.get("k1"));
                assertEquals(RawString.of("tests1"), record.get("s1"));

            }
            try (DataScanner scan = scan(manager, "SELECT s1,k1 FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> consume = scan.consume();
                assertEquals(1, consume.size());
                DataAccessor record = consume.get(0);
                assertThat(record, instanceOf(ZeroCopyProjection.RuntimeProjectedDataAccessor.class));
                assertEquals("k1", record.getFieldNames()[1]);
                assertEquals("s1", record.getFieldNames()[0]);
                assertEquals(RawString.of("testk1"), record.get(1));
                assertEquals(RawString.of("tests1"), record.get(0));
                assertEquals(RawString.of("testk1"), record.get("k1"));
                assertEquals(RawString.of("tests1"), record.get("s1"));
            }
            try (DataScanner scan = scan(manager, "SELECT s1 FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> consume = scan.consume();
                assertEquals(1, consume.size());
                DataAccessor record = consume.get(0);
                assertThat(record, instanceOf(RuntimeProjectedDataAccessor.class));
                assertEquals("s1", record.getFieldNames()[0]);
                assertEquals(RawString.of("tests1"), record.get(0));
                assertEquals(RawString.of("tests1"), record.get("s1"));
            }

            try (DataScanner scan = scan(manager, "SELECT k1 FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> consume = scan.consume();
                assertEquals(1, consume.size());
                DataAccessor record = consume.get(0);
                assertThat(record, instanceOf(RuntimeProjectedDataAccessor.class));
                assertEquals("k1", record.getFieldNames()[0]);
                assertEquals(RawString.of("testk1"), record.get(0));
                assertEquals(RawString.of("testk1"), record.get("k1"));
            }
        }
    }

    private static PlannerOp plan(final DBManager manager, String query) throws StatementExecutionException {
        TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT,
                query,
                Collections.emptyList(),
                true, true, true, -1);

        return translate.plan.originalRoot;
    }

    @SuppressWarnings("unchecked")
    private static <T> T assertInstanceOf(PlannerOp plan, Class<T> aClass) {
        Assert.assertTrue("expecting " + aClass + " but found " + plan.getClass(),
                plan.getClass().equals(aClass));
        return (T) plan;
    }

    @Test
    public void testIsDdl() {

        assertTrue(CalcitePlanner.isDDL("CREATE TABLE"));
        assertTrue(CalcitePlanner.isDDL("   CREATE TABLE `sm_machine`"));
        assertTrue(CalcitePlanner.isDDL("CREATE TABLE `sm_machine` (\n"
                + "  `ip` varchar(20) NOT NULL DEFAULT '',"));
        assertTrue(CalcitePlanner.isDDL("CREATE TABLE `sm_machine` (\n"
                + "  `ip` varchar(20) NOT NULL DEFAULT '',\n"
                + "  `firefox_version` int(50) DEFAULT NULL,\n"
                + "  `chrome_version` int(50) DEFAULT NULL,\n"
                + "  `ie_version` int(50) DEFAULT NULL,\n"
                + "  `log` varchar(2000) DEFAULT NULL,\n"
                + "  `offset` int(50) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`ip`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"));
        assertTrue(CalcitePlanner.isDDL("creaTe TABLE"));
        assertTrue(CalcitePlanner.isDDL("alter table"));
        assertTrue(CalcitePlanner.isDDL("coMMIT transaction"));
        assertTrue(CalcitePlanner.isDDL("rollBACK transaction"));
        assertTrue(CalcitePlanner.isDDL("begin transaction"));
        assertTrue(CalcitePlanner.isDDL("drop table"));
        assertFalse(CalcitePlanner.isDDL("select * from"));
        assertFalse(CalcitePlanner.isDDL("explain select * from"));
    }

}
