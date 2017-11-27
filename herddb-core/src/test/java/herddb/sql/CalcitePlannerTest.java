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

import herddb.core.DBManager;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.planner.BindableTableScanOp;
import herddb.model.planner.DeleteOp;
import herddb.model.planner.InsertOp;
import herddb.model.planner.LimitedSortedBindableTableScanOp;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.SimpleDeleteOp;
import herddb.model.planner.SimpleInsertOp;
import herddb.model.planner.SimpleUpdateOp;
import herddb.model.planner.SortedBindableTableScanOp;
import herddb.model.planner.TableScanOp;
import herddb.model.planner.UpdateOp;
import herddb.utils.DataAccessor;
import herddb.utils.MapUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.hamcrest.CoreMatchers.instanceOf;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import org.junit.Test;

public class CalcitePlannerTest {

    @Test
    public void simplePlansTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
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
            assertInstanceOf(plan(manager, "select k1 from tblspace1.tsql order by k1"), SortedBindableTableScanOp.class);
            assertInstanceOf(plan(manager, "select k1 from tblspace1.tsql order by k1 limit 10"), LimitedSortedBindableTableScanOp.class);

        }
    }

    @Test
    public void joinsPlansTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
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
                        + " ", Collections.emptyList()).consume();
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
                        + " ", Collections.emptyList()).consume();
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
                        + " ", Arrays.asList(-1000)).consume();
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
                        + " ", Collections.emptyList()).consume();
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
                        + " ", Collections.emptyList()).consume();
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
                        + " ", Collections.emptyList()).consume();
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
        }
    }

    private static PlannerOp plan(final DBManager manager, String query) throws StatementExecutionException {
        TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT,
                query,
                Collections.emptyList(),
                true, true, true, -1);

        return translate.plan.mainStatement.unwrap(SQLPlannedOperationStatement.class
        ).getRootOp();
    }

    private static void assertInstanceOf(PlannerOp plan, Class<?> aClass) {
        Assert.assertTrue("expecting " + aClass + " but found " + plan.getClass(),
                plan.getClass().equals(aClass));
    }

}
