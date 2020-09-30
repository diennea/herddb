/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.planner.JoinOp;
import herddb.model.planner.NestedLoopJoinOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.SimpleScanOp;
import herddb.model.planner.SortOp;
import herddb.utils.DataAccessor;
import herddb.utils.MapUtils;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests on basic JOIN queries
 *
 * @author enrico.olivelli
 */
public class SimpleJoinTest {

    @Test
    public void testStartTransactionInJoinBranch() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table2 (k2 string primary key,n2 int,s2 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('a',1,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('b',2,'B')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('c',3,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('d',4,'A')", Collections.emptyList());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2"
                        + " ON t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList(), true, true, false, -1);
                translated.context.setForceRetainReadLock(true);
                if (manager.getPlanner() instanceof CalcitePlanner) {
                    assertThat(translated.plan.originalRoot, instanceOf(NestedLoopJoinOp.class));
                    NestedLoopJoinOp join = (NestedLoopJoinOp) translated.plan.originalRoot;
                    assertThat(join.getLeft(), instanceOf(SimpleScanOp.class));
                    assertThat(join.getRight(), instanceOf(SimpleScanOp.class));
                }
                // we want that the left branch of the join starts the transactoion
                ScanResult scanResult = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.AUTOTRANSACTION_TRANSACTION));
                List<DataAccessor> tuples = scanResult.dataScanner.consumeAndClose();
                assertTrue(scanResult.transactionId > 0);
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());
                TestUtils.commitTransaction(manager, "tblspace1", scanResult.transactionId);
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2"
                        + " ON t1.n1 > 0"
                        + "   and t2.n2 >= 1"
                        + "   order by t1.n1, t2.n2", Collections.emptyList(), true, true, false, -1); // with order by
                translated.context.setForceRetainReadLock(true);

                assertThat(translated.plan.originalRoot, instanceOf(SortOp.class));
                if (manager.getPlanner() instanceof CalcitePlanner) {
                    NestedLoopJoinOp join = (NestedLoopJoinOp) ((SortOp) translated.plan.originalRoot).getInput();
                    assertThat(join.getLeft(), instanceOf(SimpleScanOp.class));
                    assertThat(join.getRight(), instanceOf(SimpleScanOp.class));
                } else {
                    JoinOp join = (JoinOp) ((SortOp) translated.plan.originalRoot).getInput();
                    assertThat(join.getLeft(), instanceOf(SimpleScanOp.class));
                    assertThat(join.getRight(), instanceOf(SimpleScanOp.class));
                }

                // we want that the left branch of the join starts the transactoion
                ScanResult scanResult = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.AUTOTRANSACTION_TRANSACTION));
                List<DataAccessor> tuples = scanResult.dataScanner.consumeAndClose();
                assertTrue(scanResult.transactionId > 0);
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());
                TestUtils.commitTransaction(manager, "tblspace1", scanResult.transactionId);
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT t1.s1 FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2"
                        + " ON t1.n1 > 0"
                        + "   and t2.n2 >= 1"
                        + "   order by t1.n1, t2.n2", Collections.emptyList(), true, true, false, -1); // with order by on non selected fields
                translated.context.setForceRetainReadLock(true);

                assertThat(translated.plan.originalRoot, instanceOf(ProjectOp.class));
                SortOp sort = (SortOp) ((ProjectOp) translated.plan.originalRoot).getInput();
                assertThat(((ProjectOp) translated.plan.originalRoot).getProjection(), instanceOf(ProjectOp.ZeroCopyProjection.class));
                if (manager.getPlanner() instanceof CalcitePlanner) {
                    NestedLoopJoinOp join = (NestedLoopJoinOp) sort.getInput();
                    assertThat(join.getLeft(), instanceOf(SimpleScanOp.class));
                    assertThat(join.getRight(), instanceOf(SimpleScanOp.class));
                } else {
                    JoinOp join = (JoinOp) sort.getInput();
                    assertThat(join.getLeft(), instanceOf(SimpleScanOp.class));
                    assertThat(join.getRight(), instanceOf(SimpleScanOp.class));
                }

                // we want that the left branch of the join starts the transactoion
                ScanResult scanResult = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.AUTOTRANSACTION_TRANSACTION));
                List<DataAccessor> tuples = scanResult.dataScanner.consumeAndClose();
                assertTrue(scanResult.transactionId > 0);
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(1, t.getFieldNames().length);
                    assertEquals("s1", t.getFieldNames()[0]);
                }
                assertEquals(4, tuples.size());
                TestUtils.commitTransaction(manager, "tblspace1", scanResult.transactionId);
            }
        }
    }

    @Test
    public void testSimpleJoinNoWhere() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table2 (k2 string primary key,n2 int,s2 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table3 (k1 string primary key,n3 int,s3 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('a',1,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table3 (k1,n3,s3) values('a',1,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('b',2,'B')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table3 (k1,n3,s3) values('b',3,'B')", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('c',3,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('d',4,'A')", Collections.emptyList());

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table3 t3"
                        + " LEFT JOIN tblspace1.table2 t2 ON t3.n3 = t2.n2",
                        Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n3", t.getFieldNames()[1]);
                    assertEquals("s3", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n3", 1, "s3", "A",
                        "k2", null, "n2", null, "s2", null
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n3", 3, "s3", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table3 t3"
                        + " RIGHT JOIN tblspace1.table2 t2 ON t3.n3 = t2.n2",
                        Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n3", t.getFieldNames()[1]);
                    assertEquals("s3", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n3", 3, "s3", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", null, "n3", null, "s3", null,
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table3 t3"
                        + " FULL OUTER JOIN tblspace1.table2 t2 ON t3.n3 = t2.n2",
                        Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n3", t.getFieldNames()[1]);
                    assertEquals("s3", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(3, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n3", 3, "s3", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", null, "n3", null, "s3", null,
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", null, "n3", null, "s3", null,
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
                        + " tblspace1.table1 t1 "
                        + " NATURAL JOIN tblspace1.table2 t2 "
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a",
                        "k2", "c"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a",
                        "k2", "d"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b",
                        "k2", "c"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b",
                        "k2", "d"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
                        + " tblspace1.table1 t1 "
                        + " NATURAL JOIN tblspace1.table2 t2 "
                        + " WHERE t1.n1 >= 2"
                        + "   and t2.n2 >= 4", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b",
                        "k2", "d"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.*,t2.* FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.* FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t2.* FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                    assertEquals("s2", t.getFieldNames()[2]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t2.s2 FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(1, t.getFieldNames().length);
                    assertEquals("s2", t.getFieldNames()[0]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 > 0"
                        + "   and t2.n2 >= 4", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " NATURAL JOIN tblspace1.table2 t2"
                        + " WHERE t1.n1 <= t2.n2", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM "
                        + " tblspace1.table1 t1 "
                        + " NATURAL JOIN tblspace1.table2 t2 "
                        + " WHERE t1.n1 <= t2.n2 "
                        + "and t2.n2 <= 3", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM "
                        + " tblspace1.table1 t1 "
                        + " JOIN tblspace1.table2 t2 ON t1.n1 <= t2.n2 "
                        + " and t2.n2 <= 3", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1 as first, t2.k1 as seco FROM"
                        + " tblspace1.table1 t1 "
                        + " NATURAL JOIN tblspace1.table3 t2 " // NATURAL JOIN is on columns with the same name and type
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("first", t.getFieldNames()[0]);
                    assertEquals("seco", t.getFieldNames()[1]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "first", "a",
                        "seco", "a"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "first", "b",
                        "seco", "b"
                ))));
            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1 as first, t2.k1 as seco FROM"
                        + " tblspace1.table1 t1 "
                        + " NATURAL JOIN tblspace1.table3 t2 " // NATURAL is on columns with the same name and type
                        + " WHERE t1.n1 + 1 = t2.n3", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("first", t.getFieldNames()[0]);
                    assertEquals("seco", t.getFieldNames()[1]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "first", "b",
                        "seco", "b"
                ))));
            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1 as first, t2.k1 as seco FROM"
                        + " tblspace1.table1 t1 "
                        + " INNER JOIN tblspace1.table3 t2 ON t1.k1=t2.k1" // NATURAL is on columns with the same name and type
                        + " WHERE t1.n1 + 1 = t2.n3", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("first", t.getFieldNames()[0]);
                    assertEquals("seco", t.getFieldNames()[1]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "first", "b",
                        "seco", "b"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t2.n2, t1.s1, t2.k2 FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2"
                        + " ON  t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("n2", t.getFieldNames()[0]);
                    assertEquals("s1", t.getFieldNames()[1]);
                    assertEquals("k2", t.getFieldNames()[2]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s1", "A",
                        "k2", "c", "n2", 3
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s1", "A",
                        "k2", "d", "n2", 4
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s1", "B",
                        "k2", "c", "n2", 3
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "s1", "B",
                        "k2", "d", "n2", 4
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT t2.*, t1.* FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2"
                        + " ON t1.n1 > 0"
                        + "   and t2.n2 >= 1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k2", t.getFieldNames()[0]);
                    assertEquals("n2", t.getFieldNames()[1]);
                    assertEquals("s2", t.getFieldNames()[2]);
                    assertEquals("k1", t.getFieldNames()[3]);
                    assertEquals("n1", t.getFieldNames()[4]);
                    assertEquals("s1", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "n1", 1, "s1", "A",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "c", "n2", 3, "s2", "A"
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "n1", 2, "s1", "B",
                        "k2", "d", "n2", 4, "s2", "A"
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2 ON  (1=1)"
                        + " ORDER BY n2,n1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());

                int i = 0;
                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "a", "n1", 1, "s1", "A",
                                "k2", "c", "n2", 3, "s2", "A"
                        )));

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "b", "n1", 2, "s1", "B",
                                "k2", "c", "n2", 3, "s2", "A"
                        )));

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "a", "n1", 1, "s1", "A",
                                "k2", "d", "n2", 4, "s2", "A"
                        )));

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "b", "n1", 2, "s1", "B",
                                "k2", "d", "n2", 4, "s2", "A"
                        )));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
                        + " tblspace1.table1 t1"
                        + " JOIN tblspace1.table2 t2 ON (1<>2)"
                        + " ORDER BY n2 desc,n1", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {

                    assertEquals(6, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("n1", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("k2", t.getFieldNames()[3]);
                    assertEquals("n2", t.getFieldNames()[4]);
                    assertEquals("s2", t.getFieldNames()[5]);
                }
                assertEquals(4, tuples.size());

                int i = 0;

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "a", "n1", 1, "s1", "A",
                                "k2", "d", "n2", 4, "s2", "A"
                        )));

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "b", "n1", 2, "s1", "B",
                                "k2", "d", "n2", 4, "s2", "A"
                        )));

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "a", "n1", 1, "s1", "A",
                                "k2", "c", "n2", 3, "s2", "A"
                        )));

                assertTrue(
                        tuples.get(i++).toMap().equals(MapUtils.map(
                                "k1", "b", "n1", 2, "s1", "B",
                                "k2", "c", "n2", 3, "s2", "A"
                        )));

            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
                        + " tblspace1.table1 t1 "
                        + " JOIN tblspace1.table2 t2 "
                        + " ON t1.n1 + 3 <= t2.n2", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(2, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a",
                        "k2", "d"
                ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2, t1.s1, t2.s2 FROM"
                        + " tblspace1.table1 t1 "
                        + " LEFT JOIN tblspace1.table2 t2 "
                        + " ON t1.s1 = t2.s2"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(4, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("s2", t.getFieldNames()[3]);
                }
                assertEquals(3, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "c", "s1", "A", "s2", "A"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "d", "s1", "A", "s2", "A"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "k2", null, "s1", "B", "s2", null
                ))));
            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2, t1.s1, t2.s2 FROM"
                        + " tblspace1.table1 t1 "
                        + " RIGHT JOIN tblspace1.table2 t2 "
                        + " ON t1.s1 = t2.s2"
                        + " ", Collections.emptyList()).consumeAndClose();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(4, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("s2", t.getFieldNames()[3]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "c", "s1", "A", "s2", "A"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "d", "s1", "A", "s2", "A"
                ))));
            }

        }
    }

    @Test
    public void testSubQueryInSelect() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            assumeThat(manager.getPlanner(), instanceOf(CalcitePlanner.class));
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table2 (k2 string primary key,n2 int,s2 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('a',1,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('b',2,'B')", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('c',3,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('d',4,'A')", Collections.emptyList());

            {
                List<DataAccessor> tuples = scan(manager,
                        "SELECT t1.k1, max(n1) as maxn1, max(select n2 from tblspace1.table2 t2 WHERE t1.s1=t2.s2) as maxn2 FROM "
                        + " tblspace1.table1 t1 "
                        + " group by k1", Collections.emptyList()).consumeAndClose();

                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("maxn1", t.getFieldNames()[1]);
                    assertEquals("maxn2", t.getFieldNames()[2]);
                }

                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", RawString.of("a"), "maxn1", 1, "maxn2", 4
                ))));

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", RawString.of("b"), "maxn1", 2, "maxn2", null
                ))));

            }

            {
                List<DataAccessor> tuples = scan(manager,
                        "SELECT t1.k1, max(n1) as maxn1, max(select n2 from tblspace1.table2 t2 WHERE t1.s1=t2.s2) as maxn2 FROM "
                        + " tblspace1.table1 t1 "
                        + " group by k1"
                        + " order by max(select n2 from tblspace1.table2 t2 WHERE t1.s1=t2.s2) desc", Collections.emptyList()).consumeAndClose();

                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("maxn1", t.getFieldNames()[1]);
                    assertEquals("maxn2", t.getFieldNames()[2]);
                }

                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.get(0).toMap().equals(MapUtils.map(
                                "k1", RawString.of("b"), "maxn1", 2, "maxn2", null
                        )));
                assertTrue(
                        tuples.get(1).toMap().equals(MapUtils.map(
                                "k1", RawString.of("a"), "maxn1", 1, "maxn2", 4)));

            }

            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('a',1,'A')", Collections.emptyList());

            {

                List<DataAccessor> tuples = scan(manager,
                        "SELECT * FROM tblspace1.table1 t1, tblspace1.table2 t2 WHERE t1.k1=? and t2.k2=? and t1.k1=t2.k2", Arrays.asList("a", "a")).consumeAndClose();

                for (DataAccessor t : tuples) {
                    System.out.println("t:" + t);
                    assertEquals(6, t.getFieldNames().length);
                }
                assertEquals(1, tuples.size());

            }
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testJoinUnsortedKey() throws Exception {
        String nodeId = "localhost";
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        // this test is using FileDataStorageManager, because it sorts PK in a way that
        // reproduces an error due to the order or elements returned during the scan
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            execute(manager,
                    "CREATE TABLE customer (customer_id long primary key,contact_email string,contact_person string, creation long, deleted boolean, modification long, name string, vetting boolean)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE TABLE license (license_id long primary key,application string, creation long,data string, deleted boolean, modification long,signature string, customer_id long , license_key_id string)",
                    Collections.emptyList());

            execute(manager, "INSERT INTO license (license_id,customer_id) values(1,1)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(2,1)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(3,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(4,4)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(5,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(6,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(7,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(8,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(9,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(10,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(11,3)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(12,5)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(13,6)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(14,7)", Collections.emptyList());
            execute(manager, "INSERT INTO license (license_id,customer_id) values(15,3)", Collections.emptyList());

            execute(manager, "INSERT INTO customer (customer_id) values(1)", Collections.emptyList());
            execute(manager, "INSERT INTO customer (customer_id) values(2)", Collections.emptyList());
            execute(manager, "INSERT INTO customer (customer_id) values(3)", Collections.emptyList());
            execute(manager, "INSERT INTO customer (customer_id) values(4)", Collections.emptyList());
            execute(manager, "INSERT INTO customer (customer_id) values(5)", Collections.emptyList());
            execute(manager, "INSERT INTO customer (customer_id) values(6)", Collections.emptyList());
            execute(manager, "INSERT INTO customer (customer_id) values(7)", Collections.emptyList());

            {
                try (DataScanner scan1 = scan(manager,
                        "SELECT t0.license_id,c.customer_id FROM license t0, customer c WHERE c.customer_id = 3 AND t0.customer_id = 3 AND c.customer_id = t0.customer_id",
                        Collections.emptyList())) {

                    List<DataAccessor> consume = scan1.consumeAndClose();
                    System.out.println("NUM " + consume.size());
                    assertEquals(9, consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }

                }
            }

            // joins during transactions must release transaction resources properly
            {
                long tx = TestUtils.beginTransaction(manager, TableSpace.DEFAULT);
                try (DataScanner scan1 = scan(manager,
                        "SELECT t0.license_id,c.customer_id FROM license t0, customer c WHERE c.customer_id = 3 AND t0.customer_id = 3 AND c.customer_id = t0.customer_id",
                        Collections.emptyList(), new TransactionContext(tx))) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    assertEquals(9, consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }

                }
                TestUtils.commitTransaction(manager, TableSpace.DEFAULT, tx);
            }
        }
    }

    @Test
    public void testExists() throws Exception {
        String nodeId = "localhost";
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        // this test is using FileDataStorageManager, because it sorts PK in a way that
        // reproduces an error due to the order or elements returned during the scan
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            assumeThat(manager.getPlanner(), instanceOf(CalcitePlanner.class));
            manager.start();
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            execute(manager,
                    "CREATE TABLE a (k1 int primary key, v1 int)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE TABLE b (k2 int primary key, v2 int)",
                    Collections.emptyList());

            execute(manager, "INSERT INTO a (k1,v1) values(1,1)", Collections.emptyList());
            execute(manager, "INSERT INTO b (k2,v2) values(1,1)", Collections.emptyList());

            {
                try (DataScanner scan1 = scan(manager,
                        " select k1 as kk, v1 as vv from a where 1=1 and ( NOT EXISTS (SELECT NULL FROM b WHERE b.v2=a.k1 AND  1=1  AND b.k2 = 2 ))", Collections.emptyList())) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                    assertEquals(1, consume.size());

                }
            }

            {
                try (DataScanner scan1 = scan(manager,
                        " select k1 as kk, v1 as vv from a where 1=1 and ( NOT EXISTS (SELECT NULL FROM b WHERE b.v2=a.k1 AND  1=1  AND b.k2 = 1 ))", Collections.emptyList())) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                    assertEquals(0, consume.size());

                }
            }

            // joins during transactions must release transaction resources properly
            {
                long tx = TestUtils.beginTransaction(manager, TableSpace.DEFAULT);
                try (DataScanner scan1 = scan(manager,
                        " select k1 as kk, v1 as vv from a where 1=1 and ( NOT EXISTS (SELECT NULL FROM b WHERE b.v2=a.k1 AND  1=1  AND b.k2 = 2 ))",
                        Collections.emptyList(), new TransactionContext(tx));) {
                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                    assertEquals(1, consume.size());
                }
                TestUtils.commitTransaction(manager, TableSpace.DEFAULT, tx);
            }

            {
                long tx = TestUtils.beginTransaction(manager, TableSpace.DEFAULT);
                try (DataScanner scan1 = scan(manager,
                        " select k1 as kk, v1 as vv from a where 1=1 and ( EXISTS (SELECT 1 FROM b WHERE b.v2=a.k1 AND b.k2 = 1 ))", Collections.emptyList(),
                        new TransactionContext(tx))) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                    assertEquals(1, consume.size());
                }
                TestUtils.commitTransaction(manager, TableSpace.DEFAULT, tx);
            }
        }

    }
}
