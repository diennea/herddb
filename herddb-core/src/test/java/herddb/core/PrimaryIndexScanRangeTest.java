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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.index.PrimaryIndexRangeScan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class PrimaryIndexScanRangeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void primaryIndexPrefixScanTest() throws Exception {

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',3,6,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',4,7,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',5,5,'n2')", Collections.emptyList());

            performBasicPlannerTests(manager);

            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());
        }

    }

    @Test
    public void primaryIndexPrefixScanFileTest() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',3,6,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',5,7,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',6,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('f',7,7,'n2')", Collections.emptyList());

            performBasicPlannerTests(manager);

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(6, tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertNull(scan.getComparator());
                assertNotNull(scan.getLimits());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(6, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(5, tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 3,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(6, tuples.get(0).get("n1"));
                    assertEquals(7, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 4,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(1, tuples.size());
                    assertEquals(7, tuples.get(0).get("n1"));
                }
            }

            // add a record in the middle of the sorted recordset
            DMLStatementExecutionResult res = TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',4,6,'n2')", Collections.emptyList(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long tx = res.transactionId;

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(4, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(6, tuples.get(3).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(6, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(5, tuples.get(3).get("n1"));
                    assertEquals(6, tuples.get(4).get("n1"));
                    assertEquals(7, tuples.get(5).get("n1"));
                }
            }
            System.out.println("QUAAAAAAAA");
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                }
            }
            System.out.println("QUIIIIII");
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    for (DataAccessor tuple : tuples) {
                        System.out.println("tuple: " + tuple.toMap());
                    }
                    assertEquals(3, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(4, tuples.get(1).get("n1"));
                    assertEquals(5, tuples.get(2).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(4, tuples.get(1).get("n1"));
                    assertEquals(6, tuples.get(2).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(4, tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 3,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(5, tuples.get(0).get("n1"));
                    assertEquals(6, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 4,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(6, tuples.get(0).get("n1"));
                    assertEquals(7, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 5,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(1, tuples.size());
                    assertEquals(7, tuples.get(0).get("n1"));
                }
            }

            // add other records in the context of the transaction
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',8,6,'n3')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',9,6,'n3')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',10,6,'n3')", Collections.emptyList(), new TransactionContext(tx));

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 ", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(9, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(5, tuples.get(3).get("n1"));
                    assertEquals(6, tuples.get(4).get("n1"));
                    assertEquals(7, tuples.get(5).get("n1"));
                    assertEquals(8, tuples.get(6).get("n1"));
                    assertEquals(9, tuples.get(7).get("n1"));
                    assertEquals(10, tuples.get(8).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 7", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(7, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(5, tuples.get(3).get("n1"));
                    assertEquals(6, tuples.get(4).get("n1"));
                    assertEquals(7, tuples.get(5).get("n1"));
                    assertEquals(8, tuples.get(6).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 "
                        + "limit 200", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(8, tuples.get(0).get("n1"));
                    assertEquals(9, tuples.get(1).get("n1"));
                    assertEquals(10, tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 ", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(8, tuples.get(0).get("n1"));
                    assertEquals(9, tuples.get(1).get("n1"));
                    assertEquals(10, tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 desc", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(10, tuples.get(0).get("n1"));

                    assertEquals(9, tuples.get(1).get("n1"));
                    assertEquals(8, tuples.get(2).get("n1"));

                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(2, tuples.size());
                    assertEquals(8, tuples.get(0).get("n1"));
                    assertEquals(9, tuples.get(1).get("n1"));
                }
            }

        }

    }

    private void performBasicPlannerTests(final DBManager manager) throws StatementExecutionException, DataScannerException {
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(2, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(2, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<=3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT *"
                            + " FROM tblspace1.t1 "
                            + "WHERE n1>=2 "
                            + "and n2<=6 "
                            + "order by n1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by N1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by N1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by n1 desc", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by n1 asc, n2", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
    }

}
