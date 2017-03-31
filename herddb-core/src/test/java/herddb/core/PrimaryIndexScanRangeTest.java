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

import herddb.file.FileDataStorageManager;
import herddb.index.PrimaryIndexRangeScan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexRangeScan;
import herddb.index.SecondaryIndexSeek;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 *
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
            new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
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

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<=3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }

        }

    }

    @Test
    public void primaryIndexPrefixScanFileTest() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
            new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null);) {
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

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<=3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }

        }

    }

}
