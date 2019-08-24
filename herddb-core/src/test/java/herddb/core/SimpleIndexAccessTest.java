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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexSeek;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.SQLRecordPredicate;
import herddb.sql.TranslatedQuery;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class SimpleIndexAccessTest {

    @Test
    public void multipleColumnPrimaryKeyPrefixScanTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
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
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1235))).getUpdateCount());

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey"), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(2, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 = ? and n1 <> 1235", Arrays.asList("mykey"), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 = ? and n1 = 1235", Arrays.asList("mykey"), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan1.consume().size());
                }
            }

        }
    }

    @Test
    public void multipleColumnPrimaryKeyPrefixScanWithAliasTest() throws Exception {

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.q1_HISTORY (\n"
                    + "  MSG_ID           BIGINT        NOT NULL,\n"
                    + "  SID              TINYINT       NOT NULL,  \n"
                    + "  STATUS           INT           NOT NULL,\n"
                    + "  TIMESTAMP        TIMESTAMP,\n"
                    + "  STATUSLINE       VARCHAR(2000) NULL,\n"
                    + "  IDBOUNCECATEGORY SMALLINT      NULL,\n"
                    + "  PRIMARY KEY  (MSG_ID, SID)\n"
                    + ") ;", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_HISTORY(MSG_ID,SID,STATUS) values(1,1,1)", Collections.emptyList()).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_HISTORY(MSG_ID,SID,STATUS) values(1,2,1)", Collections.emptyList()).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_HISTORY(MSG_ID,SID,STATUS) values(1,3,1)", Collections.emptyList()).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_HISTORY(MSG_ID,SID,STATUS) values(2,1,1)", Collections.emptyList()).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.q1_HISTORY(MSG_ID,SID,STATUS) values(2,2,1)", Collections.emptyList()).getUpdateCount());

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, ""
                        + "SELECT H.SID, H.STATUS, H.`TIMESTAMP`, H.STATUSLINE, H.IDBOUNCECATEGORY "
                        + "FROM tblspace1.q1_HISTORY AS H "
                        + "WHERE H.MSG_ID=?", Arrays.asList(1), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(3, scan1.consume().size());
                }
            }

        }
    }

    @Test
    public void whereOnPkScanTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string,"
                    + "n1 int primary key,"
                    + "s1 string "
                    + ")", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(1235))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(1236))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey4", Integer.valueOf(1237))).getUpdateCount());

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,"
                        + "'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where n1 >= ?", Arrays.asList(1235), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate() instanceof SQLRecordPredicate);
                SQLRecordPredicate sqlPred = (SQLRecordPredicate) scan.getPredicate();
                assertTrue(sqlPred.getPrimaryKeyFilter() != null);
                System.out.println("pkFilter:" + sqlPred.getPrimaryKeyFilter());

                try (DataScanner scan1 = ((ScanResult) manager.executePlan(translate.plan, translate.context, TransactionContext.NO_TRANSACTION)).dataScanner) {
                    assertEquals(3, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,"
                        + "'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where n1 >= 30", Arrays.asList(1235), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate() instanceof SQLRecordPredicate);
                SQLRecordPredicate sqlPred = (SQLRecordPredicate) scan.getPredicate();
                assertTrue(sqlPred.getPrimaryKeyFilter() != null);
                System.out.println("pkFilter:" + sqlPred.getPrimaryKeyFilter());

                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(4, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,"
                        + "'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql"
                        + " where n1 >= 30 and n1 >= 1235", Arrays.asList(1235), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate() instanceof SQLRecordPredicate);
                SQLRecordPredicate sqlPred = (SQLRecordPredicate) scan.getPredicate();
                assertTrue(sqlPred.getPrimaryKeyFilter() != null);
                System.out.println("pkFilter:" + sqlPred.getPrimaryKeyFilter());

                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(3, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT k1 as theKey,"
                        + "'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where n1 >= 10000 or n1 >= ?", Arrays.asList(1235), true, true, false, -1);
                ScanStatement scan = translate.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate() instanceof SQLRecordPredicate);
                SQLRecordPredicate sqlPred = (SQLRecordPredicate) scan.getPredicate();
                assertNull(sqlPred.getPrimaryKeyFilter());
                System.out.println("pkFilter:" + sqlPred.getPrimaryKeyFilter());

                try (DataScanner scan1 = manager.scan(scan, translate.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(3, scan1.consume().size());
                }
            }

        }
    }
}
