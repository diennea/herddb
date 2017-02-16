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

/**
 * 
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class IndexScanRangeTest {
    
    @Test
    public void hashSecondaryIndexPrefixScan() throws Exception {
        secondaryIndexPrefixScan(Index.TYPE_HASH);
    }
    
    @Test
    public void brinSecondaryIndexPrefixScan() throws Exception {
        secondaryIndexPrefixScan(Index.TYPE_BRIN);
    }
    
    @Test
    public void hashSecondaryIndexSeek() throws Exception {
        secondaryIndexSeek(Index.TYPE_HASH);
    }
    
    @Test
    public void brinSecondaryIndexSeek() throws Exception {
        secondaryIndexSeek(Index.TYPE_BRIN);
    }
    
    @Test
    public void hashsecondaryIndexRangeScan() throws Exception {
        secondaryIndexRangeScan(Index.TYPE_HASH);
    }
    
    @Test
    public void brinSecondaryIndexRangeScan() throws Exception {
        secondaryIndexRangeScan(Index.TYPE_BRIN);
    }
    
    @Test
    public void hashNoIndexOperation() throws Exception {
        noIndexOperation(Index.TYPE_HASH);
    }
    
    @Test
    public void brinNoIndexOperation() throws Exception {
        noIndexOperation(Index.TYPE_BRIN);
    }
    
    private void secondaryIndexPrefixScan(String indexType) throws Exception {
        
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("n2", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            Index index = Index
                .builder()
                .onTable(table)
                .type(indexType)
                .column("n1", ColumnTypes.INTEGER)
                .column("n2", ColumnTypes.INTEGER)
                .build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',3,5,'n2')", Collections.emptyList());

            // create index, it will be built using existing data
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1=2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }
            
        }

    }
    
    private void secondaryIndexSeek(String indexType) throws Exception {
        
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("n2", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            Index index = Index
                .builder()
                .onTable(table)
                .type(indexType)
                .column("n1", ColumnTypes.INTEGER)
                .build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',3,5,'n2')", Collections.emptyList());

            // create index, it will be built using existing data
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            {
                TranslatedQuery translated = manager
                    .getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1=2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }
            
        }

    }
    
    private void secondaryIndexRangeScan(String indexType) throws Exception {
        
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("n2", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            Index index = Index
                .builder()
                .onTable(table)
                .type(indexType)
                .column("n1", ColumnTypes.INTEGER)
                .build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',3,5,'n2')", Collections.emptyList());

            // create index, it will be built using existing data
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1>=1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(5, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<=1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(1, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<=2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(4, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1>= 1 and n1<=2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(4, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<= 1 and n1>=2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(0, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1>1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(4, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(4, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(0, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(1, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1> 1 and n1<3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }
            
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1' and n1>=1 and n1<=2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexRangeScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }
            
        }

    }
    
    private void noIndexOperation(String indexType) throws Exception {
        
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("n2", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            Index index = Index
                .builder()
                .onTable(table)
                .type(indexType)
                .column("n1", ColumnTypes.INTEGER)
                .build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',2,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',3,5,'n2')", Collections.emptyList());

            // create index, it will be built using existing data
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1<n2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                System.out.println("indexOperation:" + scan.getPredicate().getIndexOperation());
                
                assertNull(scan.getPredicate().getIndexOperation());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(5, scan1.consume().size());
                }
            }
            
        }

    }

}
