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
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class SimpleIHashIndexTest {

    @Test
    public void createIndexOnTableWithData() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_HASH)
                    .column("name", ColumnTypes.STRING).
                    build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList());

            // create index, it will be built using existing data
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);

            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(3, scan1.consume().size());
            }

        }

    }

    @Test
    public void createIndexOnEmptyTable() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_HASH)
                    .column("name", ColumnTypes.STRING).
                    build();

            // create index, initially it will be empty
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList());

            TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);

            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(3, scan1.consume().size());
            }

        }

    }

    @Test
    public void updateIndexOnDML() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_HASH)
                    .column("name", ColumnTypes.STRING).
                    build();

            // create index, initially it will be empty
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList());

            TestUtils.executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id='c'", Collections.emptyList());

            {
                TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }

            TestUtils.executeUpdate(manager, "UPDATE tblspace1.t1 set name='n1' WHERE id='e'", Collections.emptyList());
            {
                TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(3, scan1.consume().size());
                }
            }

        }

    }

    @Test
    public void updateIndexOnDMLUsingTransactions() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_HASH)
                    .column("name", ColumnTypes.STRING).
                    build();

            // create index, initially it will be empty
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            long tx = TestUtils.beginTransaction(manager, "tblspace1");
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList(), new TransactionContext(tx));

              {
                TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx));) {
                    assertEquals(3, scan1.consume().size());
                }
            }
            
            TestUtils.executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id='c'", Collections.emptyList(), new TransactionContext(tx));

            {
                TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(0, scan1.consume().size());
                }
            }
            {
                TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx));) {
                    assertEquals(2, scan1.consume().size());
                }
            }
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            {
                TranslatedQuery translated = manager.getTranslator().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                    assertEquals(2, scan1.consume().size());
                }
            }

        }

    }

}
