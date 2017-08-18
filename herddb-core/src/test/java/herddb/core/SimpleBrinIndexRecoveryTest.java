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

import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.SecondaryIndexSeek;
import herddb.index.brin.BRINIndexManager;
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
 * Tests on index creation and recovery after restart
 *
 * @author enrico.olivelli
 */
public class SimpleBrinIndexRecoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void createRecoveryIndex_withcheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
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
                .type(Index.TYPE_BRIN)
                .column("name", ColumnTypes.STRING).
                build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList());

            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(3, scan1.consume().size());
            }

            manager.checkpoint();
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();
            assertTrue(manager.waitForTablespace("tblspace1", 10000));
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true, false, -1);

            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(3, scan1.consume().size());
            }

        }

    }

    @Test
    public void createRecoveryIndex_withduoblecheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
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
                .type(Index.TYPE_BRIN)
                .column("name", ColumnTypes.STRING).
                build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('a','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('b','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('c','n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('d','n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('e','n2')", Collections.emptyList());

            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(3, scan1.consume().size());
            }

            manager.checkpoint();
            manager.checkpoint();
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();
            assertTrue(manager.waitForTablespace("tblspace1", 10000));
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='n1'", Collections.emptyList(), true, true, false, -1);

            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(3, scan1.consume().size());
            }

        }

    }

    @Test
    public void createRecoveryIndex_withrepeatedkey() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        int id = 0;
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
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
                .type(Index.TYPE_BRIN)
                .column("name", ColumnTypes.STRING).
                build();

            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            BRINIndexManager brin = (BRINIndexManager) manager
                    .getTableSpaceManager(table.tablespace)
                    .getIndexesOnTable(table.name)
                    .get(index.name);

            while(brin.getNumBlocks() < 2) {
                id++;
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('" + id + "','my_repeatad_key')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            }

            // some data on disk
            manager.checkpoint();

            // some data to be recovered from log
            while(brin.getNumBlocks() < 3) {
                id++;
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,name) values('" + id + "','my_repeatad_key')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList());) {
                assertEquals(id, scan1.consume().size());
            }
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();
            assertTrue(manager.waitForTablespace("tblspace1", 10000));
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name='my_repeatad_key'", Collections.emptyList(), true, true, false, -1);

            ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(id, scan1.consume().size());
            }

        }

    }

}
