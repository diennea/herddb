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
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.codec.RecordSerializer;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.SecondaryIndexSeek;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.TruncateTableStatement;
import herddb.model.commands.UpdateStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import static org.junit.Assert.fail;

/**
 * Recovery from file
 *
 * @author enrico.olivelli
 */
public class RestartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void recoverTableCreatedInTransaction() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
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
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.checkpoint();
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            DMLStatementExecutionResult executeStatement = (DMLStatementExecutionResult) manager.executeStatement(new UpdateStatement("tblspace1", "t1", new Record(key, key), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, executeStatement.getUpdateCount());
            // on the log there is an UPDATE for a record which is not on the log
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test
    public void recoverTableCreatedInTransaction2() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
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
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.checkpoint();
            DMLStatementExecutionResult executeStatement = (DMLStatementExecutionResult) manager.executeStatement(new UpdateStatement("tblspace1", "t1", new Record(key, key), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            assertEquals(1, executeStatement.getUpdateCount());
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            DMLStatementExecutionResult executeStatement = (DMLStatementExecutionResult) manager.executeStatement(new UpdateStatement("tblspace1", "t1", new Record(key, key), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, executeStatement.getUpdateCount());
            // on the log there is an UPDATE for a record which is not on the log
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test
    public void recoverTableCreatedInTransaction3() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
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
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();
            DMLStatementExecutionResult executeStatement = (DMLStatementExecutionResult) manager.executeStatement(new UpdateStatement("tblspace1", "t1", new Record(key, key), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, executeStatement.getUpdateCount());

        }

//        try (DBManager manager = new DBManager("localhost",
//                new FileMetadataStorageManager(metadataPath),
//                new FileDataStorageManager(dataPath),
//                new FileCommitLogManager(logsPath),
//                tmoDir, null)) {
//            manager.start();
//
//            manager.waitForTablespace("tblspace1", 10000);
//
//            DMLStatementExecutionResult executeStatement = (DMLStatementExecutionResult) manager.executeStatement(new UpdateStatement("tblspace1", "t1", new Record(key, key), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
//            assertEquals(1, executeStatement.getUpdateCount());
//            // on the log there is an UPDATE for a record which is not on the log
//        }
        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test
    public void recoverTableTruncatedNoCheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
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
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            manager.checkpoint();
            manager.executeStatement(new TruncateTableStatement("tblspace1", table.name), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            GetResult result2 = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result2.found());
            // no checkpoint
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());
        }

    }

    @Test
    public void recoverTableTruncatedWithCheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
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
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            manager.checkpoint();
            manager.executeStatement(new TruncateTableStatement("tblspace1", table.name), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            GetResult result2 = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result2.found());
            manager.checkpoint();
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());
        }

    }

    @Test
    public void recoverTableAndIndexWithCheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        Table table;
        Index index;

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            index = Index.builder().onTable(table).column("name", ColumnTypes.STRING).type(Index.TYPE_BRIN).build();

            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            manager.executeStatement(new InsertStatement("tblspace1", table.name, RecordSerializer.makeRecord(table, "id", 1, "name", "uno")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(1, scan1.consume().size());
            }

            manager.checkpoint();

        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            /* Access through key to page */
            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(1, scan1.consume().size());
            }

        }

    }

    @Test
    public void recoverTableAndIndexNoCheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        Table table;
        Index index;

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            index = Index.builder().onTable(table).column("name", ColumnTypes.STRING).type(Index.TYPE_BRIN).build();

            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            manager.executeStatement(new InsertStatement("tblspace1", table.name, RecordSerializer.makeRecord(table, "id", 1, "name", "uno")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(1, scan1.consume().size());
            }

            // no checkpoint
        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            assertTrue(manager.waitForBootOfLocalTablespaces(30000));

            /* Access through key to page */
            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(1, scan1.consume().size());
            }

        }

    }

    /**
     * Evaluate that a recover work even on a table without any data inserted
     */
    @Test
    public void recoverTableAndIndexNoData() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        Table table;
        Index index;

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            index = Index.builder().onTable(table).column("name", ColumnTypes.STRING).type(Index.TYPE_BRIN).build();

            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(0, scan1.consume().size());
            }

            manager.checkpoint();

        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            /* Access through key to page */
            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(0, scan1.consume().size());
            }

        }

    }

    @Test
    public void recoverAfterrPartialCheckpoint() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        Table table1;
        Table table2;

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            table1 = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            table2 = Table
                .builder()
                .tablespace("tblspace1")
                .name("t2")
                .column("id", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            manager.executeStatement(new CreateTableStatement(table1), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateTableStatement(table2), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id) values(1)", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t2(id) values(1)", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t2(id) values(2)", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t2(id) values(3)", Collections.emptyList());

            assertTrue(manager.get(new GetStatement("tblspace1", table1.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(2), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(3), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(new Runnable() {
                @Override
                public void run() {
                    throw new RuntimeException("error!");
                }
            });
            try {
                manager.checkpoint();
                fail();
            } catch (RuntimeException err) {
                err.printStackTrace();
                assertEquals("error!", err.getMessage());
            }

            assertTrue(manager.get(new GetStatement("tblspace1", table1.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(2), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(3), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            assertTrue(manager.get(new GetStatement("tblspace1", table1.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(2), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(3), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

            manager.checkpoint();

            assertTrue(manager.get(new GetStatement("tblspace1", table1.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(2), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            assertTrue(manager.get(new GetStatement("tblspace1", table2.name, Bytes.from_int(3), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
        }

    }

}
