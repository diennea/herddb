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
package herddb.file;

import herddb.core.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.Bytes;
import java.nio.file.Files;

/**
 * Recovery from file
 *
 * @author enrico.olivelli
 */
public class CleanupOnCheckPointTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void cleanupTransactionsFile() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
        String nodeId = "localhost";

        FileDataStorageManager storageManager = new FileDataStorageManager(dataPath);

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            storageManager,
            new FileCommitLogManager(logsPath),
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
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();

            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.checkpoint();

            assertEquals(new LogSequenceNumber(1, 2), manager.getTableSpaceManager("tblspace1").getLog().getLastSequenceNumber());

            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertEquals(new LogSequenceNumber(1, 3), manager.getTableSpaceManager("tblspace1").getLog().getLastSequenceNumber());

            String uuid = manager.getTableSpaceManager("tblspace1").getTableSpaceUUID();
            Path path = dataPath.resolve(uuid + ".tablespace");
            Files.list(path).forEach(p -> {
                System.out.println("path:" + p);
            });
            assertTrue(Files.isRegularFile(path.resolve("transactions.1.2.tx")));
            assertTrue(Files.isRegularFile(path.resolve("tables.1.2.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("indexes.1.2.tablesmetadata")));

            assertFalse(Files.isRegularFile(path.resolve("transactions.1.3.tx")));
            assertFalse(Files.isRegularFile(path.resolve("tables.1.3.tablesmetadata")));
            assertFalse(Files.isRegularFile(path.resolve("indexes.1.3.tablesmetadata")));

            manager.checkpoint();

            assertFalse(Files.isRegularFile(path.resolve("transactions.1.2.tx")));
            assertFalse(Files.isRegularFile(path.resolve("tables.1.2.tablesmetadata")));
            assertFalse(Files.isRegularFile(path.resolve("indexes.1.2.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("transactions.1.3.tx")));
            assertTrue(Files.isRegularFile(path.resolve("tables.1.3.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("indexes.1.3.tablesmetadata")));
        }

    }

    @Test
    public void cleanupTransactionsFileWithUnCommittedTable() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key = Bytes.from_string("k1");
        String nodeId = "localhost";

        FileDataStorageManager storageManager = new FileDataStorageManager(dataPath);

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            storageManager,
            new FileCommitLogManager(logsPath),
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

            assertEquals(new LogSequenceNumber(1, 2), manager.getTableSpaceManager("tblspace1").getLog().getLastSequenceNumber());

            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertEquals(new LogSequenceNumber(1, 3), manager.getTableSpaceManager("tblspace1").getLog().getLastSequenceNumber());

            String uuid = manager.getTableSpaceManager("tblspace1").getTableSpaceUUID();
            Path path = dataPath.resolve(uuid + ".tablespace");
            Files.list(path).forEach(p -> {
                System.out.println("path:" + p);
            });
            assertTrue(Files.isRegularFile(path.resolve("transactions.1.2.tx")));
            assertTrue(Files.isRegularFile(path.resolve("tables.1.2.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("indexes.1.2.tablesmetadata")));

            assertFalse(Files.isRegularFile(path.resolve("transactions.1.3.tx")));
            // il commit della create table fa creare i file che contengono la nuova tabella
            assertTrue(Files.isRegularFile(path.resolve("tables.1.3.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("indexes.1.3.tablesmetadata")));

            manager.checkpoint();

            assertFalse(Files.isRegularFile(path.resolve("transactions.1.2.tx")));
            assertFalse(Files.isRegularFile(path.resolve("tables.1.2.tablesmetadata")));
            assertFalse(Files.isRegularFile(path.resolve("indexes.1.2.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("transactions.1.3.tx")));
            assertTrue(Files.isRegularFile(path.resolve("tables.1.3.tablesmetadata")));
            assertTrue(Files.isRegularFile(path.resolve("indexes.1.3.tablesmetadata")));
        }

    }

}
