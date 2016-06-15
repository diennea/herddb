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

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
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
                new FileCommitLogManager(logsPath),
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
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
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
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
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
                new FileCommitLogManager(logsPath),
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
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
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
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
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
            
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key, key)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),TransactionContext.NO_TRANSACTION);
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
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

}
