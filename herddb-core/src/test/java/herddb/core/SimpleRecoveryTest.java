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
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.ColumnTypes;
import herddb.model.ConstValueRecordFunction;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableAlreadyExistsException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Recovery from file
 *
 * @author enrico.olivelli
 */
public class SimpleRecoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void createTable1_aftercheckpoint() throws Exception {

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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

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
            try {
                manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (TableAlreadyExistsException alreadyExists) {
            }
        }

    }
    
     @Test
    public void createTable1_nocheckpoint() throws Exception {

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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

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
            try {
                manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (TableAlreadyExistsException alreadyExists) {
            }
        }

    }

    @Test
    public void createInsertAndRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);
            GetStatement get = new GetStatement("tblspace1", "t1", key, null);
            GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            assertEquals(key, result.getRecord().key);
            assertEquals(value, result.getRecord().value);
        }

    }

    @Test
    public void createInsertInTransactionAndRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);
            GetStatement get = new GetStatement("tblspace1", "t1", key, null);
            GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            assertEquals(key, result.getRecord().key);
            assertEquals(value, result.getRecord().value);
        }

    }

    @Test
    public void rollbackInsertTransactionOnRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes key2 = Bytes.from_int(1235);
        Bytes value = Bytes.from_long(8888);

        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            InsertStatement insert = new InsertStatement("tblspace1", "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

            // transaction is not committed, and the leader dies, key MUST not appear anymore
            InsertStatement insert2 = new InsertStatement("tblspace1", "t1", new Record(key2, value));
            assertEquals(1, manager.executeUpdate(insert2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            {
                GetStatement get = new GetStatement("tblspace1", "t1", key2, null);
                GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(key2, result.getRecord().key);
                assertEquals(value, result.getRecord().value);
            }

            {
                // transaction rollback occurred
                GetStatement get = new GetStatement("tblspace1", "t1", key, null);
                GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertFalse(result.found());
            }

        }

    }

    @Test
    public void rollbackDeleteTransactionOnRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

            // transaction is not committed, and the leader dies, key appear again
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            DeleteStatement delete = new DeleteStatement("tblspace1", "t1", key, null);
            assertEquals(1, manager.executeUpdate(delete, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            {
                GetStatement get = new GetStatement("tblspace1", "t1", key, null);
                GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(key, result.getRecord().key);
                assertEquals(value, result.getRecord().value);
            }

        }

    }

    @Test
    public void rollbackUpdateTransactionOnRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);
        Bytes value2 = Bytes.from_long(8889);
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

            // transaction is not committed, and the leader dies, key appear again with the old value
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            UpdateStatement update = new UpdateStatement("tblspace1", "t1", new Record(key, value2), null);
            assertEquals(1, manager.executeUpdate(update, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            {
                GetStatement get = new GetStatement("tblspace1", "t1", key, null);
                GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
                assertEquals(key, result.getRecord().key);
                assertEquals(value, result.getRecord().value);
            }

        }

    }

    @Test
    public void rollbackCreateTableOnRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
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

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            Table transacted_table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t2")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
            CreateTableStatement st_create = new CreateTableStatement(transacted_table);
            manager.executeStatement(st_create, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));

            InsertStatement insert = new InsertStatement("tblspace1", "t2", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

            GetStatement get = new GetStatement("tblspace1", "t2", key, null);
            GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            assertTrue(result.found());
            assertEquals(key, result.getRecord().key);
            assertEquals(value, result.getRecord().value);

            // transaction is not committed, and the leader dies, the table will disappear
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetStatement get = new GetStatement("tblspace1", "t2", key, null);
            try {
                manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (TableDoesNotExistException error) {
            }

        }

    }

    @Test
    public void autoIncrementAfterRestart_from_snapshot() throws Exception {

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

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.INTEGER)
                    .primaryKey("id", true)
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new AutoIncrementPrimaryKeyRecordFunction(), new ConstValueRecordFunction(new byte[0]));
            DMLStatementExecutionResult insertResult = manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, insertResult.getUpdateCount());
            int newValue = insertResult.getKey().to_int();
            assertEquals(1, newValue);

            GetResult get = manager.get(new GetStatement("tblspace1", "t1", insertResult.getKey(), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(get.found());

            long next_value = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getNextPrimaryKeyValue();
            assertEquals(2, next_value);

            manager.checkpoint();

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new AutoIncrementPrimaryKeyRecordFunction(), new ConstValueRecordFunction(new byte[0]));
            DMLStatementExecutionResult insertResult = manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, insertResult.getUpdateCount());
            int newValue = insertResult.getKey().to_int();
            assertEquals(2, newValue);

            GetResult get = manager.get(new GetStatement("tblspace1", "t1", insertResult.getKey(), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(get.found());

            long next_value = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getNextPrimaryKeyValue();
            assertEquals(3, next_value);
        }

    }

    @Test
    public void autoIncrementAfterRestart_from_log() throws Exception {

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

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.INTEGER)
                    .primaryKey("id", true)
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new AutoIncrementPrimaryKeyRecordFunction(), new ConstValueRecordFunction(new byte[0]));
            DMLStatementExecutionResult insertResult = manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, insertResult.getUpdateCount());
            int newValue = insertResult.getKey().to_int();
            assertEquals(1, newValue);

            GetResult get = manager.get(new GetStatement("tblspace1", "t1", insertResult.getKey(), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(get.found());

            long next_value = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getNextPrimaryKeyValue();
            assertEquals(2, next_value);

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            InsertStatement insert = new InsertStatement("tblspace1", "t1", new AutoIncrementPrimaryKeyRecordFunction(), new ConstValueRecordFunction(new byte[0]));
            DMLStatementExecutionResult insertResult = manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertEquals(1, insertResult.getUpdateCount());
            int newValue = insertResult.getKey().to_int();
            assertEquals(2, newValue);

            GetResult get = manager.get(new GetStatement("tblspace1", "t1", insertResult.getKey(), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(get.found());

            long next_value = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getNextPrimaryKeyValue();
            assertEquals(3, next_value);
        }

    }

}
