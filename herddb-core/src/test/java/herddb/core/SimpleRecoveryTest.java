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
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
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
    public void createTable1() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();
        }

    }

    @Test
    public void createInsertAndRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();

            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert).getUpdateCount());
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);
            GetStatement get = new GetStatement(TableSpace.DEFAULT, "t1", key, null);
            GetResult result = manager.get(get);
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

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(TableSpace.DEFAULT))).getTransactionId();

            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, "t1", new Record(key, value)).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(insert).getUpdateCount());

            manager.executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, tx));
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);
            GetStatement get = new GetStatement(TableSpace.DEFAULT, "t1", key, null);
            GetResult result = manager.get(get);
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

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(TableSpace.DEFAULT))).getTransactionId();
            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, "t1", new Record(key, value)).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(insert).getUpdateCount());

            // transaction is not committed, and the leader dies, key MUST not appear anymore
            InsertStatement insert2 = new InsertStatement(TableSpace.DEFAULT, "t1", new Record(key2, value));
            assertEquals(1, manager.executeUpdate(insert2).getUpdateCount());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            {
                GetStatement get = new GetStatement(TableSpace.DEFAULT, "t1", key2, null);
                GetResult result = manager.get(get);
                assertTrue(result.found());
                assertEquals(key2, result.getRecord().key);
                assertEquals(value, result.getRecord().value);
            }

            {
                // transaction rollback occurred
                GetStatement get = new GetStatement(TableSpace.DEFAULT, "t1", key, null);
                GetResult result = manager.get(get);
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

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();

            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert).getUpdateCount());

            // transaction is not committed, and the leader dies, key appear again
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(TableSpace.DEFAULT))).getTransactionId();
            DeleteStatement delete = new DeleteStatement(TableSpace.DEFAULT, "t1", key, null).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(delete).getUpdateCount());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            {
                GetStatement get = new GetStatement(TableSpace.DEFAULT, "t1", key, null);
                GetResult result = manager.get(get);
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

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();

            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, "t1", new Record(key, value));
            assertEquals(1, manager.executeUpdate(insert).getUpdateCount());

            // transaction is not committed, and the leader dies, key appear again with the old value
            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(TableSpace.DEFAULT))).getTransactionId();
            UpdateStatement update = new UpdateStatement(TableSpace.DEFAULT, "t1", new Record(key, value2), null).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(update).getUpdateCount());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            {
                GetStatement get = new GetStatement(TableSpace.DEFAULT, "t1", key, null);
                GetResult result = manager.get(get);
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

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2);
            manager.flush();

            Table transacted_table = Table
                    .builder()
                    .name("t2")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(TableSpace.DEFAULT))).getTransactionId();
            CreateTableStatement st_create = new CreateTableStatement(transacted_table).setTransactionId(tx);
            manager.executeStatement(st_create);

            InsertStatement insert = new InsertStatement(TableSpace.DEFAULT, "t2", new Record(key, value)).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(insert).getUpdateCount());

            GetStatement get = new GetStatement(TableSpace.DEFAULT, "t2", key, null).setTransactionId(tx);
            GetResult result = manager.get(get);
            assertTrue(result.found());
            assertEquals(key, result.getRecord().key);
            assertEquals(value, result.getRecord().value);

            // transaction is not committed, and the leader dies, the table will disappear
        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath))) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            GetStatement get = new GetStatement(TableSpace.DEFAULT, "t2", key, null);
            try {
                manager.get(get);
                fail();
            } catch (TableDoesNotExistException error) {
            }

        }

    }

}
