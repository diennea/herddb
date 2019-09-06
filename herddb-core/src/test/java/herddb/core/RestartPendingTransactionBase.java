/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.core;

import static org.junit.Assert.assertTrue;
import herddb.model.ColumnTypes;
import herddb.model.ConstValueRecordFunction;
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
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Recovery from file
 *
 * @author enrico.olivelli
 */
public abstract class RestartPendingTransactionBase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    protected abstract DBManager buildDBManager(
            String nodeId,
            Path metadataPath,
            Path dataPath,
            Path logsPath,
            Path tmoDir) throws Exception;

    @Test @Ignore
    public void recoverUpdateInTransaction() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key1 = Bytes.from_string("k1");
        Bytes key2 = Bytes.from_string("k2");
        Bytes key3 = Bytes.from_string("k3");
        String nodeId = "localhost";
        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId),
                    nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key1, key1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key3, key3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).
                    getTransactionId();
            manager.executeStatement(new UpdateStatement("tblspace1", table.name, new ConstValueRecordFunction(key2),
                    new ConstValueRecordFunction(key3), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // transaction which contains the update will be replayed at reboot
        }

        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key1, null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test @Ignore
    public void recoverDeleteInTransaction() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key1 = Bytes.from_string("k1");
        Bytes key2 = Bytes.from_string("k2");
        Bytes key3 = Bytes.from_string("k3");
        String nodeId = "localhost";
        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId),
                    nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key1, key1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key3, key3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).
                    getTransactionId();
            manager.executeStatement(new DeleteStatement("tblspace1", table.name, key2, null),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // transaction which contains the update will be replayed at reboot
        }

        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key1, null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test @Ignore
    public void recoverUpdate() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key1 = Bytes.from_string("k1");
        Bytes key2 = Bytes.from_string("k2");
        Bytes key3 = Bytes.from_string("k3");
        String nodeId = "localhost";
        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId),
                    nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key1, key1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key3, key3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            manager.executeStatement(new UpdateStatement("tblspace1", table.name, new ConstValueRecordFunction(key2),
                    new ConstValueRecordFunction(key3), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            long tx2 = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).
                    getTransactionId();
            manager.executeStatement(new UpdateStatement("tblspace1", table.name, new ConstValueRecordFunction(key2),
                    new ConstValueRecordFunction(key3), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(tx2));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx2), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // transactions which contains the update will be replayed at reboot
        }

        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key1, null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test @Ignore
    public void recoverDelete() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key1 = Bytes.from_string("k1");
        Bytes key2 = Bytes.from_string("k2");
        Bytes key3 = Bytes.from_string("k3");
        String nodeId = "localhost";
        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId),
                    nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key1, key1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key3, key3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            manager.executeStatement(new DeleteStatement("tblspace1", table.name, key2, null),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        }

        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key1, null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test @Ignore
    public void recoverUpdateInTransaction2() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key1 = Bytes.from_string("k1");
        Bytes key2 = Bytes.from_string("k2");
        Bytes key3 = Bytes.from_string("k3");
        String nodeId = "localhost";
        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId),
                    nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key1, key1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key3, key3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).
                    getTransactionId();
            manager.executeStatement(new DeleteStatement("tblspace1", table.name, key2, null),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            manager.executeStatement(new UpdateStatement("tblspace1", table.name, new ConstValueRecordFunction(key2),
                    new ConstValueRecordFunction(key3), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // transaction which contains the update will be replayed at reboot
        }

        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key1, null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }

    @Test
    public void recoverCommitInTransaction() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        Bytes key1 = Bytes.from_string("k1");
        Bytes key2 = Bytes.from_string("k2");
        Bytes key3 = Bytes.from_string("k3");
        String nodeId = "localhost";
        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId),
                    nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();
            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key1, key1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key2, key2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new InsertStatement("tblspace1", table.name, new Record(key3, key3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).
                    getTransactionId();


            manager.executeStatement(new UpdateStatement("tblspace1", table.name, new ConstValueRecordFunction(key2),
                    new ConstValueRecordFunction(key3), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(tx));

            manager.checkpoint();

            // the commit tx command is to be replayed
            // the behaviour is different from BK and File commit logs
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        }

        try (DBManager manager = buildDBManager(nodeId,
                metadataPath,
                dataPath,
                logsPath,
                tmoDir)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            GetResult result = manager.get(new GetStatement("tblspace1", "t1", key2, null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

    }
}
