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
package herddb.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import herddb.cluster.BookkeeperCommitLog;
import herddb.codec.RecordSerializer;
import herddb.core.TestUtils;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.ZKTestEnv;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DisklessClusterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testSwitchServerWithFirstServerLostAndNoRecoveryFromLog() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        // do not keep transaction log
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 500);
        // not automatic checkpoint
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);

        // second server, new disk
        ServerConfiguration serverconfig_2 = serverconfig_1.copy()
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        String nodeId1;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);
            nodeId1 = server_1.getNodeId();
            TestUtils.execute(server_1.getManager(), "CREATE TABLE tt(n1 string primary key, n2 int)", Collections.emptyList());
            TestUtils.execute(server_1.getManager(), "CREATE INDEX aa ON tt(n2)", Collections.emptyList());
            TestUtils.execute(server_1.getManager(), "INSERT INTO tt(n1,n2) values('a',1)", Collections.emptyList());

            // start new ledger
            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            log.rollNewLedger();

            Thread.sleep(1000);

            // flush to "disk" (Bookkeeper)
            // the first ledger can be thrown away
            server_1.getManager().checkpoint();

            // assert that it is not possible to boot just by reading from the log
            assertFalse(log.getActualLedgersList().getActiveLedgers().contains(log.getActualLedgersList().getFirstLedger()));

            // scan (with index)
            assertEquals(1, TestUtils.scan(server_1.getManager(), "SELECT * FROM tt where n2=1", Collections.emptyList()).consumeAndClose().size());

        }

        // now we totally lose server1 and start a new server
        // data is stored on ZK + BK, so no data loss
        // start server_2
        try (Server server_2 = new Server(serverconfig_2)) {
            server_2.start();
            // ensure that we are running with a different server identity
            assertNotEquals(server_2.getNodeId(), nodeId1);

            // assign default table space to the new server
            TestUtils.execute(server_2.getManager(),
                    server_2.getNodeId(),
                    "ALTER TABLESPACE '" + TableSpace.DEFAULT + "','leader:" + server_2.getNodeId() + "','replica:" + server_2.getNodeId() + "'", Collections.emptyList(),
                    TransactionContext.NO_TRANSACTION);

            // recovery will start from checkpoint, not from the log
            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

            // scan (with index)
            assertEquals(1, TestUtils.scan(server_2.getManager(), "SELECT * FROM tt where n2=1", Collections.emptyList()).consumeAndClose().size());
        }
    }

    @Test
    public void testSwitchServerAutomatically() throws Exception {

        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        // do not keep transaction log
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 500);
        // not automatic checkpoint
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);

        // second server, new disk
        ServerConfiguration serverconfig_2 = serverconfig_1.copy()
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        String nodeId1;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);
            nodeId1 = server_1.getNodeId();
            TestUtils.execute(server_1.getManager(),
                    server_1.getNodeId(),
                    "ALTER TABLESPACE '" + TableSpace.DEFAULT + "','replica:*','maxLeaderInactivityTime:5000'", Collections.emptyList(),
                    TransactionContext.NO_TRANSACTION);

            TestUtils.execute(server_1.getManager(), "CREATE TABLE tt(n1 string primary key, n2 int)", Collections.emptyList());
            TestUtils.execute(server_1.getManager(), "CREATE INDEX aa ON tt(n2)", Collections.emptyList());
            TestUtils.execute(server_1.getManager(), "INSERT INTO tt(n1,n2) values('a',1)", Collections.emptyList());

            // start new ledger
            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            log.rollNewLedger();

            Thread.sleep(1000);

            // flush to "disk" (Bookkeeper)
            // the first ledger can be thrown away
            server_1.getManager().checkpoint();

            // assert that it is not possible to boot just by reading from the log
            assertFalse(log.getActualLedgersList().getActiveLedgers().contains(log.getActualLedgersList().getFirstLedger()));

            // scan (with index)
            assertEquals(1, TestUtils.scan(server_1.getManager(), "SELECT * FROM tt where n2=1", Collections.emptyList()).consumeAndClose().size());

        }

        // now we totally lose server1 and start a new server
        // data is stored on ZK + BK, so no data loss
        // the new server will auto assign itself to the tablespace and become leader
        // because we have replica=* (any server) and maxLeaderInactivityTime=5000
        // start server_2
        try (Server server_2 = new Server(serverconfig_2)) {
            server_2.start();
            // ensure that we are running with a different server identity
            assertNotEquals(server_2.getNodeId(), nodeId1);

            // recovery will start from checkpoint, not from the log
            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

            // scan (with index)
            assertEquals(1, TestUtils.scan(server_2.getManager(), "SELECT * FROM tt where n2=1", Collections.emptyList()).consumeAndClose().size());
        }
    }

    @Test
    public void testRestartWithCheckpoint() throws Exception {
        testRestart(true);
    }

    @Test
    public void testRestartWithoutCheckpoint() throws Exception {
        testRestart(false);
    }

    private void testRestart(boolean withCheckpoint) throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.NOTNULL_STRING)
                    .primaryKey("c")
                    .build();
            Index index1 = Index
                    .builder()
                    .onTable(table)
                    .column("d", ColumnTypes.NOTNULL_STRING)
                    .type(Index.TYPE_BRIN)
                    .build();
            Index index2 = Index
                    .builder()
                    .onTable(table)
                    .column("d", ColumnTypes.NOTNULL_STRING)
                    .column("c", ColumnTypes.NOTNULL_INTEGER)
                    .type(Index.TYPE_HASH)
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", "sd")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new CreateIndexStatement(index1), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index2), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", "sd")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = TestUtils.scan(server_1.getManager(), "SELECT * FROM t1", Collections.emptyList());) {
                assertEquals(2, scan.consume().size());
            }
            if (withCheckpoint) {
                server_1.getManager().checkpoint();
            }
        }

        // restart, we are in diskless-mode
        // the base dir can be changed without problems
        // usually the server will start on a new machine with ephemeral disk
        serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

            try (DataScanner scan = TestUtils.scan(server_1.getManager(), "SELECT * FROM t1", Collections.emptyList());) {
                assertEquals(2, scan.consume().size());
            }
        }

        // restart, we are in diskless-mode
        // the base dir can be changed without problems
        // usually the server will start on a new machine with ephemeral disk
        serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

            try (DataScanner scan = TestUtils.scan(server_1.getManager(), "SELECT * FROM t1", Collections.emptyList());) {
                assertEquals(2, scan.consume().size());
            }
        }
    }

}
