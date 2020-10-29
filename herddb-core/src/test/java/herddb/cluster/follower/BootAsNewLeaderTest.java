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
package herddb.cluster.follower;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.LedgersInfo;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.RecordSerializer;
import herddb.index.SecondaryIndexSeek;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import herddb.utils.SystemInstrumentation;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class BootAsNewLeaderTest extends MultiServerBase {

    @Test
    public void testLeaderOfflineLogAvailableForcedNewLeader() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_BRIN)
                    .column("s", ColumnTypes.STRING)
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "1")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "s", "2")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "s", "3")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "s", "4")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // set forcibly server2 as new leader
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server2", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        }

        // server 2 boots, it figures out to be a leader,
        // it can recover from log
        try (Server server_2 = new Server(serverconfig_2)) {
            server_2.start();

            assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, true));

            // wait for data to arrive on server_2
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }
            assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION).found());

            TranslatedQuery translated = server_2.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_2.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
        }

        // reboot server_2
        try (Server server_2 = new Server(serverconfig_2)) {
            server_2.start();
            assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, true));
            TranslatedQuery translated = server_2.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_2.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
        }
    }

    @Test
    public void testLeaderOnlineLogNoMoreAvailableDataAlreadyPresentBootAsNewLeader() throws Exception {

        final AtomicInteger countErase = new AtomicInteger();
        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener("eraseTablespaceData") {
            @Override
            public void acceptSingle(Object... args) throws Exception {
                countErase.incrementAndGet();
            }
        });

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 1);
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());
        Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("s", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
        Index index = Index
                .builder()
                .onTable(table)
                .type(Index.TYPE_BRIN)
                .column("s", ColumnTypes.STRING)
                .build();
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "1")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "s", "2")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "s", "3")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "s", "4")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = server_1.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_1.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
        }

        String tableSpaceUUID;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                tableSpaceUUID = man.describeTableSpace(TableSpace.DEFAULT).uuid;
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5, "s", "5")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().checkpoint();
        }
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 6, "s", "6")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().checkpoint();
        }

        assertEquals(0, countErase.get());

        LogSequenceNumber server2checkpointPosition;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            // start server_2, and flush data locally
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));
                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                // force a checkpoint, data is flushed to disk
                server_2.getManager().checkpoint();
                server2checkpointPosition = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog().getLastSequenceNumber();
                System.out.println("server2 checkpoint time: " + server2checkpointPosition);
            }

        }

        // server_2 now is offline for a while
        // start again server_1, in order to create a new ledger
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            BookkeeperCommitLog ll = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            // server_1 make much  progress
            ll.rollNewLedger();
            ll.rollNewLedger();
            ll.rollNewLedger();
            ll.rollNewLedger();
            ll.rollNewLedger();
            ll.rollNewLedger();
            ll.rollNewLedger();
            ll.rollNewLedger();
            // a checkpoint will delete old ledgers
            server_1.getManager().checkpoint();

            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                System.out.println("ledgerList: " + ledgersList);
                assertEquals(1, ledgersList.getActiveLedgers().size());
                assertTrue(!ledgersList.getActiveLedgers().contains(ledgersList.getFirstLedger()));
                // we want to be sure that server_2 cannot recover from log
                assertTrue(!ledgersList.getActiveLedgers().contains(server2checkpointPosition.ledgerId));
            }

            assertEquals(1, countErase.get());

            // make server2 boot as leader, it will fence out server1
            // but server1 is not writing so it won't know from BK
            // it will see his new role from zookeeper (with a 'watch' notification)
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server2", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // data should  be downloaded again from the other server,
            // but the new leader is server_2 itself
            // is should not be able to boot
            // but it also should not corrupt tablespace state by
            // trying to become leader
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertFalse(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 10000, true /*
                 * leader
                 */));
            }

            // make server_1 leader again
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // now server_2 must be able to boot, downloading data from server_1 and easing local state
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertFalse(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, true /*
                 * follower
                 */));
            }

            // make server_2 leader again
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server2", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // stop server_1
            server_1.close();

            // now server_2 must be able to boot as leader,
            // it must have enough local data to boot
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, true /*
                 * leader
                 */));
            }

        }

    }

}
