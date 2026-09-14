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
package herddb.cluster.follower;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.cluster.LedgersInfo;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.SystemInstrumentation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * A replica that holds no data at all for a tablespace, and that cannot replay the log either because the ledgers it
 * would have to start from have been dropped, has to download the whole content of the tablespace from the leader.
 * <p>
 * This is the plain, backup-unrelated use of the full download: the log says nothing about snapshots or restores, it
 * simply does not go back far enough. It shares all of its machinery with the download that a restore from a backup
 * triggers, so it is checked on its own to make sure that the two do not get mixed up:
 * </p>
 * <ul>
 * <li>the missing history has to be reported as a plain {@code FullRecoveryNeededException}, and recovery has to fall
 * back to the download on that exception and not only on the ones that talk about restores;</li>
 * <li>the download has to leave the replica with the complete content of the tablespace;</li>
 * <li>the checkpoint that closes the recovery has to persist that content, so that the replica does not have to
 * download it all over again at the next boot. Nothing may inhibit that checkpoint on a tablespace that was never
 * restored from a snapshot.</li>
 * </ul>
 */
public class FollowerFullDownloadOnPrunedLogTest extends MultiServerBase {

    private static final int RECORDS = 20;

    @Test
    public void replicaWithNoLocalDataRecoversFromSnapshotWhenTheLogWasPruned() throws Exception {

        // counts how many times the content of a tablespace is erased to make room for a downloaded snapshot,
        // which is what tells a full download apart from a plain replay of the log
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
        // every checkpoint drops the ledgers that are not needed any more
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 1);
        // no checkpoint may run on its own: the only one the replica takes has to be the one that closes recovery
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("s", ColumnTypes.STRING)
                .primaryKey("c")
                .build();

        int written = 0;
        String tableSpaceUUID;

        // the tablespace is created and filled, and server2 is declared a replica of it while it is still offline
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            written = insertRecords(server_1, table, written, 10);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            tableSpaceUUID = server_1.getMetadataStorageManager().describeTableSpace(TableSpace.DEFAULT).uuid;
        }

        // every restart opens a new ledger and every checkpoint drops the ones that are not needed any more,
        // until the ledger the tablespace started from is gone for good
        for (int i = 0; i < 2; i++) {
            try (Server server_1 = new Server(serverconfig_1)) {
                server_1.start();
                server_1.waitForStandaloneBoot();
                written = insertRecords(server_1, table, written, 5);
                server_1.getManager().checkpoint();
            }
        }
        assertEquals(RECORDS, written);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            LedgersInfo ledgersList = readLedgersList(server_1, tableSpaceUUID);
            // the tablespace cannot be rebuilt by replaying the log any more: a node that holds no data at all has
            // to start from the very first ledger of the tablespace, and that ledger has been dropped
            assertFalse("the first ledger of the tablespace is still there, the log can still be replayed from the"
                    + " beginning and the test would not check the download: " + ledgersList,
                    ledgersList.getActiveLedgers().contains(ledgersList.getFirstLedger()));

            // nothing has been downloaded so far
            assertEquals(0, countErase.get());

            // server2 joins the cluster as a replica, with no data at all for this tablespace
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                List<DataAccessor> records = waitForRecords(server_2, RECORDS);
                assertEquals("the replica did not come up with the whole content of the tablespace",
                        RECORDS, records.size());
                assertEquals(expectedKeys(), keysOf(records));

                // the content did not come from the log, it was downloaded from the leader
                assertEquals(1, countErase.get());

                TableSpaceManager tableSpaceManager = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                assertFalse("the tablespace manager of the replica is failed", tableSpaceManager.isFailed());
                assertFalse(tableSpaceManager.isLeader());

                // The checkpoint that closes the recovery has to have written the downloaded content to disk.
                // Nothing else can have taken a checkpoint here: the periodic one is disabled and this tablespace
                // was never restored from a snapshot, so nothing may inhibit the one that closes the recovery.
                LogSequenceNumber persisted = server_2
                        .getManager()
                        .getDataStorageManager()
                        .getLastcheckpointSequenceNumber(tableSpaceUUID);
                assertFalse("the data downloaded from the leader was never persisted by the checkpoint that closes"
                        + " the recovery, the replica would have to download it again at the next boot",
                        persisted.isStartOfTime());
            }
        }

        // the replica boots again: it now holds the downloaded content and it must not download it a second time
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                List<DataAccessor> records = waitForRecords(server_2, RECORDS);
                assertEquals(RECORDS, records.size());
                assertEquals(expectedKeys(), keysOf(records));
                assertEquals("the replica downloaded the tablespace again, the content persisted by the previous boot"
                        + " was not usable", 1, countErase.get());
            }
        }
    }

    private static int insertRecords(Server server, Table table, int alreadyWritten, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            int key = alreadyWritten + i + 1;
            server.getManager().executeUpdate(
                    new InsertStatement(TableSpace.DEFAULT, "t1",
                            RecordSerializer.makeRecord(table, "c", key, "s", "record" + key)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }
        return alreadyWritten + count;
    }

    private LedgersInfo readLedgersList(Server server, String tableSpaceUUID) throws Exception {
        ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server.getMetadataStorageManager();
        return ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(),
                testEnv.getPath() + "/ledgers", tableSpaceUUID);
    }

    private static List<DataAccessor> waitForRecords(Server server, int expected) throws Exception {
        List<DataAccessor> records = Collections.emptyList();
        for (int i = 0; i < 200; i++) {
            try (DataScanner scanner = scan(server.getManager(), "SELECT c FROM " + TableSpace.DEFAULT + ".t1",
                    Collections.emptyList())) {
                records = scanner.consume();
            } catch (Exception notReadyYet) {
                records = Collections.emptyList();
            }
            if (records.size() >= expected) {
                return records;
            }
            Thread.sleep(100);
        }
        return records;
    }

    private static List<Integer> expectedKeys() {
        List<Integer> keys = new ArrayList<>();
        for (int i = 1; i <= RECORDS; i++) {
            keys.add(i);
        }
        return keys;
    }

    private static List<Integer> keysOf(List<DataAccessor> records) {
        List<Integer> keys = new ArrayList<>();
        for (DataAccessor record : records) {
            keys.add(((Number) record.get("c")).intValue());
        }
        Collections.sort(keys);
        return keys;
    }
}
