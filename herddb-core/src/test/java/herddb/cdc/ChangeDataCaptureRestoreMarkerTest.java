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

package herddb.cdc;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.codec.RecordSerializer;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.log.RestoredFromSnapshot;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A restore replaces the whole content of a tablespace without writing a single record to its commit log, so from the
 * markers it leaves there on the log stops describing what the tablespace holds. Change data capture reads nothing but
 * that log, so it has to stop at the marker: a consumer that is not told keeps mirroring the content the tablespace
 * had before the restore, for good, and nothing ever looks wrong.
 *
 * <p>
 * The capture is driven the way anybody who uses it drives it, through {@link ChangeDataCapture#run()}, and against a
 * tablespace that a real restore really created. That is the only way this is worth testing: the log reports whatever
 * the acceptor throws as a problem of its own, so the failure that says "this tablespace was restored" can arrive at
 * the caller disguised as a log that is momentarily unavailable, which is the one disguise that must never happen. A
 * caller that retries on an unavailable log, the natural policy because that is what a passing BookKeeper problem
 * looks like, would read the same marker forever.
 * </p>
 */
public class ChangeDataCaptureRestoreMarkerTest {

    private static final String RESTORED_TABLESPACE = "restoredts";

    private static final String TABLE_NAME = "t1";

    private static final int ROWS_IN_BACKUP = 4;

    private static final int BOOT_TIMEOUT = 60000;

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
    public void testCaptureOfARestoredTableSpaceStopsAtTheMarker() throws Exception {
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverConfiguration.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverConfiguration.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverConfiguration.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
        clientConfiguration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
        clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        String restoredTableSpaceUUID;
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();

            Table table = Table
                    .builder()
                    .name(TABLE_NAME)
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                server.getManager().executeUpdate(
                        new InsertStatement(TableSpace.DEFAULT, TABLE_NAME,
                                RecordSerializer.makeRecord(table, "c", i, "d", 2)),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            }

            try (HDBClient client = new HDBClient(clientConfiguration);
                 HDBConnection connection = client.openConnection()) {
                ByteArrayOutputStream backup = new ByteArrayOutputStream();
                BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, backup, new ProgressListener() {
                });
                BackupUtils.restoreTableSpace(RESTORED_TABLESPACE, server.getNodeId(), connection,
                        new ByteArrayInputStream(backup.toByteArray()), new ProgressListener() {
                        });
            }
            assertTrue("the restored tablespace never booted",
                    server.getManager().waitForTablespace(RESTORED_TABLESPACE, BOOT_TIMEOUT, true));
            restoredTableSpaceUUID =
                    server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).getTableSpaceUUID();

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            try (ChangeDataCapture cdc = new ChangeDataCapture(restoredTableSpaceUUID, clientConfiguration,
                    mutations::add, LogSequenceNumber.START_OF_TIME, new InMemoryTableHistoryStorage())) {
                cdc.start();
                try {
                    cdc.run();
                    fail("the capture read the whole log of a tablespace that was restored from a snapshot as if"
                            + " nothing had happened: from the restore on, that log does not describe the content"
                            + " of the tablespace any more, so whoever consumes these mutations mirrors a content"
                            + " that is not there and has no way to notice");
                } catch (LogNotAvailableException wrongKind) {
                    fail("the capture stopped at the restore, but it reported it as a commit log that is not"
                            + " available, which is what a passing BookKeeper problem looks like. Anybody who"
                            + " retries on that, and retrying is the right thing to do for a log problem, reads the"
                            + " very same marker again on every attempt and never gets anywhere: " + wrongKind);
                } catch (ChangeDataCapture.TableSpaceRestoredFromSnapshotException expected) {
                    assertNotNull("the consumer has to be told where to start the capture again once it has rebuilt"
                            + " its copy of the tablespace", expected.getLogSequenceNumber());
                    assertSame("the marker that opens the restore is the first entry on the log of a restored"
                            + " tablespace, so it is the one the capture has to stop at",
                            RestoredFromSnapshot.Phase.STARTED, expected.getPhase());
                    assertTrue("the reason the capture stopped has to name the tablespace it was capturing, there"
                            + " can be one capture per tablespace and a bare message says nothing",
                            String.valueOf(expected.getMessage()).contains(restoredTableSpaceUUID));
                }
            }

            assertEquals("a restore writes no record to the commit log, so no mutation can come out of the log of a"
                    + " restored tablespace: " + mutations, 0, mutations.size());
        }
    }

    /**
     * The schema history the capture needs. Nothing is ever stored in it here, because the capture stops at the very
     * first entry of the log.
     */
    private static final class InMemoryTableHistoryStorage implements ChangeDataCapture.TableSchemaHistoryStorage {

        private final Map<String, SortedMap<LogSequenceNumber, Table>> tableHistory = new ConcurrentHashMap<>();

        @Override
        public void storeSchema(LogSequenceNumber lsn, Table table) {
            tableHistory.computeIfAbsent(table.name, name -> new TreeMap<>()).put(lsn, table);
        }

        @Override
        public Table fetchSchema(LogSequenceNumber lsn, String tableName) {
            SortedMap<LogSequenceNumber, Table> history = tableHistory.get(tableName);
            if (history == null) {
                return null;
            }
            return history.headMap(lsn).isEmpty() ? null : history.get(history.headMap(lsn).lastKey());
        }
    }
}
