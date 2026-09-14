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

package herddb.cluster;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.backup.BackupFileConstants;
import herddb.backup.BackupUtils;
import herddb.backup.DumpedTableMetadata;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.client.TableSpaceDumpReceiver;
import herddb.client.TableSpaceRestoreSource;
import herddb.codec.RecordSerializer;
import herddb.core.DBManager;
import herddb.core.HerdDBInternalException;
import herddb.core.TableSpaceManager;
import herddb.core.TestUtils;
import herddb.file.FileCommitLogManager;
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.CommitLogResult;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.log.RestoredFromSnapshot;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ExtendedDataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A restore streams the content of a backup straight into the storage of the leader: no table is created and no record
 * is written through the commit log, so the log of a restored tablespace says nothing about the data the tablespace
 * holds. These tests are about the two markers the restore leaves on that log, the only trace of it that another node,
 * or the same node after a reboot, can read.
 *
 * <p>
 * Most of them run on a single standalone server, which is enough for everything that happens on the leader and lets
 * the log be read back from the files it is written to. The ones that need a commit log that behaves in a particular
 * way, because what they are about is what a boot finds on it, drive a bare {@link DBManager} instead. The
 * consequences for a replica that really follows a leader are covered by {@link BackupRestoreReplicaTest}.
 * </p>
 */
public class RestoreFromSnapshotMarkerTest {

    private static final String RESTORED_TABLESPACE = "restoredts";

    private static final String TABLE_NAME = "t1";

    /**
     * Number of rows contained in the backup.
     */
    private static final int ROWS_IN_BACKUP = 4;

    /**
     * How long we wait for a tablespace we expect to boot.
     */
    private static final int BOOT_TIMEOUT = 60000;

    /**
     * How long we wait for a tablespace we expect NOT to boot. It only has to be long enough for the activator to try
     * at least once.
     */
    private static final int FAILED_BOOT_TIMEOUT = 10000;

    /**
     * Text of the storage failure injected in the last step of a restore, recognisable in whatever the client reports.
     */
    private static final String STORAGE_FAILURE = "the storage of this node refuses to write";

    /**
     * Text of the failure injected on the client side, in the middle of a restore, where what the test is about is
     * what the server is left holding.
     */
    private static final String CLIENT_GAVE_UP_MID_RESTORE = "the client cannot read the rest of the backup";

    /**
     * How long a client that expects an immediate answer waits for it.
     */
    private static final int IMPATIENT_CLIENT_TIMEOUT = 20000;

    /**
     * How long the planner waits for a tablespace it cannot find, where the point of the test is that the
     * tablespace is not there at all.
     */
    private static final int PLANNER_WAIT_FOR_TABLESPACE = 2000;

    /**
     * Node the tests that drive a bare {@link DBManager} run on.
     */
    private static final String THIS_NODE = "localhost";

    /**
     * A node that is not this one, used where a tablespace has to be led by somebody else.
     */
    private static final String ANOTHER_NODE = "someothernode";

    /**
     * How long the leader of a tablespace may stay silent before the nodes that replicate it try to take it over.
     * The leaders these tests give a tablespace to never send a single ping, so this is as short as a tablespace is
     * allowed to have it.
     */
    private static final long LEADER_INACTIVITY_TIMEOUT = 5000;

    /**
     * How long a node that must not take a tablespace over is watched: the time the leader has to stay silent for
     * before the takeover is even considered, and then room for several passes of the activator.
     */
    private static final int TIME_GIVEN_TO_THE_TAKEOVER = (int) LEADER_INACTIVITY_TIMEOUT + 15000;

    /**
     * Uuid of the tablespace of the checks that ask the promotion guard directly, where the metadata is built by
     * hand instead of being created through the metadata storage.
     */
    private static final String TABLESPACE_UUID = "0e0e2a95a1e94a7f9d5e3d4d1d0cd8f8";

    /**
     * Where the marker of a restore sits on the log of those checks. The value itself does not matter, only that
     * there are positions before and after it.
     */
    private static final LogSequenceNumber POSITION_OF_THE_MARKER = new LogSequenceNumber(3, 7);

    /**
     * A position the local data of a node that downloaded the restored content is aligned to: past the marker, so
     * that node never reads the marker again.
     */
    private static final LogSequenceNumber POSITION_AFTER_THE_MARKER = new LogSequenceNumber(3, 8);

    /**
     * A log that cannot be replayed from where it is for a reason that has nothing to do with any restore. This is
     * what the commit log says when the ledgers that hold the missing part are gone.
     */
    private static final String LOG_IS_INCOMPLETE =
            "Cannot recover tablespace " + RESTORED_TABLESPACE + " from BookKeeper, not enough data";

    /**
     * A version a restore marker has never been written with. It only has to be something this version of HerdDB
     * does not know how to read.
     */
    private static final long UNKNOWN_MARKER_VERSION = 99;

    /**
     * How long a restore is allowed to stand still before the node it runs on gives up on it, where a test drives
     * that timeout. It only has to be long enough to be told from the time two calls take.
     */
    private static final long RESTORE_INACTIVITY_TIMEOUT = 1000;

    /**
     * How long a node that asked for a dump it is never going to get waits to be told so.
     */
    private static final int DUMP_FAILURE_TIMEOUT = 30000;

    /**
     * How long a tablespace that cannot be served here is watched, to see how often this node tries it again. The
     * activator makes a pass a second, so an unpaced node boots it about that many times.
     */
    private static final int TIME_GIVEN_TO_THE_BOOT_RETRIES = 15000;

    /**
     * How many boots of a tablespace that cannot be served here are more than that window is worth. Every one of them
     * reads the tail of the commit log of the tablespace in full and reports the same failure.
     */
    private static final int TOO_MANY_BOOTS = 7;

    /**
     * How many reads of the commit log of a tablespace this node cannot take the leadership of are more than the
     * window they are counted over is worth.
     */
    private static final int TOO_MANY_LOG_READS = 10;

    /**
     * How many times a tablespace that cannot be served here is booted before the test repairs it. By then the
     * attempts are far enough apart that the next one is several seconds away, which is what makes it possible to
     * tell a repair that was acted upon from one that merely happened to fall on the next attempt.
     */
    private static final int BOOTS_BEFORE_THE_REPAIR = 5;

    /**
     * How long an operator who has just moved the leadership of a tablespace waits for this node to act on it.
     */
    private static final int TIME_GIVEN_TO_THE_REPAIR = 6000;

    /**
     * A commit log that cannot be read at all, for a reason that has nothing to do with any restore.
     */
    private static final String LOG_CANNOT_BE_READ = "the ledgers of this tablespace cannot be opened";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * The marker that opens the restore has to be the first thing a reader of the log meets, well before any change of
     * a table that the log itself never created.
     */
    @Test
    public void testRestoreMarkersAreTheOnlyEntriesOnTheLog() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        String tableSpaceUUID;
        String nodeId;
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            createTableWithRows(server);
            restoreBackupOfTheDefaultTableSpace(server);
            nodeId = server.getNodeId();
            tableSpaceUUID = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).getTableSpaceUUID();
        }

        List<LogEntry> entries = new ArrayList<>();
        for (LogEntry entry : readLog(baseDir, tableSpaceUUID, nodeId)) {
            if (entry.type != LogEntryType.NOOP) {
                entries.add(entry);
            }
        }
        assertFalse("the commit log of the restored tablespace holds nothing at all", entries.isEmpty());

        LogEntry first = entries.get(0);
        assertEquals("the restore has to declare itself on the log before anything else, otherwise a node replaying"
                + " that log has no way to know that the data of the tablespace never went through it,"
                + " found " + first, LogEntryType.RESTORED_FROM_SNAPSHOT, first.type);
        // a log entry that names a table is routed to the manager of that table, which has no idea what this
        // entry is about: the marker describes the whole tablespace and must not name any table
        assertNull("the marker must not be bound to a table", first.tableName);
        assertEquals("the marker must not belong to a transaction", 0, first.transactionId);

        RestoredFromSnapshot started = RestoredFromSnapshot.deserialize(first.value.to_array());
        assertEquals(RestoredFromSnapshot.Phase.STARTED, started.getPhase());
        assertEquals(RESTORED_TABLESPACE, started.getTableSpaceName());
        assertEquals(tableSpaceUUID, started.getTableSpaceUUID());

        assertEquals("the restore writes nothing else to the log, it only opens and closes itself: " + entries,
                2, entries.size());
        LogEntry last = entries.get(1);
        assertEquals(LogEntryType.RESTORED_FROM_SNAPSHOT, last.type);
        RestoredFromSnapshot finished = RestoredFromSnapshot.deserialize(last.value.to_array());
        assertEquals(RestoredFromSnapshot.Phase.FINISHED, finished.getPhase());
        assertFalse("the marker that closes the restore has to tell which snapshot the data comes from",
                LogSequenceNumber.START_OF_TIME.equals(finished.getSnapshotLogSequenceNumber()));
    }

    /**
     * Everything a marker says has to survive the trip through the log: what a node does when it meets one is decided
     * out of the payload alone, long after the node that wrote it has gone.
     */
    @Test
    public void testTheMarkerSurvivesTheTripThroughTheLog() throws Exception {
        RestoredFromSnapshot written = new RestoredFromSnapshot(RestoredFromSnapshot.Phase.FINISHED,
                RESTORED_TABLESPACE, TABLESPACE_UUID, POSITION_OF_THE_MARKER);
        RestoredFromSnapshot readBack = RestoredFromSnapshot.deserialize(written.serialize());

        assertEquals(RestoredFromSnapshot.Phase.FINISHED, readBack.getPhase());
        assertEquals(RESTORED_TABLESPACE, readBack.getTableSpaceName());
        assertEquals(TABLESPACE_UUID, readBack.getTableSpaceUUID());
        assertEquals(POSITION_OF_THE_MARKER, readBack.getSnapshotLogSequenceNumber());
    }

    /**
     * A marker whose payload this version does not understand is refused rather than guessed at. The meaning of a
     * marker is that the content of the tablespace is not where the reader expects it, so a reader that got the
     * payload wrong would carry on with a tablespace it cannot serve.
     */
    @Test
    public void testAMarkerOfAnUnknownShapeIsRefused() throws Exception {
        try {
            RestoredFromSnapshot.deserialize(markerPayloadOfVersion(UNKNOWN_MARKER_VERSION));
            fail("a restore marker of an unknown shape was read as if this version had written it");
        } catch (IllegalArgumentException expected) {
        }
        // the same bytes, with the version this class writes: the payload itself is fine, it is the version that
        // makes the difference
        RestoredFromSnapshot marker = RestoredFromSnapshot.deserialize(markerPayloadOfVersion(1));
        assertEquals(RestoredFromSnapshot.Phase.STARTED, marker.getPhase());
        assertEquals(RESTORED_TABLESPACE, marker.getTableSpaceName());
        assertEquals(TABLESPACE_UUID, marker.getTableSpaceUUID());
        assertEquals(POSITION_OF_THE_MARKER, marker.getSnapshotLogSequenceNumber());
    }

    /**
     * The payload of a restore marker, written by hand so that a test can say which version it claims to be.
     */
    private static byte[] markerPayloadOfVersion(long version) throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(payload)) {
            out.writeVLong(version);
            out.writeVLong(0);
            out.writeVInt(RestoredFromSnapshot.Phase.STARTED.getCode());
            out.writeUTF(RESTORED_TABLESPACE);
            out.writeUTF(TABLESPACE_UUID);
            out.writeLong(POSITION_OF_THE_MARKER.ledgerId);
            out.writeLong(POSITION_OF_THE_MARKER.offset);
        }
        return payload.toByteArray();
    }

    /**
     * The markers must not get in the way of the node that ran the restore: it holds the data and its checkpoint is
     * above both of them, so a reboot right after the restore has nothing to replay and everything to load.
     */
    @Test
    public void testLeaderRebootsRightAfterTheRestore() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            createTableWithRows(server);
            restoreBackupOfTheDefaultTableSpace(server);
        }

        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            server.waitForTableSpaceBoot(RESTORED_TABLESPACE, BOOT_TIMEOUT, true);
            assertEquals("the restored tablespace lost its data across a reboot of the node that restored it",
                    ROWS_IN_BACKUP, countRowsOfTheRestoredTable(server));
        }
    }

    /**
     * A restore that stops in the middle leaves the tablespace with a fragment of the snapshot in it, and nobody can
     * fix that on the node that leads it: the data comes from a client, not from another node, and a leader has
     * nobody to download from. The tablespace must not come back serving that fragment, and it must not come back
     * empty either: an empty tablespace is indistinguishable from one that has just been created, so an application
     * would find no tables, create its own and build on a foundation that was supposed to hold the restored data.
     */
    @Test
    public void testInterruptedRestoreLeavesATableSpaceThatDoesNotServeClients() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            createTableWithRows(server);
            try (HDBClient client = newClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    // a backup that ends right after its own header: the restore declares itself on the log and
                    // then dies, exactly as it would if the process running it was killed
                    BackupUtils.restoreTableSpace(RESTORED_TABLESPACE, server.getNodeId(), connection,
                            new ByteArrayInputStream(truncatedBackup()), new ProgressListener() {
                            });
                    fail("the restore of a truncated backup was expected to fail");
                } catch (Exception expected) {
                }
            }

            // the connection that was driving the restore is gone and nobody else can complete it. The tablespace
            // must be taken out of service straight away: it holds a fragment of the snapshot, and a tablespace
            // that stays up in that state serves that fragment to whoever asks and never takes a checkpoint again
            assertTrue("the tablespace was left in service after the restore that was replacing its content was"
                    + " abandoned, holding a fragment of the snapshot",
                    waitForTableSpaceOutOfService(server, RESTORED_TABLESPACE));
        }

        assertTableSpaceRefusesToBootAndIsLeftUntouched(baseDir);
    }

    /**
     * The same as above, one step later: the restore streamed everything and declared itself complete, but the node
     * went down before the checkpoint that persists the restored tables. The tables only live in memory up to that
     * checkpoint, so on the next boot the log is all there is, and it describes nothing. This node is the leader, so
     * it cannot download the data from anywhere either, and the conclusion is the same.
     */
    @Test
    public void testRestoreThatNeverReachedItsCheckpointLeavesATableSpaceThatDoesNotBoot() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            createTableWithRows(server);
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // write the marker that closes a restore and stop here, without the checkpoint that
            // makes the restored data usable
            CommitLog log = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).getLog();
            RestoredFromSnapshot restore = new RestoredFromSnapshot(RestoredFromSnapshot.Phase.FINISHED,
                    RESTORED_TABLESPACE, server.getManager().getTableSpaceManager(RESTORED_TABLESPACE)
                    .getTableSpaceUUID(), new LogSequenceNumber(1, 1));
            log.log(LogEntryFactory.restoredFromSnapshot(restore), true).getLogSequenceNumber();
        }

        assertTableSpaceRefusesToBootAndIsLeftUntouched(baseDir);
    }

    /**
     * Boots a node whose data directory holds a tablespace whose content was replaced by a restore this node does not
     * hold, and checks everything that has to be true afterwards: the tablespace does not boot, a client asking for it
     * is not served, and the tablespace is still registered in the metadata every node of the cluster reads.
     *
     * <p>
     * That last point is the whole difference between an unavailable tablespace and a lost one. Nothing this node can
     * read tells a restore that was interrupted from one that completed on another node, so removing the tablespace
     * would take the restored content away from whichever node is holding it.
     * </p>
     */
    private void assertTableSpaceRefusesToBootAndIsLeftUntouched(Path baseDir) throws Exception {
        ServerConfiguration configuration = newServerConfigurationWithAutoPort(baseDir)
                // a query for a tablespace that is not served waits for it to show up, in case the node is still
                // starting. This one is never going to show up, and the whole minute the planner would spend
                // waiting for it adds nothing to what this test is about
                .set(ServerConfiguration.PROPERTY_PLANNER_WAITFORTABLESPACE_TIMEOUT, PLANNER_WAIT_FOR_TABLESPACE);
        try (Server server = new Server(configuration)) {
            server.start();
            server.waitForStandaloneBoot();

            assertFalse("a node that holds nothing of the tablespace booted it anyway, so an application finds an"
                    + " empty tablespace where the restored data was supposed to be",
                    server.getManager().waitForTablespace(RESTORED_TABLESPACE, FAILED_BOOT_TIMEOUT, false));
            assertTableSpaceIsStillRegistered(server.getManager());
            assertTableSpaceIsNotServed(server);
        }
    }

    /**
     * The activator checkpoints every tablespace of the node on its own schedule and knows nothing about restores. A
     * checkpoint that falls between the two markers writes the fragment of the snapshot the tablespace holds at that
     * moment to the storage and, above all, records that fragment as the state the tablespace is aligned to. The
     * marker that opened the restore is then below that position, and the log is replayed from strictly after it: the
     * next boot never sees the marker again and accepts half a snapshot as a healthy tablespace. On a leader the same
     * checkpoint also drops the ledgers up to that position, so the marker can be gone for good.
     * <p>
     * The restore is driven through the very calls the connection peer makes when it serves a client, one step at a
     * time, because what has to happen between two of those steps is the point of the test.
     * </p>
     */
    @Test
    public void testCheckpointInTheMiddleOfARestoreDoesNotHideIt() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
            String tableSpaceUUID = tableSpaceManager.getTableSpaceUUID();
            LogSequenceNumber checkpointBeforeTheRestore =
                    server.getManager().getDataStorageManager().getLastcheckpointSequenceNumber(tableSpaceUUID);

            // the first two steps of a restore driven by a client: the marker that opens it, then the first table
            tableSpaceManager.beginRestore();
            tableSpaceManager.beginRestoreTable(tableOfTheRestoredTableSpace().serialize(),
                    new LogSequenceNumber(1, 1));

            // the periodic checkpoint of the activator falls right here, in the middle of the restore
            server.getManager().checkpoint();

            assertEquals("a checkpoint taken in the middle of a restore moved the position the tablespace is aligned"
                    + " to above the marker that opened the restore: the log is replayed from strictly after that"
                    + " position, so no boot will ever meet the marker again",
                    checkpointBeforeTheRestore,
                    server.getManager().getDataStorageManager().getLastcheckpointSequenceNumber(tableSpaceUUID));

            // the node dies here, with the restore still running and no marker closing it
        }

        assertTableSpaceRefusesToBootAndIsLeftUntouched(baseDir);
    }

    /**
     * The last step of a restore is the only one that writes to the commit log and takes a checkpoint, so it is the
     * only one that can fail for a reason that has nothing to do with the statement being executed: an unavailable log
     * or a storage that refuses to write. Whatever the reason, the client is waiting for an answer to that request and
     * has to be told, otherwise it sits there until its own timeout expires and reports that instead of the real
     * problem.
     */
    @Test
    public void testFailureOfTheLastStepOfARestoreIsReportedToTheClient() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // the checkpoint the last step of the restore takes cannot be completed
            server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).setAfterTableCheckPointAction(() -> {
                throw new DataStorageManagerException(STORAGE_FAILURE);
            });

            try (HDBClient client = newImpatientClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    // one table and no rows: the least a restore can carry and still give the checkpoint of its
                    // last step something to write
                    connection.restoreTableSpace(RESTORED_TABLESPACE,
                            new RestoreSourceOfTables(tableOfTheRestoredTableSpace()));
                    fail("the restore was expected to fail, the checkpoint that closes it cannot be taken");
                } catch (HDBException expected) {
                    assertTrue("the client was not told why the restore failed, it got " + expected
                            + " instead. An error the last step of the restore cannot report leaves the client"
                            + " waiting for a reply that nobody is going to send",
                            String.valueOf(expected.getMessage()).contains(STORAGE_FAILURE));
                }
            }
        }
    }

    /**
     * Taking leadership reads the log a second time, with fencing on, and that pass is not a formality: fencing is
     * what makes the entries the previous incarnation of this node wrote just before dying readable at all, so
     * entries can show up there that the ordinary pass never saw. A restore that was left open is exactly such an
     * entry, and it has the same consequence there as on an ordinary boot: this node holds none of that snapshot and,
     * as the leader, has nobody to download it from, so it does not boot the tablespace.
     */
    @Test
    public void testRestoreLeftBehindByTheOldLeaderIsMetByTheFencedPass() throws Exception {
        LogEntry markerOfARestoreThatWasLeftOpen = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.STARTED, RESTORED_TABLESPACE, "",
                        LogSequenceNumber.START_OF_TIME));

        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log,
                        anEntryOnlyAFencedReaderSees(markerOfARestoreThatWasLeftOpen)));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE, Collections.singleton(THIS_NODE),
                            THIS_NODE, 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertFalse("the marker of the restore that was left open is readable only by the pass that fences the"
                    + " log, and that pass let the tablespace through: this node is serving a tablespace it holds"
                    + " nothing of", manager.waitForTablespace(RESTORED_TABLESPACE, FAILED_BOOT_TIMEOUT, false));
            assertTableSpaceIsStillRegistered(manager);
        }
    }

    /**
     * The very same log, a marker that opens a restore with no marker closing it, means two different things
     * depending on what the node that reads it can do about it. A node that only replicates the tablespace can
     * download the content from the leader, so it boots and waits for the marker that closes the restore to reach
     * it. Refusing to boot there would be wrong twice over, because a tablespace that fails to boot takes the whole
     * process down when {@code server.halt.on.tablespace.boot.error} is set, which is what the packaged
     * configuration does, so one node restoring a tablespace would kill every other node of the cluster along with
     * every other tablespace they serve.
     */
    @Test
    public void testReplicaBootsWhileTheLeaderIsStillRestoring() throws Exception {
        LogEntry markerOfARestoreStillRunning = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.STARTED, RESTORED_TABLESPACE, "",
                        LogSequenceNumber.START_OF_TIME));

        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, anEntryEveryReaderSees(markerOfARestoreStillRunning)));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            // a tablespace this node only replicates: the restore is running on the node that leads it
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                            new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), ANOTHER_NODE, 2, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertTrue("this node refused to boot a tablespace it only replicates because the leader is in the"
                    + " middle of a restore. It has nothing to do but wait: the marker that closes the restore will"
                    + " reach it down the log and that is what makes it download the new content",
                    manager.waitForTablespace(RESTORED_TABLESPACE, BOOT_TIMEOUT, false));
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(RESTORED_TABLESPACE);
            assertNotNull("no tablespace manager for " + RESTORED_TABLESPACE, tableSpaceManager);
            assertFalse("the tablespace booted as failed on a node that only has to wait for the restore of the"
                    + " leader to be over", tableSpaceManager.isFailed());
            assertFalse("this node is not the leader of " + RESTORED_TABLESPACE, tableSpaceManager.isLeader());

            assertNull("this node holds a content that does not match the tail of the log it is reading, and a"
                    + " checkpoint would record that content as the state of the tablespace, hiding the marker of"
                    + " the restore from the next boot",
                    tableSpaceManager.checkpoint(false, false, false));
        }
    }

    /**
     * A node that leads a tablespace and meets a restore marker above the point its own data stops at refuses to boot
     * it, and changes nothing else. Both markers lead there, because both say the same thing to a reader: the log
     * does not describe the content of the tablespace, and this node does not hold it.
     *
     * <p>
     * This is where a node ends up when an operator moves the leadership by hand, with
     * {@code ALTER TABLESPACE 'ts','leader:thisnode'}, onto a node that does not hold the restored content. The
     * automatic takeover refuses the promotion before it happens, but a leader named by an operator is not refused
     * anything.
     * </p>
     *
     * <p>
     * Not booting the tablespace is all this node may do. It has nothing of it and, as the leader, nobody to
     * download it from; and it cannot conclude anything about the restore itself, which may well have completed on a
     * node that is holding every byte of it. Removing the tablespace from the metadata the whole cluster shares
     * would take the restored data away from that node, so the tablespace is left exactly as it is and stays
     * unavailable until the leadership goes back.
     * </p>
     */
    @Test
    public void testLeaderThatDoesNotHoldTheContentRefusesToBootAndChangesNothing() throws Exception {
        for (RestoredFromSnapshot.Phase phase : RestoredFromSnapshot.Phase.values()) {
            LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                    new RestoredFromSnapshot(phase, RESTORED_TABLESPACE, "", LogSequenceNumber.START_OF_TIME));

            CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                    new LogWithSomethingAtTheEndOfRecovery(log, anEntryEveryReaderSees(marker)));

            try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                    new MemoryDataStorageManager(), logManager, null, null)) {
                manager.start();
                manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
                // this node leads the tablespace, and the restore that replaced its content ran on the other one
                manager.executeStatement(
                        new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                                new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), THIS_NODE, 2, 0, 0),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertFalse("a node that holds nothing of the tablespace booted it as its leader after meeting a"
                        + " marker of phase " + phase + ", so an application finds an empty tablespace where the"
                        + " restored data was supposed to be",
                        manager.waitForTablespace(RESTORED_TABLESPACE, FAILED_BOOT_TIMEOUT, false));
                assertTableSpaceIsStillRegistered(manager);
                TableSpace tableSpace =
                        manager.getMetadataStorageManager().describeTableSpace(RESTORED_TABLESPACE);
                assertEquals("the tablespace this node could not boot was altered, out of a marker of phase " + phase
                        + " that says nothing about what the other nodes hold", THIS_NODE, tableSpace.leaderId);
                assertEquals("the replicas of a tablespace this node could not boot were changed",
                        new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), tableSpace.replicas);
            }
        }
    }

    /**
     * The leadership of a tablespace can move onto this node while this node is booting it: recovery reads the whole
     * log and takes minutes on a large tablespace, and the metadata is read once before it starts and once after it
     * ends. A node that starts the boot as a replica and finishes it as the leader boots as the leader, and by then
     * the marker it read is behind it: the pass that takes leadership replays only the tail of the log, and a
     * download it was about to ask for cannot be served by the node that is now the leader itself.
     *
     * <p>
     * What it must not do is boot. Between the two markers the tablespace holds a fragment of a snapshot and takes no
     * checkpoint, and a node that reached the leader branch having forgotten that would take the first checkpoint
     * that comes along, write that fragment to the storage and move the position the tablespace is aligned to above
     * the marker, which no boot would ever meet again. It must not stop the node either: the state is permanent, so
     * a node that stopped over it would be gone for as long as it takes an operator to notice.
     * </p>
     */
    @Test
    public void testNodeThatBecomesLeaderWhileItIsRecoveringDoesNotBoot() throws Exception {
        for (RestoredFromSnapshot.Phase phase : RestoredFromSnapshot.Phase.values()) {
            LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                    new RestoredFromSnapshot(phase, RESTORED_TABLESPACE, "", LogSequenceNumber.START_OF_TIME));
            AtomicReference<DBManager> theNode = new AtomicReference<>();
            AtomicBoolean leadershipAlreadyMoved = new AtomicBoolean();
            AtomicInteger halts = new AtomicInteger();

            CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                    new LogWithSomethingAtTheEndOfRecovery(log, (from, consumer, fencing) -> {
                        if (fencing) {
                            // the pass that takes leadership starts from the tail of the log, well above the
                            // marker, so it never meets it
                            return;
                        }
                        if (leadershipAlreadyMoved.compareAndSet(false, true)) {
                            makeThisNodeTheLeader(theNode.get());
                        }
                        consumer.accept(TAIL_OF_THE_LOG, marker);
                    }));

            try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                    new MemoryDataStorageManager(), logManager, null, null)) {
                theNode.set(manager);
                manager.setHaltOnTableSpaceBootError(true);
                manager.setHaltProcedure(halts::incrementAndGet);
                manager.start();
                manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
                // this node only replicates the tablespace when the boot begins
                manager.executeStatement(
                        new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                                new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), ANOTHER_NODE, 2, 0, 0),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue("the leadership never moved onto this node while it was recovering, so this test is not"
                        + " about what it says it is, with a marker of phase " + phase,
                        leadershipAlreadyMoved.get()
                                || waitForLeadershipToMove(manager, RESTORED_TABLESPACE, THIS_NODE, BOOT_TIMEOUT));
                assertFalse("this node became the leader of the tablespace in the middle of its own recovery, after"
                        + " meeting a marker of phase " + phase + ", and booted it anyway: it is serving a content"
                        + " that is not the content of the tablespace, and the next checkpoint buries the marker"
                        + " for good", manager.waitForTablespace(RESTORED_TABLESPACE, FAILED_BOOT_TIMEOUT, false));
                assertEquals("the node stopped itself over a tablespace it can never serve, after meeting a marker"
                        + " of phase " + phase, 0, halts.get());
                assertTableSpaceIsStillRegistered(manager);
            }
        }
    }

    /**
     * Hands the leadership of the tablespace to the node that is running, which is what an operator or a takeover
     * does to the metadata while that node is busy with something else.
     */
    private static void makeThisNodeTheLeader(DBManager manager) throws LogNotAvailableException {
        try {
            MetadataStorageManager metadata = manager.getMetadataStorageManager();
            TableSpace previous = metadata.describeTableSpace(RESTORED_TABLESPACE);
            metadata.updateTableSpace(TableSpace.builder().cloning(previous).leader(THIS_NODE).build(), previous);
        } catch (Exception error) {
            throw new LogNotAvailableException(error);
        }
    }

    /**
     * A tablespace this node cannot serve must not take the node down with it, whatever
     * {@code server.halt.on.tablespace.boot.error} says. Stopping the process is meant for a failure a restart may
     * clear; this one never clears itself, so a node that stopped over it would stop again at every start, taking
     * every other tablespace it serves with it and leaving nobody able to receive the command that repairs it.
     *
     * <p>
     * And the command has to work. Dropping the tablespace is the way out when no node holds its content any more,
     * and it is aimed at a tablespace that is not running anywhere: it goes straight to the metadata, from whichever
     * node the client happens to be connected to.
     * </p>
     */
    @Test
    public void testTableSpaceThatCannotBeServedNeitherHaltsTheNodeNorResistsBeingDropped() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            createTableWithRows(server);
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // the content of the tablespace was replaced and this node never got it: the marker is on the log and
            // nothing of the replacement ever reached the storage
            CommitLog log = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).getLog();
            RestoredFromSnapshot restore = new RestoredFromSnapshot(RestoredFromSnapshot.Phase.FINISHED,
                    RESTORED_TABLESPACE, server.getManager().getTableSpaceManager(RESTORED_TABLESPACE)
                    .getTableSpaceUUID(), new LogSequenceNumber(1, 1));
            log.log(LogEntryFactory.restoredFromSnapshot(restore), true).getLogSequenceNumber();
        }

        AtomicInteger halts = new AtomicInteger();
        ServerConfiguration configuration = newServerConfigurationWithAutoPort(baseDir)
                // what the packaged configuration of a server does
                .set(ServerConfiguration.PROPERTY_HALT_ON_TABLESPACE_BOOT_ERROR, true)
                .set(ServerConfiguration.PROPERTY_PLANNER_WAITFORTABLESPACE_TIMEOUT, PLANNER_WAIT_FOR_TABLESPACE);
        try (Server server = new Server(configuration)) {
            server.getManager().setHaltProcedure(halts::incrementAndGet);
            server.start();
            server.waitForStandaloneBoot();

            assertFalse("a node that holds nothing of the tablespace booted it anyway",
                    server.getManager().waitForTablespace(RESTORED_TABLESPACE, FAILED_BOOT_TIMEOUT, false));
            assertEquals("the node stopped itself because one tablespace cannot be served. It stops again at every"
                    + " start, so every other tablespace it serves is down for good and there is no live node left"
                    + " to give the command that repairs it to", 0, halts.get());
            assertEquals("the tablespaces this node can serve stopped answering because of the one it cannot",
                    ROWS_IN_BACKUP, countRowsOfTheDefaultTable(server));

            // the way out, issued the way an operator issues it: an ordinary connection, aimed at a tablespace that
            // works, naming the tablespace that does not
            try (HDBClient client = newClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                connection.executeUpdate(TableSpace.DEFAULT,
                        "DROP TABLESPACE '" + RESTORED_TABLESPACE + "'", TransactionContext.NOTRANSACTION_ID,
                        false, true, Collections.emptyList());
            }

            assertNull("a tablespace that cannot be served on any node cannot be dropped either, so its name is"
                    + " taken for good and the restore can never be run again under it",
                    server.getManager().getMetadataStorageManager().describeTableSpace(RESTORED_TABLESPACE));
        }
    }

    /**
     * A tablespace this node cannot serve is refused at every boot, and nothing that happens between one pass of the
     * activator and the next changes that. Booting it once a second opens every ledger of its commit log that follows
     * the last checkpoint of this node once a second, and reports the same failure once a second, for as long as
     * nobody repairs anything: the one tablespace that cannot be served would cost more than all the ones that can,
     * and would bury their logs under its own.
     *
     * <p>
     * So the attempts are spaced out. They are spaced out and not given up on: what is put off is the attempt, never
     * the question, because the answer lives in a commit log that belongs to whoever leads the tablespace and the
     * position this node reads it from stands still exactly while this is going on.
     * </p>
     */
    @Test
    public void testTableSpaceThisNodeCannotServeIsNotBootedOnEveryPass() throws Exception {
        AtomicInteger boots = new AtomicInteger();
        try (DBManager manager = nodeThatCannotServeTheRestoredTableSpace(boots)) {
            createTableWithRowsInTheDefaultTableSpace(manager);
            giveThisNodeTheLeadershipOfTheRestoredTableSpace(manager);

            int bootsWhileWatching = waitForCount(boots, TOO_MANY_BOOTS, TIME_GIVEN_TO_THE_BOOT_RETRIES);
            assertTrue("this node booted a tablespace it cannot serve " + bootsWhileWatching + " times in "
                    + TIME_GIVEN_TO_THE_BOOT_RETRIES + " ms, which is once per pass of the activator: each of them"
                    + " reads the tail of the commit log of the tablespace in full and reports the same failure",
                    bootsWhileWatching < TOO_MANY_BOOTS);
            assertTrue("this node booted a tablespace it cannot serve only " + bootsWhileWatching + " time(s) in "
                    + TIME_GIVEN_TO_THE_BOOT_RETRIES + " ms: it is not waiting before it tries again, it has stopped"
                    + " asking, and a tablespace that somebody repairs elsewhere is never picked up",
                    bootsWhileWatching > 1);

            assertFalse("the node stopped over a tablespace it cannot serve", manager.isStopped());
            assertEquals("the tablespaces this node can serve stopped answering because of the one it cannot",
                    ROWS_IN_BACKUP, countRowsOfTheDefaultTable(manager));
        }
    }

    /**
     * The wait between one attempt and the next is dropped as soon as the tablespace is not the tablespace that was
     * refused any more. Giving the leadership to a node that holds the content is one of the two ways out of a
     * tablespace that cannot be served here, and it is issued by an operator who is watching: making them wait out an
     * interval that was chosen for a tablespace nobody was repairing would be its own kind of failure.
     */
    @Test
    public void testLeadershipGivenToAnotherNodeIsActedUponWithoutWaitingForTheNextBoot() throws Exception {
        AtomicInteger boots = new AtomicInteger();
        try (DBManager manager = nodeThatCannotServeTheRestoredTableSpace(boots)) {
            giveThisNodeTheLeadershipOfTheRestoredTableSpace(manager);

            int bootsBeforeTheRepair = waitForCount(boots, BOOTS_BEFORE_THE_REPAIR, TIME_GIVEN_TO_THE_BOOT_RETRIES);
            assertTrue("the tablespace was booted only " + bootsBeforeTheRepair + " times, so the attempts are not"
                    + " far enough apart yet for this test to tell a repair that was acted upon from one that fell on"
                    + " the next attempt anyway", bootsBeforeTheRepair >= BOOTS_BEFORE_THE_REPAIR);

            // the way out, issued by an operator: the leadership goes to the node that holds the content
            TestUtils.execute(manager, "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + ANOTHER_NODE + "'",
                    Collections.emptyList());

            assertTrue("this node did not boot the tablespace again within " + TIME_GIVEN_TO_THE_REPAIR + " ms of the"
                    + " leadership being given to another node, so an operator repairing a tablespace waits out an"
                    + " interval that was chosen for the tablespace as it was before the repair",
                    waitForCount(boots, bootsBeforeTheRepair + 1, TIME_GIVEN_TO_THE_REPAIR) > bootsBeforeTheRepair);
        }
    }

    /**
     * A tablespace whose content is not here is the one boot failure this node does not stop over, and it has to stay
     * the only one. Every other way a boot can fail leaves the state of that tablespace unknown, which is what
     * {@code server.halt.on.tablespace.boot.error} is for: the node that cannot say what it holds stops instead of
     * serving it.
     */
    @Test
    public void testBootFailureThatIsNotTheRefusalOfATableSpaceStillStopsTheNode() throws Exception {
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, (from, consumer, fencing) -> {
                    throw new LogNotAvailableException(LOG_CANNOT_BE_READ);
                }));

        AtomicInteger halts = new AtomicInteger();
        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.setHaltOnTableSpaceBootError(true);
            manager.setHaltProcedure(halts::incrementAndGet);
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE, Collections.singleton(THIS_NODE),
                            THIS_NODE, 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertTrue("the boot of a tablespace failed because its commit log cannot be read, which says nothing"
                    + " about any restore and leaves the state of that tablespace unknown, and the node did not stop"
                    + " although " + ServerConfiguration.PROPERTY_HALT_ON_TABLESPACE_BOOT_ERROR + " is on",
                    waitForCount(halts, 1, FAILED_BOOT_TIMEOUT) >= 1);
        }
    }

    /**
     * A node that holds nothing of the restored tablespace and is its leader, so that every boot of that tablespace is
     * refused. The logs it is asked to create for that tablespace are counted, which is one per boot: the log of a
     * tablespace is created just before the tablespace manager that boots it and thrown away with it.
     */
    private DBManager nodeThatCannotServeTheRestoredTableSpace(AtomicInteger boots) throws Exception {
        LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.FINISHED, RESTORED_TABLESPACE, "",
                        LogSequenceNumber.START_OF_TIME));

        CommitLogManager logManager = commitLogManagerWhere((tableSpaceName, log) -> {
            if (!RESTORED_TABLESPACE.equals(tableSpaceName)) {
                return log;
            }
            boots.incrementAndGet();
            return new LogWithSomethingAtTheEndOfRecovery(log, anEntryEveryReaderSees(marker));
        });

        DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null);
        try {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
        } catch (Exception error) {
            manager.close();
            throw error;
        }
        return manager;
    }

    /**
     * Creates the restored tablespace with this node as its leader and another node as a replica, which is the state
     * an operator moves the leadership out of.
     */
    private static void giveThisNodeTheLeadershipOfTheRestoredTableSpace(DBManager manager) throws Exception {
        manager.executeStatement(
                new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                        new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), THIS_NODE, 2, 0, 0),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
    }

    /**
     * Waits for a counter to reach a value and reports what it holds at the end of the wait, whether it got there or
     * not: what these tests are about is how often something happens, so both answers are results.
     */
    private static int waitForCount(AtomicInteger counter, int target, int timeout) throws Exception {
        for (int i = 0; i < timeout / 100 && counter.get() < target; i++) {
            Thread.sleep(100);
        }
        return counter.get();
    }

    /**
     * A node that is only watching a restore somebody else is running holds the content the restore is replacing,
     * and cannot checkpoint until the restore is closed: a checkpoint would record that content as the state of the
     * tablespace and bury the marker that opened the restore. Nothing is ever written to the log to say that a
     * restore was abandoned, so a node whose restore stands still for good leaves every replica of that tablespace
     * in that state for good: no checkpoint, dirty pages never persisted, and every boot replaying the log from the
     * same place.
     */
    @Test
    public void testReplicaStopsWaitingForARestoreOfAnotherNodeThatStandsStill() throws Exception {
        LogEntry markerOfARestoreOfAnotherNode = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.STARTED, RESTORED_TABLESPACE, "",
                        LogSequenceNumber.START_OF_TIME));

        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, anEntryEveryReaderSees(markerOfARestoreOfAnotherNode)));

        ServerConfiguration configuration = new ServerConfiguration()
                .set(ServerConfiguration.PROPERTY_RESTORE_MAX_INACTIVITY_TIME, RESTORE_INACTIVITY_TIMEOUT);

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null, configuration, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                            new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), ANOTHER_NODE, 2, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue("this node never booted the tablespace it replicates, so it was never in the position this"
                    + " test is about", manager.waitForTablespace(RESTORED_TABLESPACE, BOOT_TIMEOUT, false));

            // the activator is the one that takes a failed tablespace out of service and boots it again, and the
            // point of the test is what the checkpoint decides before it gets there
            manager.setActivatorPauseStatus(true);
            try {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(RESTORED_TABLESPACE);
                assertNotNull("no tablespace manager for " + RESTORED_TABLESPACE, tableSpaceManager);

                assertNull("a checkpoint was taken while the content of the tablespace was being replaced: it"
                        + " records the content this node still holds as the state of the tablespace and buries"
                        + " the marker of the restore below the position the next boot replays from",
                        tableSpaceManager.checkpoint(false, false, false));
                assertFalse("a restore whose marker was read a moment ago was given up on, so a restore of a"
                        + " snapshot large enough can never be watched to the end",
                        tableSpaceManager.isFailed());

                Thread.sleep(RESTORE_INACTIVITY_TIMEOUT * 2);
                assertNull("a checkpoint was taken while the content of the tablespace was being replaced",
                        tableSpaceManager.checkpoint(false, false, false));
                assertTrue("this node is still waiting for a restore that has been standing still longer than any"
                        + " restore is allowed to: it never takes another checkpoint, so nothing it holds is ever"
                        + " persisted again and every boot replays the log from the same place",
                        tableSpaceManager.isFailed());
            } finally {
                manager.setActivatorPauseStatus(false);
            }
        }
    }

    /**
     * A log can refuse to be replayed from where it is for reasons that have nothing to do with a restore, a ledger
     * that is no longer available being the everyday one. The pass that takes leadership has to report what really
     * happened: telling an operator who has just lost a ledger that a snapshot replaced the tablespace and that the
     * restore has to be run again sends them looking for a restore that never happened.
     */
    @Test
    public void testLeadershipTakeoverDoesNotBlameARestoreThatNeverHappened() throws Exception {
        // only the pass that takes leadership fails: the ordinary boot has to get through, otherwise the test
        // would be about the wrong pass over the log
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, (from, consumer, fencing) -> {
                    if (fencing) {
                        throw new FullRecoveryNeededException(LOG_IS_INCOMPLETE);
                    }
                }));

        BootFailures failures = new BootFailures();
        Logger rootLogger = Logger.getLogger("");
        rootLogger.addHandler(failures);
        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE, Collections.singleton(THIS_NODE),
                            THIS_NODE, 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertFalse("a tablespace whose log cannot be replayed must not take leadership",
                    manager.waitForTablespace(RESTORED_TABLESPACE, FAILED_BOOT_TIMEOUT, false));
            assertNotNull("taking leadership of " + RESTORED_TABLESPACE + " failed without reporting the reason the"
                    + " log gave", failures.logCannotBeReplayed.get());
            Throwable blamedOnARestore = failures.restoreBlamed.get();
            if (blamedOnARestore != null) {
                fail("taking leadership of " + RESTORED_TABLESPACE + " failed because the log cannot be replayed,"
                        + " which has nothing to do with a restore, and the operator was told to run a restore"
                        + " again\n" + stackTraceOf(blamedOnARestore));
            }
        } finally {
            rootLogger.removeHandler(failures);
        }
    }

    /**
     * A node that replicates a tablespace whose content was replaced by a restore, and that has not downloaded that
     * content yet, must not be promoted to leader when the leader goes away.
     *
     * <p>
     * Its log can be read from end to end, which is all that the promotion guard used to ask for, and it still does
     * not describe the tablespace: the restore streamed its data straight into the storage of the leader and left
     * nothing but its markers on the log. This node holds none of that content and cannot rebuild it, and a leader
     * has nobody to download from, so the moment it boots as the leader it refuses to boot the tablespace at all.
     * Taking the leadership would turn a tablespace that is merely leaderless into one that is not served by
     * anybody, and it would take it away from a replica that did download the whole content and could lead it.
     * </p>
     *
     * <p>
     * Refusing the promotion leaves the tablespace without a leader until the old one comes back or an operator steps
     * in, which is the price of leaving the leadership to a node that can serve it.
     * </p>
     */
    @Test
    public void testReplicaThatNeverGotTheRestoredContentIsNotPromotedToLeader() throws Exception {
        LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.STARTED, RESTORED_TABLESPACE, "",
                        LogSequenceNumber.START_OF_TIME));

        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, anEntryEveryReaderSees(marker)));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            // a tablespace this node only replicates, whose leader never sends a ping: the activator of this node is
            // free to take it over as soon as it decides that the leader is gone
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                            new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), ANOTHER_NODE, 2, 0,
                            LEADER_INACTIVITY_TIMEOUT),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue("this node never booted the tablespace it replicates, so it was never in the position this"
                    + " test is about", manager.waitForTablespace(RESTORED_TABLESPACE, BOOT_TIMEOUT, false));

            assertFalse("this node took the leadership of a tablespace it holds nothing of, so nobody serves it any"
                    + " more and a replica that did download the restored content cannot take it either",
                    waitForLeadershipToMove(manager, RESTORED_TABLESPACE, THIS_NODE,
                            TIME_GIVEN_TO_THE_TAKEOVER));
            assertTableSpaceIsStillRegistered(manager);
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(RESTORED_TABLESPACE);
            assertNotNull("this node stopped replicating " + RESTORED_TABLESPACE, tableSpaceManager);
            assertFalse("this node is leading a tablespace whose content it never got",
                    tableSpaceManager.isLeader());
        }
    }

    /**
     * Whether this node could lead a tablespace is answered by reading the part of the commit log of that tablespace
     * this node has not replayed yet, and the question is asked for as long as the leader of the tablespace stays
     * silent. A leader can stay silent for a very long time, and the node watching a restore it did not get is the
     * worst case of all: it takes no checkpoint while it waits, so the position it reads that log from never moves and
     * every pass reads the same ledgers, from the same place, to the same end.
     *
     * <p>
     * So the question is asked less and less often, and it never stops being asked: the answer is a property of a log
     * that keeps growing under a node that is standing still, and the one thing that must not happen is a node that
     * decided once that it cannot lead a tablespace and never looks again.
     * </p>
     */
    @Test
    public void testLeadershipThisNodeCannotTakeIsNotAskedAboutOnEveryPass() throws Exception {
        LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.STARTED, RESTORED_TABLESPACE, "",
                        LogSequenceNumber.START_OF_TIME));
        AtomicInteger logReads = new AtomicInteger();

        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, (from, consumer, fencing) -> {
                    logReads.incrementAndGet();
                    consumer.accept(TAIL_OF_THE_LOG, marker);
                }));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            // a tablespace this node only replicates, whose leader never sends a ping: this node asks itself whether
            // it could take it over on every pass of its activator, from the moment it gives up on that leader
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE,
                            new HashSet<>(Arrays.asList(THIS_NODE, ANOTHER_NODE)), ANOTHER_NODE, 2, 0,
                            LEADER_INACTIVITY_TIMEOUT),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue("this node never booted the tablespace it replicates, so it was never in the position this"
                    + " test is about", manager.waitForTablespace(RESTORED_TABLESPACE, BOOT_TIMEOUT, false));

            int reads = waitForCount(logReads, TOO_MANY_LOG_READS, TIME_GIVEN_TO_THE_TAKEOVER);
            assertTrue("this node read the commit log of a tablespace it cannot take the leadership of " + reads
                    + " times in " + TIME_GIVEN_TO_THE_TAKEOVER + " ms, which is once per pass of its activator: it"
                    + " opens every ledger that follows a checkpoint position that is not moving, to reach the same"
                    + " conclusion every second for as long as the leader stays away",
                    reads < TOO_MANY_LOG_READS);
            assertTrue("this node read the commit log of the tablespace only " + reads + " time(s) in "
                    + TIME_GIVEN_TO_THE_TAKEOVER + " ms: it is not asking less often, it has stopped asking, and a"
                    + " node that could lead the tablespace by the time the answer changed would never find out",
                    reads > 2);
            assertEquals("this node took the leadership of a tablespace it holds nothing of, so this test watched a"
                    + " node that had stopped asking for a reason of its own", ANOTHER_NODE,
                    manager.getMetadataStorageManager().describeTableSpace(RESTORED_TABLESPACE).leaderId);
        }
    }

    /**
     * The guard above must cost nothing to the replica the failover is there for. A node that has downloaded the
     * content of the restored tablespace and checkpointed it is aligned above the marker: it never reads that part of
     * the log again, and it is the one node that can lead the tablespace, so it has to be allowed to.
     */
    @Test
    public void testReplicaThatDownloadedTheRestoredContentIsStillPromoted() throws Exception {
        LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.FINISHED, RESTORED_TABLESPACE, TABLESPACE_UUID,
                        POSITION_OF_THE_MARKER));

        assertTrue("a replica whose data is aligned above the marker of the restore cannot take leadership, so a"
                + " tablespace restored from a snapshot can never fail over again",
                isLocallyRecoverable(anEntryAt(POSITION_OF_THE_MARKER, marker),
                        dataAlignedTo(POSITION_AFTER_THE_MARKER)));
        // the same log, read by a node whose data stops below the marker: this is the node the guard is for, and it
        // is what tells the answer above from a log that was never read
        assertFalse("a replica whose data stops below the marker of the restore was allowed to take leadership",
                isLocallyRecoverable(anEntryAt(POSITION_OF_THE_MARKER, marker), new MemoryDataStorageManager()));
    }

    /**
     * A tablespace that was never restored has no marker on its log, and nothing about its failover changes: neither
     * the ordinary one, of a replica that holds a checkpoint of its own, nor the failover of a tablespace that has
     * never been written to, whose replicas hold no checkpoint at all and still have to be able to take over.
     */
    @Test
    public void testTableSpaceThatWasNeverRestoredIsStillPromoted() throws Exception {
        LogEntry ordinaryEntry = LogEntryFactory.beginTransaction(1);

        assertTrue("a replica of a tablespace that was never restored cannot take leadership any more",
                isLocallyRecoverable(anEntryAt(POSITION_OF_THE_MARKER, ordinaryEntry),
                        dataAlignedTo(POSITION_AFTER_THE_MARKER)));
        assertTrue("a replica of a tablespace that was never written to, and that therefore holds no checkpoint at"
                + " all, cannot take leadership any more: an empty log is not a restore",
                isLocallyRecoverable((from, consumer, fencing) -> {
                }, new MemoryDataStorageManager()));
    }

    /**
     * A log that cannot be replayed from where the data of the node stopped refuses the promotion on its own, as it
     * always did, and there is nothing to look for in a log that cannot be read in the first place.
     */
    @Test
    public void testALogThatCannotBeReplayedStillRefusesThePromotionOnItsOwn() throws Exception {
        LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.STARTED, RESTORED_TABLESPACE, TABLESPACE_UUID,
                        LogSequenceNumber.START_OF_TIME));
        AtomicInteger reads = new AtomicInteger();
        EndOfRecovery countedRead = (from, consumer, fencing) -> {
            reads.incrementAndGet();
            consumer.accept(POSITION_OF_THE_MARKER, marker);
        };

        CommitLogManager logManager = commitLogManagerWhere((tableSpaceName, log) ->
                new LogWithSomethingAtTheEndOfRecovery(log, countedRead, false));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            assertFalse("a node whose log cannot be replayed from the position its data stopped at was allowed to"
                    + " take leadership", manager.isTableSpaceLocallyRecoverable(aTableSpaceLedBySomebodyElse()));
            assertEquals("the log was read even though it had already said that it cannot be replayed", 0,
                    reads.get());
        }
    }

    /**
     * Asks the promotion guard whether this node could lead a tablespace it replicates, out of a given commit log and
     * a given local storage. What the guard reads of the tablespace is its name and its uuid, so the metadata is
     * built by hand: booting the tablespace is not what these checks are about.
     */
    private static boolean isLocallyRecoverable(
            EndOfRecovery log, DataStorageManager dataStorageManager
    ) throws Exception {
        CommitLogManager logManager = commitLogManagerWhere((tableSpaceName, plainLog) ->
                new LogWithSomethingAtTheEndOfRecovery(plainLog, log));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                dataStorageManager, logManager, null, null)) {
            manager.start();
            return manager.isTableSpaceLocallyRecoverable(aTableSpaceLedBySomebodyElse());
        }
    }

    /**
     * The metadata of a tablespace this node replicates and somebody else leads.
     */
    private static TableSpace aTableSpaceLedBySomebodyElse() {
        return TableSpace
                .builder()
                .name(RESTORED_TABLESPACE)
                .uuid(TABLESPACE_UUID)
                .leader(ANOTHER_NODE)
                .replica(ANOTHER_NODE)
                .replica(THIS_NODE)
                .build();
    }

    /**
     * A storage that reports the local data of the tablespace as aligned to a given position, which is what the
     * storage of a node that has downloaded the content of the tablespace and checkpointed it says.
     */
    private static DataStorageManager dataAlignedTo(LogSequenceNumber position) {
        DataStorageManagerAlignedTo dataStorageManager = new DataStorageManagerAlignedTo();
        dataStorageManager.alignTo(position);
        return dataStorageManager;
    }

    /**
     * The same, for a test that has to move the data of the node while it runs: that is what a node does when it
     * downloads the content of a restored tablespace and checkpoints it, and it is the one thing that can change
     * the answer of the promotion guard.
     */
    private static final class DataStorageManagerAlignedTo extends MemoryDataStorageManager {

        private volatile LogSequenceNumber position = LogSequenceNumber.START_OF_TIME;

        void alignTo(LogSequenceNumber position) {
            this.position = position;
        }

        @Override
        public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) {
            return TABLESPACE_UUID.equals(tableSpace) ? position : LogSequenceNumber.START_OF_TIME;
        }
    }

    /**
     * The last step of a restore writes the marker that closes it and then takes the checkpoint that makes the
     * restored content usable. If that checkpoint fails the tablespace is left with a log that claims the restore is
     * complete and a storage that knows nothing about it: the next boot of the leader refuses to start it. That is
     * the right conclusion, but it has to be reached now, while somebody is watching, and not months later at the
     * first restart of the node.
     */
    @Test
    public void testFailureOfTheCheckpointThatClosesARestoreTakesTheTableSpaceOutOfService() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // the checkpoint that closes the restore cannot be completed
            server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).setAfterTableCheckPointAction(() -> {
                throw new DataStorageManagerException(STORAGE_FAILURE);
            });

            try (HDBClient client = newImpatientClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    // one table and no rows: the least a restore can carry and still give the checkpoint that
                    // closes it something to write
                    connection.restoreTableSpace(RESTORED_TABLESPACE,
                            new RestoreSourceOfTables(tableOfTheRestoredTableSpace()));
                    fail("the restore was expected to fail, the checkpoint that closes it cannot be taken");
                } catch (HDBException expected) {
                }
            }

            assertTrue("the restore declared itself complete on the log and then failed to take the checkpoint that"
                    + " makes the restored content usable. The tablespace was left in service anyway, so nothing"
                    + " tells anybody that it will refuse to boot from now on",
                    waitForTableSpaceOutOfService(server, RESTORED_TABLESPACE));
        }
    }

    /**
     * A log backed by a replicated storage acknowledges a write asynchronously, and that acknowledgement can fail on
     * its own after the entry has already been handed over. The marker that opens a restore is then on the log for
     * everybody else to read, and the node that wrote it is the only one that does not know: if that leaves the
     * checkpoints running, one of them records the fragment of the snapshot the tablespace holds as the state of the
     * tablespace and buries the marker below the position the next boot replays from.
     */
    @Test
    public void testAMarkerWhosePositionIsLostStillStopsTheCheckpoints() throws Exception {
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogThatCannotReportWhereItWrote(log, LogEntryType.RESTORED_FROM_SNAPSHOT));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE, Collections.singleton(THIS_NODE),
                            THIS_NODE, 1, BOOT_TIMEOUT, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            writeSomethingOnTheLogOfTheRestoredTableSpace(manager);

            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(RESTORED_TABLESPACE);
            assertNotNull("the checkpoints of " + RESTORED_TABLESPACE + " do not work even before the restore, this"
                    + " test would prove nothing", tableSpaceManager.checkpoint(false, false, false));

            try {
                tableSpaceManager.beginRestore();
                fail("the restore was expected to fail, the log cannot say where it wrote the marker");
            } catch (LogNotAvailableException expected) {
            }

            assertNull("the marker that opens the restore is on the log, only its position was lost. A checkpoint"
                    + " taken now records the fragment of the snapshot this tablespace holds as its content and"
                    + " buries the marker below the position the next boot replays from",
                    tableSpaceManager.checkpoint(false, false, false));
        }
    }

    /**
     * A tablespace manager that has declared itself failed is on its way out of service: the activator is about to
     * stop it and boot a new one, and that new boot is what puts the tablespace right. Until then whatever it holds
     * is not to be trusted, and the periodic checkpoint of the node, which knows nothing about any of this, must not
     * record it as the state the tablespace is aligned to.
     */
    @Test
    public void testAFailedTableSpaceTakesNoCheckpoint() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());
            server.getManager().executeStatement(new CreateTableStatement(tableOfTheRestoredTableSpace()),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // the activator is the one that takes a failed tablespace out of service: it has to stay out of the
            // way, the point of the test is what happens before it gets there
            server.getManager().setActivatorPauseStatus(true);
            try {
                TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                assertNotNull("the checkpoints of " + RESTORED_TABLESPACE + " do not work even before the failure,"
                        + " this test would prove nothing", tableSpaceManager.checkpoint(false, false, false));

                tableSpaceManager.abortRestore("the connection that was running it is gone");

                assertTrue("a tablespace manager that gave up is expected to declare itself failed",
                        tableSpaceManager.isFailed());
                assertNull("a checkpoint of a failed tablespace records a content nobody trusts as the state the"
                        + " tablespace is aligned to, which is exactly what the boot that follows has to undo",
                        tableSpaceManager.checkpoint(false, false, false));
            } finally {
                server.getManager().setActivatorPauseStatus(false);
            }
        }
    }

    /**
     * A client whose restore failed in the middle and who simply runs it again, on the same connection, is the most
     * ordinary thing a client can do. The second attempt is refused, and it has to be: the first one left tables
     * behind, and a restore replaces a tablespace that holds nothing of its own.
     *
     * <p>
     * A refusal owns nothing and must not take the tablespace out of service when the connection closes, which is
     * why the connection forgets a tablespace whose restore was refused. What it must not forget is the attempt
     * that is still open: the two are told apart by the tablespace name, and here they share it. Handing back the
     * ownership of the first attempt leaves a tablespace whose checkpoints are inhibited, whose commit log is never
     * trimmed and that nothing ever reboots, with not one line anywhere saying so.
     * </p>
     */
    @Test
    public void testARestoreRefusedOnTopOfAnOpenOneDoesNotDisownIt() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            try (HDBClient client = newClientWithOneConnectionPerServer();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                try {
                    // the first attempt opens the restore, streams one table and then dies on the client side,
                    // which is what a truncated backup file or a broken reader does
                    connection.restoreTableSpace(RESTORED_TABLESPACE,
                            new RestoreSourceOfTables(tableOfTheRestoredTableSpace()) {
                                @Override
                                void beforeTheLastStep() {
                                    throw new IllegalStateException(CLIENT_GAVE_UP_MID_RESTORE);
                                }
                            });
                    fail("the first restore was expected to fail in the middle");
                } catch (Exception expected) {
                }

                try {
                    // the client runs the very same restore again, on the very same connection
                    connection.restoreTableSpace(RESTORED_TABLESPACE,
                            new RestoreSourceOfTables(tableOfTheRestoredTableSpace()));
                    fail("the second restore was expected to be refused: the first one left a table behind");
                } catch (HDBException expected) {
                }
            }

            assertTrue("the restore that is still open was disowned by the refusal of the one that came after it,"
                    + " so the connection going away took nothing out of service: the tablespace holds a fragment"
                    + " of the snapshot, its checkpoints are inhibited for good and no boot ever comes to put it"
                    + " right", waitForTableSpaceOutOfService(server, RESTORED_TABLESPACE));
        }
    }

    /**
     * The very first step of a restore, the one that declares it on the commit log, can fail after the marker has
     * already reached the log: the position of a write is answered asynchronously and that answer can fail on its
     * own. The tablespace is then really in the middle of a restore, with its checkpoints inhibited, and the only
     * thing that knows about it is the connection that asked for it. If that connection walks away without saying
     * so, the tablespace stays in service for good: it never takes another checkpoint, its log grows without
     * bound, and nothing ever reboots it.
     */
    @Test
    public void testARestoreThatFailedToOpenIsStillGivenUpOnWhenTheConnectionGoesAway() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new ServerWhoseLogCannotReportRestoreMarkers(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            try (HDBClient client = newImpatientClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    connection.restoreTableSpace(RESTORED_TABLESPACE, new RestoreSourceOfTables());
                    fail("the restore was expected to fail, the log cannot say where it wrote the marker that"
                            + " opens it");
                } catch (HDBException expected) {
                }
            }

            assertTrue("the marker that opens the restore is on the log and the connection that was driving that"
                    + " restore is gone, but the tablespace was left in service: its checkpoints are inhibited"
                    + " for good, so its log grows without bound and no boot ever comes to put it right",
                    waitForTableSpaceOutOfService(server, RESTORED_TABLESPACE));
        }
    }

    /**
     * A restore is a sequence of requests, and the step that declares one table complete names a table the previous
     * requests were supposed to create. Nothing guarantees that they did: HerdDB's own client gives up at the first
     * error, but the server answers whoever speaks the protocol, and a step that reaches the tablespace manager with
     * a name that is not there, or a name that belongs to a system table, must come back as an error of that
     * request. The connection peer turns a {@link HerdDBInternalException} into the reply the client is waiting for;
     * a {@code NullPointerException} or a {@code ClassCastException} reaches no handler at all, no reply is ever
     * written, and the client sits on the request until its own socket timeout expires.
     */
    @Test
    public void testStepOfARestoreThatNamesATableThatIsNotThereIsReportedAsAnError() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
            tableSpaceManager.beginRestore();

            try {
                tableSpaceManager.restoreTableFinished(TABLE_NAME, Collections.emptyList());
                fail("a step of a restore that names a table nobody created was accepted");
            } catch (HerdDBInternalException expected) {
                assertTrue("the failure does not name the table the restore could not finish: " + expected,
                        String.valueOf(expected.getMessage()).contains(TABLE_NAME));
            }

            try {
                // a name that does resolve, to something that is not a restored table at all
                tableSpaceManager.restoreTableFinished("systables", Collections.emptyList());
                fail("a step of a restore that names a system table was accepted");
            } catch (HerdDBInternalException expected) {
                assertTrue("the failure does not name the table the restore could not finish: " + expected,
                        String.valueOf(expected.getMessage()).contains("systables"));
            }
        }
    }

    /**
     * The last step of a restore writes the marker that closes it and then takes the checkpoint that persists the
     * restored content. That checkpoint can be skipped rather than fail, and a skipped checkpoint leaves exactly the
     * tablespace the next boot refuses: a log that says the restore is complete and a storage that knows nothing
     * about it. The client must not be told that the restore succeeded.
     */
    @Test
    public void testRestoreWhoseFinalCheckpointIsSkippedIsNotAcknowledged() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // the activator would take the failed tablespace away while the restore is still running, and the
            // point of the test is the answer the restore gives, not what the activator does afterwards
            server.getManager().setActivatorPauseStatus(true);
            try (HDBClient client = newImpatientClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                try {
                    // the tablespace is taken out of service in the middle of the restore, which is enough for the
                    // checkpoint of the last step to be skipped
                    connection.restoreTableSpace(RESTORED_TABLESPACE, new RestoreSourceOfTables() {
                        @Override
                        void beforeTheLastStep() {
                            tableSpaceManager.abortRestore("the tablespace is taken out of service by this test");
                        }
                    });
                    fail("the restore was acknowledged even though nothing of the restored content reached the"
                            + " storage: the operator has no way to tell that the tablespace will refuse to boot");
                } catch (HDBException expected) {
                }
            } finally {
                server.getManager().setActivatorPauseStatus(false);
            }
        }
    }

    /**
     * Everything the markers of a restore are used for rests on one thing: a tablespace that holds a restore marker
     * exists only because of that restore. That is true of the restore {@link BackupUtils} drives, and it is true
     * only because it creates the tablespace itself, with a {@code CREATE TABLESPACE} of its own. Nothing forces a
     * caller through it: {@code HDBConnection.restoreTableSpace} is public API and streams into whatever tablespace
     * it is pointed at.
     *
     * <p>
     * Aimed at a tablespace that already holds data, it used to open a restore on it, fail on the first table it
     * tried to create, and leave behind a marker claiming that the data of that tablespace never went through its
     * log. Every node that met that marker would then throw away the data it holds and go looking for a copy that
     * does not exist. So the invariant has to hold rather than be assumed: the restore is refused, nothing is
     * written, and a client that aims a restore at the wrong tablespace loses nothing.
     * </p>
     */
    @Test
    public void testRestoreAimedAtATableSpaceThatHoldsDataIsRefusedAndChangesNothing() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        String tableSpaceUUID;
        String nodeId;
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());
            createTableWithRowsInTheRestoredTableSpace(server);

            try (HDBClient client = newImpatientClient();
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    // the same table the tablespace already holds, which is what a restore aimed at the wrong
                    // tablespace looks like: the restore opens itself, fails on the first table it tries to
                    // create, and the client gives up with the marker already on the log
                    connection.restoreTableSpace(RESTORED_TABLESPACE,
                            new RestoreSourceOfTables(tableOfTheRestoredTableSpace()));
                    fail("a restore was accepted on a tablespace that already holds data: the marker it leaves on"
                            + " the log tells every node that reads it that the data of this tablespace did not come"
                            + " from the log, so each of them throws away what it holds and boots nothing");
                } catch (HDBException expected) {
                    assertTrue("the refusal does not say which tables are in the way, so nobody can tell it from"
                            + " any other failure of a restore: " + expected,
                            String.valueOf(expected.getMessage()).contains(TABLE_NAME));
                }
            }

            // the connection that aimed the restore at it is gone. A tablespace that never opened a restore has no
            // restore to be given up on: taking it out of service would be a healthy tablespace losing its service
            // because somebody pointed a restore at it
            assertFalse("a tablespace that refused a restore was taken out of service when the connection that"
                    + " asked for it went away", waitForTableSpaceOutOfService(server, RESTORED_TABLESPACE));
            assertEquals("the data of the tablespace the restore was aimed at did not survive the refusal",
                    ROWS_IN_BACKUP, countRowsOfTheRestoredTable(server));

            nodeId = server.getNodeId();
            tableSpaceUUID = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).getTableSpaceUUID();
        }

        for (LogEntry entry : readLog(baseDir, tableSpaceUUID, nodeId)) {
            assertNotEquals("a refused restore left its marker on the commit log of the tablespace. That marker is"
                    + " all a boot needs to give up on the content this tablespace holds, and nothing ever writes"
                    + " the marker that would close it", LogEntryType.RESTORED_FROM_SNAPSHOT, entry.type);
        }

        // the reboot is where a marker on the log would be met, so it is the only place that can show that there
        // is none
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            server.waitForTableSpaceBoot(RESTORED_TABLESPACE, BOOT_TIMEOUT, true);
            assertEquals("the tablespace a restore was refused on lost its data at the first reboot of the node",
                    ROWS_IN_BACKUP, countRowsOfTheRestoredTable(server));
        }
    }

    /**
     * A restore is driven by a client, one request at a time, and it inhibits the checkpoints of the tablespace it
     * is replacing for as long as it runs: that tablespace persists nothing and, being a leader, never trims its
     * commit log either. A client that dies is noticed when its connection closes, but a client that keeps the
     * connection open and simply stops, a pooled connection or a long lived application, leaves the restore open
     * for good. The blast radius is one brand new tablespace that is serving nobody, so the way out is a plain
     * timeout on a restore that stops making progress, and nothing more.
     */
    @Test
    public void testRestoreThatStopsMakingProgressIsGivenUpOn() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration configuration = newServerConfigurationWithAutoPort(baseDir);
        configuration.set(ServerConfiguration.PROPERTY_RESTORE_MAX_INACTIVITY_TIME, RESTORE_INACTIVITY_TIMEOUT);
        try (Server server = new Server(configuration)) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // the activator is the one that takes a failed tablespace out of service, and the point of the test is
            // what the checkpoint decides before it gets there
            server.getManager().setActivatorPauseStatus(true);
            try {
                TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                tableSpaceManager.beginRestore();

                // a restore of a large snapshot takes as long as it takes, and every request it is made of says so
                Thread.sleep(RESTORE_INACTIVITY_TIMEOUT * 2);
                tableSpaceManager.restoreInProgress();
                assertNull("a checkpoint taken while a restore is running persists a fragment of the snapshot and"
                        + " buries the marker that opened it", tableSpaceManager.checkpoint(false, false, false));
                assertFalse("a restore that is being driven was given up on, so a restore of a snapshot large"
                        + " enough can never complete", tableSpaceManager.isFailed());

                // and one that has stopped is given up on, so that the tablespace stops holding its own
                // checkpoints back
                Thread.sleep(RESTORE_INACTIVITY_TIMEOUT * 2);
                assertNull("a checkpoint was taken in the middle of a restore",
                        tableSpaceManager.checkpoint(false, false, false));
                assertTrue("a restore nobody is driving any more was left open: this tablespace never takes another"
                        + " checkpoint and never trims its commit log again, and the only thing that says so is one"
                        + " log line per checkpoint period", tableSpaceManager.isFailed());
            } finally {
                server.getManager().setActivatorPauseStatus(false);
            }
        }
    }

    /**
     * The guard that keeps a replica from taking the leadership of a tablespace it holds nothing of reads the tail of
     * the commit log to answer, and it reads it afresh every time it is asked. Both facts it answers are properties
     * of the log, which whoever leads the tablespace keeps writing to, while the position the local data of this node
     * sits at stands still precisely when it matters: a node waiting for a restore to be closed takes no checkpoint,
     * so an answer kept against that position would never be asked again.
     */
    @Test
    public void testThePromotionGuardReadsTheLogEveryTimeItIsAsked() throws Exception {
        LogEntry marker = LogEntryFactory.restoredFromSnapshot(
                new RestoredFromSnapshot(RestoredFromSnapshot.Phase.FINISHED, RESTORED_TABLESPACE, TABLESPACE_UUID,
                        POSITION_OF_THE_MARKER));
        AtomicInteger reads = new AtomicInteger();
        AtomicBoolean theRestoreHasHappened = new AtomicBoolean();
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, (from, consumer, fencing) -> {
                    reads.incrementAndGet();
                    if (theRestoreHasHappened.get() && POSITION_OF_THE_MARKER.after(from)) {
                        consumer.accept(POSITION_OF_THE_MARKER, marker);
                    }
                }));
        DataStorageManagerAlignedTo dataStorageManager = new DataStorageManagerAlignedTo();

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                dataStorageManager, logManager, null, null)) {
            manager.start();
            TableSpace tableSpace = aTableSpaceLedBySomebodyElse();

            assertTrue("a replica of a tablespace that was never restored cannot take leadership",
                    manager.isTableSpaceLocallyRecoverable(tableSpace));
            assertEquals("the guard did not read the log at all, so this test proves nothing", 1, reads.get());

            // The leader restores the tablespace while this node is watching it, without this node writing anything
            // of its own: the position its local data sits at does not move, and the answer of the guard has to
            // change all the same
            theRestoreHasHappened.set(true);
            assertFalse("a replica whose data stops below the marker of the restore was allowed to take leadership,"
                    + " out of an answer that was given before the restore happened",
                    manager.isTableSpaceLocallyRecoverable(tableSpace));
            assertEquals("the guard did not read the log again", 2, reads.get());

            // the node downloads the content of the restored tablespace and checkpoints it: its data now sits above
            // the marker, and it is the one node that can lead the tablespace
            dataStorageManager.alignTo(POSITION_AFTER_THE_MARKER);
            assertTrue("a replica that downloaded the restored content and checkpointed it was refused the"
                    + " leadership, so a tablespace restored from a snapshot can never fail over again",
                    manager.isTableSpaceLocallyRecoverable(tableSpace));
            assertEquals("the guard did not read the log again after the data of the node moved", 3, reads.get());
        }
    }

    /**
     * Every failure of a read of the commit log is a {@link RuntimeException}, an unavailable ledger being the
     * everyday one. Refusing the promotion is the right answer to a question that could not be asked at all, but it
     * is not the same answer as a restore, and it must not be reported as one: an operator who has just lost a
     * ledger is sent looking for a restore that never happened. The question also has to keep being asked, because a
     * log that cannot be read now can be read again later.
     */
    @Test
    public void testALogThatCannotBeReadIsNotReportedAsARestore() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogWithSomethingAtTheEndOfRecovery(log, (from, consumer, fencing) -> {
                    reads.incrementAndGet();
                    throw new LogNotAvailableException(LOG_IS_INCOMPLETE);
                }));

        RestoreReports reports = new RestoreReports();
        Logger rootLogger = Logger.getLogger("");
        rootLogger.addHandler(reports);
        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            TableSpace tableSpace = aTableSpaceLedBySomebodyElse();

            assertFalse("a node that cannot read its own commit log was allowed to take leadership",
                    manager.isTableSpaceLocallyRecoverable(tableSpace));
            assertNull("the log of " + RESTORED_TABLESPACE + " could not be read, which says nothing about any"
                    + " restore, and the node reported that the tablespace was restored from a snapshot:\n"
                    + reports.aRestoreWasReported.get(), reports.aRestoreWasReported.get());

            assertFalse(manager.isTableSpaceLocallyRecoverable(tableSpace));
            assertEquals("the log was not read again, so the node would refuse the leadership for good even after"
                    + " the log becomes readable", 2, reads.get());
        } finally {
            rootLogger.removeHandler(reports);
        }
    }

    /**
     * A tablespace manager is out of service when its commit log has failed exactly as much as when it declared
     * itself failed, which is the question {@code isFailed()} answers and the raw field does not. A checkpoint taken
     * in that state reads a last sequence number the log is in no position to give, writes the pages under it, and
     * then, on a leader, asks that same broken log to drop every ledger below it.
     */
    @Test
    public void testATableSpaceWhoseLogHasFailedTakesNoCheckpoint() throws Exception {
        AtomicReference<LogThatCanBeDeclaredFailed> theLog = new AtomicReference<>();
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log -> {
            LogThatCanBeDeclaredFailed failable = new LogThatCanBeDeclaredFailed(log);
            theLog.set(failable);
            return failable;
        });

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE, Collections.singleton(THIS_NODE),
                            THIS_NODE, 1, BOOT_TIMEOUT, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            // something on the log, so that a checkpoint has a position to record and is not skipped for want of
            // anything to write
            manager.executeStatement(new CreateTableStatement(tableOfTheRestoredTableSpace()),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // the activator is the one that takes a tablespace whose log has failed out of service, and the point
            // of the test is what the checkpoint decides before it gets there
            manager.setActivatorPauseStatus(true);
            try {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(RESTORED_TABLESPACE);
                assertNotNull("the checkpoints of " + RESTORED_TABLESPACE + " do not work even before the log"
                        + " fails, this test would prove nothing",
                        tableSpaceManager.checkpoint(false, false, false));
                int ledgersDroppedByAHealthyCheckpoint = theLog.get().dropOldLedgersCalls();

                theLog.get().fail();

                assertTrue("a tablespace manager whose log has failed is out of service, that is what the activator"
                        + " reads to take it away", tableSpaceManager.isFailed());
                assertNull("a tablespace manager whose log has failed took a checkpoint: it recorded a stale"
                        + " position as the state the tablespace is aligned to",
                        tableSpaceManager.checkpoint(false, false, false));
                assertEquals("the checkpoint of a tablespace whose log has failed asked that log to drop the"
                        + " ledgers below a position it is in no position to give",
                        ledgersDroppedByAHealthyCheckpoint, theLog.get().dropOldLedgersCalls());
            } finally {
                manager.setActivatorPauseStatus(false);
            }
        }
    }

    /**
     * The position of a marker is the state of the restore: it is what inhibits the checkpoints and what a boot
     * reads to find out where the content of the tablespace stopped being described by the log. So the markers are
     * written synchronously, and a marker that arrives with a position nobody has waited for has to be refused
     * rather than waited for: asking a deferred write where it wrote blocks the writer and throws when the
     * acknowledgement fails, which is why every other reader of an entry guards that call.
     */
    @Test
    public void testAMarkerThatWasNotLoggedSynchronouslyIsRefused() throws Exception {
        CommitLogManager logManager = logOfTheRestoredTableSpaceIs(log ->
                new LogThatDoesNotWaitForItsWrites(log, LogEntryType.RESTORED_FROM_SNAPSHOT));

        try (DBManager manager = new DBManager(THIS_NODE, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null)) {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(RESTORED_TABLESPACE, Collections.singleton(THIS_NODE),
                            THIS_NODE, 1, BOOT_TIMEOUT, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            writeSomethingOnTheLogOfTheRestoredTableSpace(manager);

            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(RESTORED_TABLESPACE);
            assertNotNull("the checkpoints of " + RESTORED_TABLESPACE + " do not work even before the restore,"
                    + " this test would prove nothing", tableSpaceManager.checkpoint(false, false, false));

            try {
                tableSpaceManager.beginRestore();
                fail("the marker of a restore was accepted from a log that had not said where it wrote it");
            } catch (HerdDBInternalException expected) {
            }

            assertNull("the marker reached the log and this node does not know where: a checkpoint taken now"
                    + " records the content of the tablespace as it is and buries the marker below the position"
                    + " the next boot replays from", tableSpaceManager.checkpoint(false, false, false));
        }
    }

    /**
     * The request that asks for a dump is acknowledged before the dump begins, because a dump takes as long as it
     * takes and the client cannot hold the request open for that. From then on the only thing that reaches the other
     * end is the stream of the dump itself, so a dump that dies in silence leaves whoever asked for it waiting for a
     * chunk that is not coming. The node that asks for one is a replica rebooting on a full download, and it has
     * erased its own copy of the tablespace before asking.
     */
    @Test
    public void testDumpThatCannotBeTakenIsReportedToWhoeverAskedForIt() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            TestUtils.execute(server.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server.getNodeId() + "','wait:"
                            + BOOT_TIMEOUT + "'", Collections.emptyList());

            // a restore is one of the reasons a dump cannot be taken at all: the tablespace holds a fragment of a
            // snapshot and no checkpoint of it may be written, and a dump is a checkpoint
            server.getManager().setActivatorPauseStatus(true);
            try {
                server.getManager().getTableSpaceManager(RESTORED_TABLESPACE).beginRestore();

                CompletableFuture<Throwable> dumpFailure = new CompletableFuture<>();
                try (HDBClient client = newImpatientClient();
                     HDBConnection connection = client.openConnection()) {
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    connection.dumpTableSpace(RESTORED_TABLESPACE, new TableSpaceDumpReceiver() {
                        @Override
                        public void onError(Throwable error) {
                            dumpFailure.complete(error);
                        }

                        @Override
                        public void finish(LogSequenceNumber logSequenceNumber) {
                            dumpFailure.completeExceptionally(
                                    new IllegalStateException("the dump declared itself complete"));
                        }
                    }, 1024, false);

                    Throwable error = dumpFailure.get(DUMP_FAILURE_TIMEOUT, TimeUnit.MILLISECONDS);
                    assertNotNull("the dump failed and the node that asked for it was told nothing", error);
                }
            } finally {
                server.getManager().setActivatorPauseStatus(false);
            }
        }
    }

    private void createTableWithRows(Server server) throws Exception {
        Table table = Table
                .builder()
                .name(TABLE_NAME)
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
        createTableWithRows(server, table, TableSpace.DEFAULT);
    }

    /**
     * The same table, in the tablespace these tests restore into, so that a restore can be aimed at a tablespace
     * that already holds data of its own.
     */
    private void createTableWithRowsInTheRestoredTableSpace(Server server) throws Exception {
        createTableWithRows(server, tableOfTheRestoredTableSpace(), RESTORED_TABLESPACE);
        assertEquals(ROWS_IN_BACKUP, countRowsOfTheRestoredTable(server));
    }

    /**
     * The same table, in the default tablespace of a node that is driven without a server around it, so that a test
     * can ask whether the tablespaces a node can serve are still answering.
     */
    private static void createTableWithRowsInTheDefaultTableSpace(DBManager manager) throws Exception {
        Table table = Table
                .builder()
                .name(TABLE_NAME)
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
        createTableWithRows(manager, table, TableSpace.DEFAULT);
    }

    private void createTableWithRows(Server server, Table table, String tableSpace) throws Exception {
        createTableWithRows(server.getManager(), table, tableSpace);
    }

    private static void createTableWithRows(DBManager manager, Table table, String tableSpace) throws Exception {
        manager.executeStatement(new CreateTableStatement(table),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        for (int i = 0; i < ROWS_IN_BACKUP; i++) {
            manager.executeUpdate(
                    new InsertStatement(tableSpace, TABLE_NAME, RecordSerializer.makeRecord(table, "c", i, "d", 2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }
    }

    /**
     * Puts something on the commit log of the tablespace the tests restore into, so that a checkpoint of it has a
     * position to record and is not skipped for want of anything to write, and leaves that tablespace empty, which
     * is the only tablespace a restore can be opened on.
     */
    private static void writeSomethingOnTheLogOfTheRestoredTableSpace(DBManager manager) throws Exception {
        manager.executeStatement(new CreateTableStatement(tableOfTheRestoredTableSpace()),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        manager.executeStatement(new DropTableStatement(RESTORED_TABLESPACE, TABLE_NAME, false),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
    }

    /**
     * A commit log manager whose logs are ordinary in-memory ones, except for the tablespace the tests restore into,
     * whose log is whatever the test needs it to be.
     */
    private static CommitLogManager logOfTheRestoredTableSpaceIs(Function<CommitLog, CommitLog> wrapper) {
        return commitLogManagerWhere((tableSpaceName, log) ->
                RESTORED_TABLESPACE.equals(tableSpaceName) ? wrapper.apply(log) : log);
    }

    /**
     * A commit log manager whose logs are ordinary in-memory ones, wrapped as the test asks for.
     */
    private static CommitLogManager commitLogManagerWhere(BiFunction<String, CommitLog, CommitLog> logOf) {
        return new CommitLogManager() {

            private final MemoryCommitLogManager plainLogs = new MemoryCommitLogManager();

            @Override
            public CommitLog createCommitLog(
                    String tableSpaceUUID, String tableSpaceName, String localNodeId
            ) throws LogNotAvailableException {
                return logOf.apply(tableSpaceName,
                        plainLogs.createCommitLog(tableSpaceUUID, tableSpaceName, localNodeId));
            }
        };
    }

    /**
     * Dumps the default tablespace and restores it into a brand new tablespace led by the same node.
     */
    private void restoreBackupOfTheDefaultTableSpace(Server server) throws Exception {
        try (HDBClient client = newClient();
             HDBConnection connection = client.openConnection()) {
            client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
            ByteArrayOutputStream backup = new ByteArrayOutputStream();
            BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, backup, new ProgressListener() {
            });
            BackupUtils.restoreTableSpace(RESTORED_TABLESPACE, server.getNodeId(), connection,
                    new ByteArrayInputStream(backup.toByteArray()), new ProgressListener() {
                    });
        }
        assertEquals(ROWS_IN_BACKUP, countRowsOfTheRestoredTable(server));
    }

    private HDBClient newClient() throws Exception {
        return new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
    }

    /**
     * A client that gives up on a request quickly. It is used where the point of the test is that the server answers
     * at all: waiting five minutes for the default timeout would only make the failure slower to see.
     */
    private HDBClient newImpatientClient() throws Exception {
        ClientConfiguration configuration = new ClientConfiguration(folder.newFolder().toPath());
        configuration.set(ClientConfiguration.PROPERTY_TIMEOUT, IMPATIENT_CLIENT_TIMEOUT);
        return new HDBClient(configuration);
    }

    /**
     * A client that holds one connection per server, so that two requests sent one after the other reach the same
     * connection on the server side. A client connection is a pool of them by default and picks one at random, so
     * without this a test cannot say which server side connection anything lands on.
     */
    private HDBClient newClientWithOneConnectionPerServer() throws Exception {
        ClientConfiguration configuration = new ClientConfiguration(folder.newFolder().toPath());
        configuration.set(ClientConfiguration.PROPERTY_TIMEOUT, IMPATIENT_CLIENT_TIMEOUT);
        configuration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
        return new HDBClient(configuration);
    }

    /**
     * Waits for a tablespace to be out of service on a node, either because its manager reports itself as failed or
     * because the activator has already taken it away.
     */
    private static boolean waitForTableSpaceOutOfService(Server server, String tableSpace) throws Exception {
        for (int i = 0; i < FAILED_BOOT_TIMEOUT / 100; i++) {
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(tableSpace);
            if (tableSpaceManager == null || tableSpaceManager.isFailed()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    /**
     * Waits for the leadership of a tablespace to move onto a given node, which is what a takeover does to the
     * metadata every node of the cluster reads.
     */
    private static boolean waitForLeadershipToMove(
            DBManager manager, String tableSpace, String newLeader, int timeout
    ) throws Exception {
        for (int i = 0; i < timeout / 100; i++) {
            TableSpace metadata = manager.getMetadataStorageManager().describeTableSpace(tableSpace);
            if (metadata != null && newLeader.equals(metadata.leaderId)) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    /**
     * A tablespace this node cannot serve is still a tablespace of the cluster. Nothing a node reads of its own log
     * tells a restore that was interrupted from one that completed somewhere else, so taking the tablespace out of
     * the metadata every node reads would take the restored content away from whichever node is holding it.
     */
    private static void assertTableSpaceIsStillRegistered(DBManager manager) throws Exception {
        assertNotNull("tablespace " + RESTORED_TABLESPACE + " was removed from the metadata every node of the"
                + " cluster reads, so every node lost it, the ones that do hold the restored content included",
                manager.getMetadataStorageManager().describeTableSpace(RESTORED_TABLESPACE));
    }

    /**
     * An application must not be able to reach a tablespace whose content is not on this node. Booting it empty would
     * be worse than not booting it at all: an empty tablespace looks exactly like one that has just been created, so
     * an application would find no tables, create its own and build on a foundation that was supposed to hold the
     * restored data.
     */
    private static void assertTableSpaceIsNotServed(Server server) throws Exception {
        try (DataScanner ignored = TestUtils.scanWithDefaultTableSpace(server.getManager(), RESTORED_TABLESPACE,
                "SELECT * FROM " + RESTORED_TABLESPACE + "." + TABLE_NAME, Collections.emptyList())) {
            fail("a query aimed at " + RESTORED_TABLESPACE + " was answered, so an application can reach a"
                    + " tablespace this node holds no content of");
        } catch (StatementExecutionException expected) {
        }
    }

    /**
     * The definition of the restored table, shaped the way the connection peer hands it to the tablespace manager
     * while it serves a restore: bound to the tablespace it is being restored into.
     */
    private static Table tableOfTheRestoredTableSpace() {
        return Table
                .builder()
                .tablespace(RESTORED_TABLESPACE)
                .name(TABLE_NAME)
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
    }

    /**
     * The beginning of a backup file, and nothing else.
     */
    private static byte[] truncatedBackup() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream eos = new ExtendedDataOutputStream(out)) {
            eos.writeUTF(BackupFileConstants.ENTRY_TYPE_START);
        }
        return out.toByteArray();
    }

    /**
     * Counts the rows of the table of the default tablespace, the one a node keeps serving while another tablespace
     * of the same node cannot be served at all.
     */
    private static int countRowsOfTheDefaultTable(Server server) throws Exception {
        return countRowsOfTheDefaultTable(server.getManager());
    }

    private static int countRowsOfTheDefaultTable(DBManager manager) throws Exception {
        try (DataScanner scan = TestUtils.scan(manager,
                "SELECT * FROM " + TABLE_NAME, Collections.emptyList())) {
            return scan.consume().size();
        }
    }

    private static int countRowsOfTheRestoredTable(Server server) throws Exception {
        // the restored tablespace is not the default one of this node, so the query has to be aimed at it
        try (DataScanner scan = TestUtils.scanWithDefaultTableSpace(server.getManager(), RESTORED_TABLESPACE,
                "SELECT * FROM " + RESTORED_TABLESPACE + "." + TABLE_NAME, Collections.emptyList())) {
            return scan.consume().size();
        }
    }

    /**
     * Reads back the whole commit log of a tablespace, from the files the server left behind.
     */
    private static List<LogEntry> readLog(Path baseDir, String tableSpaceUUID, String nodeId) throws Exception {
        List<LogEntry> entries = new ArrayList<>();
        Path logDirectory = baseDir.resolve(ServerConfiguration.PROPERTY_LOGDIR_DEFAULT);
        try (FileCommitLogManager logManager = new FileCommitLogManager(logDirectory)) {
            logManager.start();
            try (CommitLog log = logManager.createCommitLog(tableSpaceUUID, RESTORED_TABLESPACE, nodeId)) {
                log.recovery(LogSequenceNumber.START_OF_TIME, (position, entry) -> entries.add(entry), false);
            }
        }
        return entries;
    }

    private static String stackTraceOf(Throwable error) {
        StringWriter writer = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(writer)) {
            error.printStackTrace(printWriter);
        }
        return writer.toString();
    }

    /**
     * What a {@link LogWithSomethingAtTheEndOfRecovery} does once the log itself has been read back: hand over one
     * more entry, or refuse to be replayed at all.
     */
    private interface EndOfRecovery {

        /**
         * @param from the position the log is being replayed from, so that an entry can be handed over only to a
         * reader that has not gone past it yet, the way a real log skips whatever the reader has already applied
         */
        void accept(
                LogSequenceNumber from, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing
        ) throws LogNotAvailableException;
    }

    /**
     * Where the entries these logs add of their own sit. The exact value does not matter, it only has to look like a
     * position at the end of a log.
     */
    private static final LogSequenceNumber TAIL_OF_THE_LOG = new LogSequenceNumber(1, 0);

    /**
     * One more entry that only a reader that fences the log gets to see. It stands for what a ledger the previous
     * leader was writing to really does: the entries it had not acknowledged yet become readable only to the reader
     * that closes the ledger, which is the pass that takes leadership.
     */
    private static EndOfRecovery anEntryOnlyAFencedReaderSees(LogEntry entry) {
        return (from, consumer, fencing) -> {
            if (fencing) {
                consumer.accept(TAIL_OF_THE_LOG, entry);
            }
        };
    }

    /**
     * One more entry that every reader of the log sees, wherever it is replayed from.
     */
    private static EndOfRecovery anEntryEveryReaderSees(LogEntry entry) {
        return (from, consumer, fencing) -> consumer.accept(TAIL_OF_THE_LOG, entry);
    }

    /**
     * One more entry that sits at a given position of the log, and that a reader sees only if it is replaying the log
     * from below that position. This is what every real log does with the position it is asked to start from: a node
     * whose data is already aligned above an entry never reads that entry again.
     */
    private static EndOfRecovery anEntryAt(LogSequenceNumber position, LogEntry entry) {
        return (from, consumer, fencing) -> {
            if (position.after(from)) {
                consumer.accept(position, entry);
            }
        };
    }

    /**
     * A commit log whose recovery does something more than reading back what was written, so that a test can put a
     * particular entry, or a particular failure, exactly where the boot of a tablespace meets it.
     */
    private static final class LogWithSomethingAtTheEndOfRecovery extends DelegatingCommitLog {

        private final EndOfRecovery endOfRecovery;

        /**
         * Whether this log holds enough to be replayed at all, the answer a real log gives out of the ledgers it
         * still has.
         */
        private final boolean recoveryAvailable;

        LogWithSomethingAtTheEndOfRecovery(CommitLog log, EndOfRecovery endOfRecovery) {
            this(log, endOfRecovery, true);
        }

        LogWithSomethingAtTheEndOfRecovery(CommitLog log, EndOfRecovery endOfRecovery, boolean recoveryAvailable) {
            super(log);
            this.endOfRecovery = endOfRecovery;
            this.recoveryAvailable = recoveryAvailable;
        }

        @Override
        public boolean isRecoveryAvailable(LogSequenceNumber snapshotSequenceNumber) {
            return recoveryAvailable;
        }

        @Override
        public void recovery(
                LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer,
                boolean fencing
        ) throws LogNotAvailableException {
            super.recovery(snapshotSequenceNumber, consumer, fencing);
            endOfRecovery.accept(snapshotSequenceNumber, consumer, fencing);
        }
    }

    /**
     * A commit log that is a real one in every way, so that a test only has to say what it wants that log to do
     * differently.
     */
    private abstract static class DelegatingCommitLog extends CommitLog {

        protected final CommitLog log;

        DelegatingCommitLog(CommitLog log) {
            this.log = log;
        }

        @Override
        public CommitLogResult log(LogEntry entry, boolean synch) throws LogNotAvailableException {
            return log.log(entry, synch);
        }

        @Override
        public void recovery(
                LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer,
                boolean fencing
        ) throws LogNotAvailableException {
            log.recovery(snapshotSequenceNumber, consumer, fencing);
        }

        @Override
        public LogSequenceNumber getLastSequenceNumber() {
            return log.getLastSequenceNumber();
        }

        @Override
        public void startWriting(int expectedReplicaCount) throws LogNotAvailableException {
            log.startWriting(expectedReplicaCount);
        }

        @Override
        public void clear() throws LogNotAvailableException {
            log.clear();
        }

        @Override
        public void close() throws LogNotAvailableException {
            log.close();
        }

        @Override
        public boolean isFailed() {
            return log.isFailed();
        }

        @Override
        public boolean isClosed() {
            return log.isClosed();
        }

        @Override
        public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
            log.dropOldLedgers(lastCheckPointSequenceNumber);
        }
    }

    /**
     * A commit log that can be told to declare itself failed, the way a real one does when the storage behind it
     * stops answering, and that counts the ledgers it is asked to drop.
     */
    private static final class LogThatCanBeDeclaredFailed extends DelegatingCommitLog {

        private volatile boolean failed;

        private final AtomicInteger dropOldLedgersCalls = new AtomicInteger();

        LogThatCanBeDeclaredFailed(CommitLog log) {
            super(log);
        }

        void fail() {
            failed = true;
        }

        int dropOldLedgersCalls() {
            return dropOldLedgersCalls.get();
        }

        @Override
        public boolean isFailed() {
            return failed || super.isFailed();
        }

        @Override
        public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
            dropOldLedgersCalls.incrementAndGet();
            super.dropOldLedgers(lastCheckPointSequenceNumber);
        }
    }

    /**
     * A commit log that accepts an entry and hands back a position nobody has waited for, which is what every log
     * backed by a replicated storage does when it is not asked for a synchronous write.
     */
    private static final class LogThatDoesNotWaitForItsWrites extends DelegatingCommitLog {

        private final short typeItDoesNotWaitFor;

        LogThatDoesNotWaitForItsWrites(CommitLog log, short typeItDoesNotWaitFor) {
            super(log);
            this.typeItDoesNotWaitFor = typeItDoesNotWaitFor;
        }

        @Override
        public CommitLogResult log(LogEntry entry, boolean synch) throws LogNotAvailableException {
            CommitLogResult result = super.log(entry, synch);
            if (entry.type != typeItDoesNotWaitFor) {
                return result;
            }
            return new CommitLogResult(result.logSequenceNumber, true, false);
        }
    }

    /**
     * A server whose commit log, for the tablespace these tests restore into, accepts the markers of a restore and
     * then cannot say where it wrote them. It is the only way to reach, through a real client connection, a restore
     * that fails after it has already declared itself on the log.
     */
    private static final class ServerWhoseLogCannotReportRestoreMarkers extends Server {

        ServerWhoseLogCannotReportRestoreMarkers(ServerConfiguration configuration) {
            super(configuration);
        }

        @Override
        protected CommitLogManager buildCommitLogManager() {
            CommitLogManager plainLogs = super.buildCommitLogManager();
            return new CommitLogManager() {

                @Override
                public CommitLog createCommitLog(
                        String tableSpaceUUID, String tableSpaceName, String localNodeId
                ) throws LogNotAvailableException {
                    CommitLog log = plainLogs.createCommitLog(tableSpaceUUID, tableSpaceName, localNodeId);
                    if (!RESTORED_TABLESPACE.equals(tableSpaceName)) {
                        return log;
                    }
                    return new LogThatCannotReportWhereItWrote(log, LogEntryType.RESTORED_FROM_SNAPSHOT);
                }

                @Override
                public void start() throws LogNotAvailableException {
                    plainLogs.start();
                }

                @Override
                public void close() {
                    plainLogs.close();
                }
            };
        }
    }

    /**
     * A commit log that accepts an entry and then cannot say where it wrote it. A log backed by a replicated storage
     * answers the position of a write asynchronously, and that answer can fail on its own, well after the entry has
     * been handed over.
     */
    private static final class LogThatCannotReportWhereItWrote extends DelegatingCommitLog {

        private final short typeItCannotReport;

        LogThatCannotReportWhereItWrote(CommitLog log, short typeItCannotReport) {
            super(log);
            this.typeItCannotReport = typeItCannotReport;
        }

        @Override
        public CommitLogResult log(LogEntry entry, boolean synch) throws LogNotAvailableException {
            CommitLogResult result = super.log(entry, synch);
            if (entry.type != typeItCannotReport) {
                return result;
            }
            CompletableFuture<LogSequenceNumber> lost = new CompletableFuture<>();
            lost.completeExceptionally(new LogNotAvailableException("the position of the entry is lost"));
            return new CommitLogResult(lost, false, true);
        }
    }

    /**
     * A restore that carries the tables it is given and no rows at all. With no table it is the shortest sequence of
     * requests a client can send that still reaches the last step of a restore; with one it is the shortest one that
     * leaves the tablespace holding something, which is what the checkpoint of that last step has to write.
     *
     * <p>
     * The tables come from the restore and are not created beforehand, because a restore is refused on a tablespace
     * that already holds tables of its own: a tablespace that has any is not the empty tablespace a restore creates
     * for itself, and the markers of a restore mean nothing on any other.
     * </p>
     */
    private static class RestoreSourceOfTables extends TableSpaceRestoreSource {

        private final Iterator<Table> tables;

        private boolean started;

        RestoreSourceOfTables(Table... tables) {
            this.tables = Arrays.asList(tables).iterator();
        }

        @Override
        public String nextEntryType() {
            if (!started) {
                started = true;
                return BackupFileConstants.ENTRY_TYPE_START;
            }
            if (tables.hasNext()) {
                return BackupFileConstants.ENTRY_TYPE_TABLE;
            }
            beforeTheLastStep();
            return BackupFileConstants.ENTRY_TYPE_END;
        }

        @Override
        public DumpedTableMetadata nextTable() {
            return new DumpedTableMetadata(tables.next(), TAIL_OF_THE_LOG, Collections.emptyList());
        }

        /**
         * Runs on the client side, with the restore already open on the server and just before the client tells it
         * to close itself. It is the only moment a test can reach in between two steps of a restore driven by a
         * real connection.
         */
        void beforeTheLastStep() {
        }
    }

    /**
     * Watches the logs for a node telling an operator that a tablespace holds the marker of a restore.
     */
    private static final class RestoreReports extends Handler {

        /**
         * The text a node uses to report that it found a restore marker, and nothing else: a message that merely
         * says that it does not know whether there was a restore is the opposite of a report.
         */
        private static final String A_RESTORE_WAS_FOUND = "holds the marker of a restore";

        final AtomicReference<String> aRestoreWasReported = new AtomicReference<>();

        @Override
        public void publish(LogRecord record) {
            // the raw pattern, not the formatted message: everything that varies between one tablespace and
            // the next is a parameter, and what this looks for is the claim itself
            String message = record.getMessage();
            if (message != null && message.contains(A_RESTORE_WAS_FOUND)) {
                aRestoreWasReported.compareAndSet(null, message);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    /**
     * Watches the logs for the ways in which the boot of a tablespace whose restore did not complete can fail.
     */
    private static final class BootFailures extends Handler {

        /**
         * The log itself refused to be replayed, for a reason of its own.
         */
        final AtomicReference<Throwable> logCannotBeReplayed = new AtomicReference<>();

        /**
         * Whatever went wrong, a restore was named as the thing that has to be run again.
         */
        final AtomicReference<Throwable> restoreBlamed = new AtomicReference<>();

        @Override
        public void publish(LogRecord record) {
            for (Throwable cursor = record.getThrown(); cursor != null; cursor = cursor.getCause()) {
                String message = cursor.getMessage();
                if (message == null || !message.contains(RESTORED_TABLESPACE)) {
                    continue;
                }
                if (message.contains(LOG_IS_INCOMPLETE)) {
                    logCannotBeReplayed.compareAndSet(null, cursor);
                }
                if (message.contains("run again")) {
                    restoreBlamed.compareAndSet(null, cursor);
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
