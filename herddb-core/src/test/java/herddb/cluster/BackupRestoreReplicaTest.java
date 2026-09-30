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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.codec.RecordSerializer;
import herddb.core.TableManager;
import herddb.core.TableSpaceManager;
import herddb.core.TestUtils;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ZKTestEnv;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A tablespace created by restoring a backup must be able to receive a replica.
 *
 * <p>
 * {@code BackupUtils.restoreTableSpace} creates the tablespace and then streams the dump into it. On the server side
 * the tables are created by {@code TableSpaceManager.beginRestoreTable}, which calls {@code bootTable} directly instead
 * of going through the usual DDL path, so no {@code CREATE_TABLE} entry is ever written to the commit log. The leader
 * survives because it boots from its own local checkpoint, but the commit log of the restored tablespace begins above
 * the point where the tables came into existence.
 * </p>
 * <p>
 * As soon as a second node is added to the replica list, that node has no local data, so its checkpoint sequence number
 * is {@code START_OF_TIME} and it replays the commit log from the very beginning. The restore declares itself on that
 * log, so the joining node reads at the first entry that the content of the tablespace was materialised from a
 * snapshot and asks for a full download of the data of the tablespace from the leader, which is a recovery path the
 * tablespace manager already knows how to take. Should a log carry no marker, the first data change entry the node
 * meets refers to a table it never created: that used to dereference a null table manager inside
 * {@code TableSpaceManager.apply}, killing the boot of the tablespace with a bare NullPointerException, and it now
 * asks for the very same download.
 * </p>
 * <p>
 * Both tests also check that the node is still able to describe the tablespace once the download is over: the system
 * tables are created while the tablespace manager boots and they are not part of the downloaded data, so they have to
 * survive the reset the download does on the local content of the tablespace.
 * </p>
 * <p>
 * A node that is already following the tablespace when the restore happens is in the same position for a different
 * reason: it holds a content the leader has just thrown away, and the restore puts none of the replacement on the log.
 * It reads the marker while it is tailing the leader instead of while it is booting, and the only thing it can do with
 * it is take itself out of service and boot again, which is the download above.
 * </p>
 * <p>
 * A replica that has not got the restored content is also the one node that must not take the tablespace over when
 * the leader dies. Downloading the content is the only way it can serve the tablespace, and a leader has nobody to
 * download from, so a node that took it over would refuse to boot it and nobody would serve it at all.
 * </p>
 */
public class BackupRestoreReplicaTest {

    private static final String RESTORED_TABLESPACE = "restoredts";

    private static final String TABLE_NAME = "t1";

    /**
     * Table the tablespace holds before the restore replaces its content.
     */
    private static final String TABLE_BEFORE_RESTORE = "before_restore";

    /**
     * How long we allow the joining replica to boot the restored tablespace.
     */
    private static final int BOOT_TIMEOUT = 60000;

    /**
     * Number of rows contained in the backup.
     */
    private static final int ROWS_IN_BACKUP = 4;

    /**
     * First key of the rows written after the restore, i.e. of the rows that do reach the commit log.
     */
    private static final int FIRST_KEY_AFTER_RESTORE = 100;

    /**
     * Number of rows written after the restore.
     */
    private static final int ROWS_AFTER_RESTORE = 10;

    /**
     * How long a replica that must not apply anything else is watched. It only has to be long enough for the
     * follower thread to have read the entry the leader wrote, which takes one round of tailing the log.
     */
    private static final int TIME_GIVEN_TO_THE_FOLLOWER = 10000;

    /**
     * How long the activator of a node is given to notice that it has been asked to stop working.
     */
    private static final int TIME_THE_ACTIVATOR_NEEDS_TO_PAUSE = 3000;

    /**
     * How long the leader of a tablespace may stay silent before the nodes that replicate it try to take it over.
     * This is as short as a tablespace is allowed to have it.
     */
    private static final long LEADER_INACTIVITY_TIMEOUT = 5000;

    /**
     * How long a node that must not take a tablespace over is watched: the time the leader has to be silent for
     * before the takeover is even considered, and then room for several passes of the activator.
     */
    private static final int TIME_GIVEN_TO_THE_TAKEOVER = (int) LEADER_INACTIVITY_TIMEOUT + 15000;

    /**
     * How long the leader waits, with nothing to write, before it tells the bookies that everything it has written
     * can be read back. A leader does this on its own, this only makes it quick enough for a test.
     */
    private static final long TIME_THE_LEADER_STAYS_IDLE = 1000;

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

    /**
     * The joining replica cannot rebuild the tablespace out of the commit log. It must recover by downloading the
     * whole content of the tablespace from the leader.
     */
    @Test
    public void testAddReplicaToRestoredTableSpace() throws Exception {
        RecoveryEvents events = restoreBackupAndAddReplica(false);
        assertNotNull("the joining replica was expected to ask for a full download of the data of the tablespace,"
                + " because the commit log of a restored tablespace does not contain the creation of its tables",
                events.fullRecoveryNeeded.get());
    }

    /**
     * The restore declares itself on the commit log, so the joining replica knows that replaying that log is pointless
     * as soon as it reads the marker: it never gets to the point where it meets a change of a table it does not know.
     */
    @Test
    public void testJoiningReplicaStopsAtTheRestoreMarker() throws Exception {
        RecoveryEvents events = restoreBackupAndAddReplica(false);
        assertNotNull("the joining replica was expected to ask for a full download of the data of the tablespace"
                + " because of the marker the restore leaves on the commit log", events.restoredFromSnapshot.get());
        assertNull("the joining replica was expected to give up at the marker, which is the first entry of the log,"
                + " and never to reach a data change for a table that was never created",
                events.unknownTableDuringRecovery.get());
    }

    /**
     * Asking for a full download of the data of the tablespace is a recoverable condition, not a broken commit log. A
     * commit log that treats it as a failure stays failed for good, so the tablespace manager is thrown away right
     * after the download succeeded and the whole boot has to be done again from scratch.
     */
    private static void assertCommitLogIsNotConsideredFailed(RecoveryEvents events) {
        Throwable failure = events.recoverableConditionReportedAsLogFailure.get();
        if (failure != null) {
            fail("the commit log treated the request for a full download of the data of the tablespace as a failure"
                    + " of the log itself, so the tablespace manager will be dropped even if the download"
                    + " succeeds\n" + stackTraceOf(failure));
        }
    }

    /**
     * Control case: with {@code server.boot.force.download.snapshot=true} the joining node downloads a full snapshot
     * from the leader before replaying anything, so it never meets the unknown table. This is the workaround that was
     * available to operators before the fix, and it pins the other test to the defect rather than to an unrelated setup
     * problem.
     */
    @Test
    public void testAddReplicaToRestoredTableSpaceForcingSnapshotDownload() throws Exception {
        RecoveryEvents events = restoreBackupAndAddReplica(true);
        assertNull("the joining replica downloads the snapshot upfront, so it must never meet an unknown table",
                events.fullRecoveryNeeded.get());
    }

    /**
     * A replica that is already following the tablespace when the restore happens is in a worse position than one that
     * joins afterwards: it holds a content that the leader has just thrown away, and the restore writes none of the
     * replacement to the log, so there is nothing for the follower thread to apply. The marker that opens the restore
     * is the only thing that reaches it, and it has to act on that alone. Nothing else is going to arrive: a restored
     * tablespace can sit untouched for days, and until somebody writes to one of the restored tables this replica
     * would keep serving the content the restore replaced, and would answer queries with it.
     *
     * <p>
     * The restore is driven through the very calls the connection peer makes while it serves a client, because what
     * this test is about happens on the other node, not on the wire. The restored table has a name of its own, so it
     * cannot be confused with the content the tablespace had before, and so that no write ever needs to be issued
     * after the restore: whatever the replica ends up holding, it got there because of the marker.
     * </p>
     */
    @Test
    public void testReplicaAlreadyFollowingWhenTheTableSpaceIsRestored() throws Exception {

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
             Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            server_2.start();

            TestUtils.execute(server_1.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server_1.getNodeId()
                            + "','wait:" + BOOT_TIMEOUT + "'", Collections.emptyList());
            TestUtils.execute(server_1.getManager(),
                    "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                            + ",'leader:" + server_1.getNodeId() + "'"
                            + ",'replica:" + server_1.getNodeId() + "," + server_2.getNodeId() + "'",
                    Collections.emptyList());

            makeTheReplicaFollow(server_1, server_2);

            TableSpaceManager leader = server_1.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
            assertTrue("the tablespace is not led by the node that is about to restore it", leader.isLeader());

            // the restore, one step at a time. None of the data below goes through the commit log
            Table restoredTable = tableOf(TABLE_NAME);
            leader.beginRestore();
            leader.beginRestoreTable(restoredTable.serialize(), new LogSequenceNumber(1, 1));
            List<Record> restoredRows = new ArrayList<>();
            for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                restoredRows.add(RecordSerializer.makeRecord(restoredTable, "c", i, "d", 2));
            }
            ((TableManager) leader.getTableManager(TABLE_NAME)).writeFromDump(restoredRows);
            leader.restoreTableFinished(TABLE_NAME, Collections.emptyList());
            leader.restoreFinished();

            // from here on nothing at all is written to the tablespace
            assertTrue("the replica never picked up the table the restore brought in: the restore told it on the"
                    + " commit log that the content of the tablespace had been replaced, and it went on serving"
                    + " the content that is not there any more", server_2.getManager()
                    .waitForTable(RESTORED_TABLESPACE, TABLE_NAME, BOOT_TIMEOUT, false));
            waitForKeyOnReplica(server_2, ROWS_IN_BACKUP - 1);
            for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                assertTrue("row c=" + i + " of the restored table is missing on the replica that was already"
                        + " following the tablespace", existsOnReplica(server_2, i));
            }
        }
    }

    /**
     * The marker that says the leader has replaced the content of the tablespace takes the replica out of service,
     * but taking a tablespace manager out of service does not stop the thread that is tailing the leader: the
     * activator does that, on its own schedule, and until it gets there the follower thread goes on applying
     * whatever the leader writes next.
     *
     * <p>
     * That is not a harmless delay. When the restored snapshot has the same schema as the content it replaced, which
     * is exactly what restoring a backup of the tablespace itself produces, every table name in those entries still
     * resolves on this node, so nothing fails: the changes the leader made to the restored rows are applied on top
     * of the rows the restore threw away, and whoever reads this replica in the meantime is served a mixture of the
     * two contents. The follower has to stop at the marker.
     * </p>
     *
     * <p>
     * The activator of the replica is held back for the whole test, so that what is observed is the follower thread
     * alone and not a race with the reboot the activator would eventually trigger.
     * </p>
     */
    @Test
    public void testFollowerStopsApplyingEntriesAtTheRestoreMarker() throws Exception {

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
             Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            server_2.start();

            TestUtils.execute(server_1.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server_1.getNodeId()
                            + "','wait:" + BOOT_TIMEOUT + "'", Collections.emptyList());
            TestUtils.execute(server_1.getManager(),
                    "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                            + ",'leader:" + server_1.getNodeId() + "'"
                            + ",'replica:" + server_1.getNodeId() + "," + server_2.getNodeId() + "'",
                    Collections.emptyList());

            makeTheReplicaFollow(server_1, server_2);

            // The activator of the replica would reboot the tablespace as soon as it notices that it is out of
            // service, and the point of this test is what the follower thread does before that happens. The
            // activator only looks at the request at the top of its loop, after the pass it is running and after
            // its own poll for work times out, so it needs a moment to get there
            server_2.getManager().setActivatorPauseStatus(true);
            Thread.sleep(TIME_THE_ACTIVATOR_NEEDS_TO_PAUSE);
            FollowerErrors followerErrors = new FollowerErrors();
            Logger rootLogger = Logger.getLogger("");
            rootLogger.addHandler(followerErrors);
            try {
                TableSpaceManager leader = server_1.getManager().getTableSpaceManager(RESTORED_TABLESPACE);

                // the restore, one step at a time. None of the data below goes through the commit log
                Table restoredTable = tableOf(TABLE_NAME);
                leader.beginRestore();
                leader.beginRestoreTable(restoredTable.serialize(), new LogSequenceNumber(1, 1));
                List<Record> restoredRows = new ArrayList<>();
                for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                    restoredRows.add(RecordSerializer.makeRecord(restoredTable, "c", i, "d", 2));
                }
                ((TableManager) leader.getTableManager(TABLE_NAME)).writeFromDump(restoredRows);
                leader.restoreTableFinished(TABLE_NAME, Collections.emptyList());
                leader.restoreFinished();

                // Ordinary activity on the leader, right after the restore: this does reach the commit log, and it
                // names a table that only the restore ever created, so the replica has nothing to apply it to.
                //
                // The entries are written now, before the replica has had the time to read the marker, so that they
                // sit on the log next to it: a follower reads the log in batches, so this is the case where entries
                // that must not be applied are already in the hands of the node that must not apply them
                for (int i = FIRST_KEY_AFTER_RESTORE; i < FIRST_KEY_AFTER_RESTORE + ROWS_AFTER_RESTORE; i++) {
                    server_1.getManager().executeUpdate(
                            new InsertStatement(RESTORED_TABLESPACE, TABLE_NAME,
                                    RecordSerializer.makeRecord(restoredTable, "c", i, "d", 3)),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                }

                TableSpaceManager replica = server_2.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                assertNotNull("no tablespace manager on the replica", replica);
                assertTrue("the replica did not take itself out of service when the leader told it, on the commit"
                        + " log, that the content of the tablespace had been replaced",
                        waitForFailedTableSpace(replica));

                Thread.sleep(TIME_GIVEN_TO_THE_FOLLOWER);
                Throwable followerError = followerErrors.error.get();
                if (followerError != null) {
                    fail("the follower thread of the replica went on handing entries over after the marker had"
                            + " taken the tablespace out of service, and died on one of them. Stopping at the"
                            + " marker is the difference between a replica that is waiting to be rebooted and one"
                            + " that is reporting failures nobody has to look into\n" + stackTraceOf(followerError));
                }
                assertNull("the replica materialised the table the restore created out of the entries the leader"
                        + " wrote after the marker, so it is serving a content that came from nowhere",
                        replica.getTableManager(TABLE_NAME));
            } finally {
                rootLogger.removeHandler(followerErrors);
                server_2.getManager().setActivatorPauseStatus(false);
            }
        }
    }

    /**
     * A replica of a tablespace whose content has been replaced by a restore must not take the tablespace over when
     * the leader dies. It holds none of the restored content and it cannot rebuild it out of the log: the first thing
     * it would do as the new leader is meet the marker of the restore and refuse to boot the tablespace, because
     * downloading the content is the only way to get it and a leader has nobody to download from.
     *
     * <p>
     * Taking the leadership therefore turns a tablespace that is merely leaderless into one that nobody serves, and
     * it takes the leadership away from a replica that did download the whole content and could serve it. The
     * tablespace is left without a leader until the old one comes back or an operator steps in, which is the price of
     * leaving the leadership to a node that can take it.
     * </p>
     *
     * <p>
     * The leader dies with the restore still open, because that is the only way a replica that never got the content
     * is still healthy and eligible when the leader goes: a replica that reads the marker closing the restore takes
     * itself out of service straight away and is booted again, and a boot that cannot reach the leader ends there,
     * with no tablespace manager and nothing to promote. What the replica reads is the same marker either way.
     * </p>
     */
    @Test
    public void testReplicaOfARestoredTableSpaceDoesNotTakeItOverWhenTheLeaderDies() throws Exception {

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        // The marker that opens the restore is the last entry the leader writes, and a reader of a ledger gets no
        // further than the last entry the bookies have confirmed: until the leader says that the marker is one of
        // them, nobody reads it. A leader says so on its own once it has been idle for a while, and this only makes
        // that wait short enough for a test
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, TIME_THE_LEADER_STAYS_IDLE);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                TestUtils.execute(server_1.getManager(),
                        "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server_1.getNodeId()
                                + "','wait:" + BOOT_TIMEOUT + "'", Collections.emptyList());
                TestUtils.execute(server_1.getManager(),
                        "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                                + ",'leader:" + server_1.getNodeId() + "'"
                                + ",'replica:" + server_1.getNodeId() + "," + server_2.getNodeId() + "'"
                                + ",'maxLeaderInactivityTime:" + LEADER_INACTIVITY_TIMEOUT + "'",
                        Collections.emptyList());

                makeTheReplicaFollow(server_1, server_2);

                TableSpaceManager leader = server_1.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                assertTrue("the tablespace is not led by the node that is about to restore it", leader.isLeader());

                // the restore begins: the marker reaches the log, and none of the content ever will
                leader.beginRestore();

                TableSpaceManager replica = server_2.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                assertNotNull("no tablespace manager on the replica", replica);
                assertTrue("the replica never read the marker of the restore, so it is not in the position this"
                        + " test is about", waitForTheRestoreToReachTheReplica(replica));
                assertFalse("the replica took itself out of service before the leader died", replica.isFailed());

                // the leader dies with the restore open and its content nowhere
                server_1.close();

                assertFalse("the replica took leadership of a tablespace it holds no content of and then removed"
                        + " it from the metadata of the whole cluster, so every other node lost it too",
                        waitForTableSpaceToBeRemoved(server_2, RESTORED_TABLESPACE));
                TableSpace tableSpace = server_2.getManager().getMetadataStorageManager()
                        .describeTableSpace(RESTORED_TABLESPACE);
                assertNotNull("tablespace " + RESTORED_TABLESPACE + " is gone", tableSpace);
                assertEquals("the replica took leadership of a tablespace whose content it never got",
                        server_1.getNodeId(), tableSpace.leaderId);
                TableSpaceManager stillTheReplica =
                        server_2.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                assertFalse("the replica is leading a tablespace whose content it never got",
                        stillTheReplica != null && stillTheReplica.isLeader());
            }
        }
    }

    /**
     * The leadership of a restored tablespace is moved by hand onto a replica that never got the restored content.
     * The tablespace must survive it, and the node that ran the restore must still hold every row of it.
     *
     * <p>
     * This is the same marker as the test above, read on the same kind of node, and the difference is who put that
     * node in charge. The automatic takeover refuses the promotion, so it never gets this far; an operator running
     * {@code ALTER TABLESPACE 'ts','leader:othernode'} is not refused anything, and there is no shortage of reasons
     * to run it, from a planned failover to a node that is being decommissioned.
     * </p>
     *
     * <p>
     * What the new leader meets is a restore that completed: both markers are on the log, and the node that ran it is
     * holding the whole content. Nothing the new leader can read tells that apart from a restore that was interrupted
     * and left nothing anywhere, so it must not act on the difference: touching the tablespace here would take the
     * restored data away from the one node that has it.
     * </p>
     *
     * <p>
     * So the new leader refuses to boot the tablespace: it has nothing of it and, as the leader, nobody to download
     * it from. The tablespace has no working leader until the leadership goes back to the node that ran the
     * restore, which is what the end of this test does, and the data is all still there.
     * </p>
     */
    @Test
    public void testLeadershipMovedByHandToAReplicaThatNeverGotTheRestoredContent() throws Exception {

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            TestUtils.execute(server_1.getManager(),
                    "CREATE TABLESPACE '" + RESTORED_TABLESPACE + "','leader:" + server_1.getNodeId()
                            + "','wait:" + BOOT_TIMEOUT + "'", Collections.emptyList());
            TestUtils.execute(server_1.getManager(),
                    "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                            + ",'leader:" + server_1.getNodeId() + "'"
                            + ",'replica:" + server_1.getNodeId() + ",server2'",
                    Collections.emptyList());

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();
                makeTheReplicaFollow(server_1, server_2);
            }
            // the replica is down while the content of the tablespace is replaced. It holds a state of the
            // tablespace from before the restore and it never downloads what comes next, which is the same
            // position a replica that is simply slow ends up in

            TableSpaceManager leader = server_1.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
            assertTrue("the tablespace is not led by the node that is about to restore it", leader.isLeader());

            // the restore runs to the end: none of this data goes through the commit log, only the two markers do
            Table restoredTable = tableOf(TABLE_NAME);
            leader.beginRestore();
            leader.beginRestoreTable(restoredTable.serialize(), new LogSequenceNumber(1, 1));
            List<Record> restoredRows = new ArrayList<>();
            for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                restoredRows.add(RecordSerializer.makeRecord(restoredTable, "c", i, "d", 2));
            }
            ((TableManager) leader.getTableManager(TABLE_NAME)).writeFromDump(restoredRows);
            leader.restoreTableFinished(TABLE_NAME, Collections.emptyList());
            leader.restoreFinished();
            for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                assertTrue("row c=" + i + " never reached the node that ran the restore",
                        existsOnReplica(server_1, i));
            }

            // an operator moves the leadership onto the node that has not got the restored content
            TestUtils.execute(server_1.getManager(),
                    "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                            + ",'leader:server2'"
                            + ",'replica:" + server_1.getNodeId() + ",server2'",
                    Collections.emptyList());

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertFalse("the new leader removed a tablespace it holds nothing of from the metadata of the"
                        + " whole cluster: the node that ran the restore lost the whole restored content, which"
                        + " was the only copy of it",
                        waitForTableSpaceToBeRemoved(server_2, RESTORED_TABLESPACE));
                assertNotNull("tablespace " + RESTORED_TABLESPACE + " is gone", server_2.getManager()
                        .getMetadataStorageManager().describeTableSpace(RESTORED_TABLESPACE));
                TableSpaceManager newLeader = server_2.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                assertFalse("the node that holds nothing of the tablespace is serving it as its leader",
                        newLeader != null && newLeader.isLeader() && !newLeader.isFailed());

                // the way out of the mistake: the leadership goes back to the node that ran the restore, and
                // that node still holds every row of it
                TestUtils.execute(server_1.getManager(),
                        "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                                + ",'leader:" + server_1.getNodeId() + "'"
                                + ",'replica:" + server_1.getNodeId() + ",server2'",
                        Collections.emptyList());
                assertTrue("the tablespace never went back to being led by the node that ran the restore",
                        server_1.getManager().waitForTablespace(RESTORED_TABLESPACE, BOOT_TIMEOUT, true));
                for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                    assertTrue("row c=" + i + " of the restored tablespace is gone from the node that ran the"
                            + " restore", existsOnReplica(server_1, i));
                }
            }
        }
    }

    /**
     * Writes content the ordinary way and takes it away again, so that the replica really is following the
     * tablespace and holds a state of its own below the restore, and the tablespace is left with no table of its
     * own.
     *
     * <p>
     * The content cannot simply be left there. A restore is refused on a tablespace that already holds tables:
     * everything the markers of a restore are used for rests on the tablespace existing only because of that
     * restore, which is true of the tablespace a restore creates for itself and of no other. A node that meets a
     * marker while replaying its log throws away whatever it holds below it and downloads the content afresh.
     * </p>
     */
    private void makeTheReplicaFollow(Server leader, Server replica) throws Exception {
        Table tableBeforeTheRestore = tableOf(TABLE_BEFORE_RESTORE);
        leader.getManager().executeStatement(new CreateTableStatement(tableBeforeTheRestore),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        leader.getManager().executeUpdate(
                new InsertStatement(RESTORED_TABLESPACE, TABLE_BEFORE_RESTORE,
                        RecordSerializer.makeRecord(tableBeforeTheRestore, "c", 0, "d", 1)),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue("the replica never started to follow the tablespace", replica.getManager()
                .waitForTable(RESTORED_TABLESPACE, TABLE_BEFORE_RESTORE, BOOT_TIMEOUT, false));
        waitForKeyOnReplica(replica, TABLE_BEFORE_RESTORE, 0);

        leader.getManager().executeStatement(
                new DropTableStatement(RESTORED_TABLESPACE, TABLE_BEFORE_RESTORE, false),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue("the replica never applied the drop of " + TABLE_BEFORE_RESTORE + ", so it is not really"
                + " following the tablespace", waitForTableToBeGoneOnReplica(replica, TABLE_BEFORE_RESTORE));
    }

    private static boolean waitForTableToBeGoneOnReplica(Server replica, String table) throws Exception {
        for (int i = 0; i < BOOT_TIMEOUT / 100; i++) {
            TableSpaceManager tableSpaceManager = replica.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
            if (tableSpaceManager != null && tableSpaceManager.getTableManager(table) == null) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    /**
     * Watches the logs for a follower thread that died on an entry it could not apply, which is what a follower that
     * does not stop at the marker of a restore ends up doing.
     */
    private static final class FollowerErrors extends Handler {

        final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (record.getThrown() != null && message != null
                    && message.contains("follower error " + RESTORED_TABLESPACE)) {
                error.compareAndSet(null, record.getThrown());
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
     * Waits for the replica to read the marker that opens the restore. A tablespace manager that knows it is in the
     * middle of a restore refuses to take a checkpoint, because the content it holds is not what the tablespace
     * holds any more, and that refusal is how this can be observed from outside. A failed manager refuses as well,
     * so the two are told apart.
     */
    private static boolean waitForTheRestoreToReachTheReplica(TableSpaceManager replica) throws Exception {
        for (int i = 0; i < BOOT_TIMEOUT / 100; i++) {
            if (replica.checkpoint(false, false, false) == null && !replica.isFailed()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    /**
     * Waits for a tablespace to disappear from the metadata every node of the cluster reads, which is what tells a
     * tablespace that was removed from one that merely has no leader.
     */
    private static boolean waitForTableSpaceToBeRemoved(Server server, String tableSpace) throws Exception {
        for (int i = 0; i < TIME_GIVEN_TO_THE_TAKEOVER / 100; i++) {
            if (server.getManager().getMetadataStorageManager().describeTableSpace(tableSpace) == null) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private static boolean waitForFailedTableSpace(TableSpaceManager tableSpaceManager) throws Exception {
        for (int i = 0; i < BOOT_TIMEOUT / 100; i++) {
            if (tableSpaceManager.isFailed()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private static Table tableOf(String name) {
        return Table
                .builder()
                .tablespace(RESTORED_TABLESPACE)
                .name(name)
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
    }

    private RecoveryEvents restoreBackupAndAddReplica(boolean forceDownloadSnapshotOnJoiningNode) throws Exception {

        RecoveryEvents events = new RecoveryEvents();
        Logger rootLogger = Logger.getLogger("");
        rootLogger.addHandler(events);

        try {

            ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
            serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
            serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
            serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
            serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

            ServerConfiguration serverconfig_2 = serverconfig_1
                    .copy()
                    .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                    .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                    .set(ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT, forceDownloadSnapshotOnJoiningNode);

            ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
            client_configuration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
            client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

            try (Server server_1 = new Server(serverconfig_1)) {
                server_1.start();
                server_1.waitForStandaloneBoot();

                Table table = Table
                        .builder()
                        .name(TABLE_NAME)
                        .column("c", ColumnTypes.INTEGER)
                        .column("d", ColumnTypes.INTEGER)
                        .primaryKey("c")
                        .build();
                server_1.getManager().executeStatement(new CreateTableStatement(table),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                    server_1.getManager().executeUpdate(
                            new InsertStatement(TableSpace.DEFAULT, TABLE_NAME, RecordSerializer.makeRecord(table, "c", i, "d", 2)),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                }

                try (HDBClient client = new HDBClient(client_configuration);
                     HDBConnection connection = client.openConnection()) {

                    ByteArrayOutputStream backup = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, backup, new ProgressListener() {
                    });
                    byte[] backupData = backup.toByteArray();

                    // restore the backup into a brand new tablespace, led by server1
                    BackupUtils.restoreTableSpace(RESTORED_TABLESPACE, server_1.getNodeId(), connection,
                            new ByteArrayInputStream(backupData), new ProgressListener() {
                            });

                    assertEquals(ROWS_IN_BACKUP, countRows(connection));

                    // ordinary activity on the restored tablespace: unlike the tables themselves,
                    // these writes do reach the commit log
                    for (int i = FIRST_KEY_AFTER_RESTORE; i < FIRST_KEY_AFTER_RESTORE + ROWS_AFTER_RESTORE; i++) {
                        connection.executeUpdate(RESTORED_TABLESPACE,
                                "INSERT INTO " + RESTORED_TABLESPACE + "." + TABLE_NAME + "(c,d) values(" + i + ",3)",
                                TransactionContext.NOTRANSACTION_ID, false, true, Collections.emptyList());
                    }

                    assertEquals(ROWS_IN_BACKUP + ROWS_AFTER_RESTORE, countRows(connection));

                    // a second node joins the restored tablespace as a replica
                    try (Server server_2 = new Server(serverconfig_2)) {
                        server_2.start();

                        TestUtils.execute(server_1.getManager(),
                                "ALTER TABLESPACE '" + RESTORED_TABLESPACE + "'"
                                        + ",'leader:" + server_1.getNodeId() + "'"
                                        + ",'replica:" + server_1.getNodeId() + "," + server_2.getNodeId() + "'",
                                Collections.emptyList());

                        boolean replicaIsUp =
                                server_2.getManager().waitForTable(RESTORED_TABLESPACE, TABLE_NAME, BOOT_TIMEOUT, false);

                        Throwable failure = events.missingTableManager.get();
                        if (failure != null) {
                            fail("the joining replica could not boot the restored tablespace: it replayed the commit"
                                    + " log from the beginning and found a data change entry for a table that was"
                                    + " never created by any log entry\n" + stackTraceOf(failure));
                        }
                        assertTrue("the restored tablespace never booted on the joining replica", replicaIsUp);
                        assertCommitLogIsNotConsideredFailed(events);

                        TableSpaceManager replicaManager =
                                server_2.getManager().getTableSpaceManager(RESTORED_TABLESPACE);
                        assertNotNull("no tablespace manager on the joining replica", replicaManager);
                        assertFalse("the tablespace manager of the joining replica is failed",
                                replicaManager.isFailed());
                        assertFalse("the joining replica was expected to be a follower", replicaManager.isLeader());

                        assertSystemTablesAreAvailableOnReplica(server_1, server_2);

                        // the replica must really hold the data, both the rows that came from the backup
                        // and the rows that were written to the commit log afterwards
                        waitForKeyOnReplica(server_2, FIRST_KEY_AFTER_RESTORE + ROWS_AFTER_RESTORE - 1);
                        for (int i = 0; i < ROWS_IN_BACKUP; i++) {
                            assertTrue("row c=" + i + " is missing on the joining replica",
                                    existsOnReplica(server_2, i));
                        }
                        for (int i = FIRST_KEY_AFTER_RESTORE; i < FIRST_KEY_AFTER_RESTORE + ROWS_AFTER_RESTORE; i++) {
                            assertTrue("row c=" + i + " is missing on the joining replica",
                                    existsOnReplica(server_2, i));
                        }
                    }
                }
            }
        } finally {
            rootLogger.removeHandler(events);
        }
        return events;
    }

    /**
     * A node that downloads the whole content of a tablespace from the leader must keep the system tables of that
     * tablespace. They are not part of the downloaded data, they are created once while the tablespace manager boots,
     * and they are the only way the CLI, the web console and any client have to introspect the tablespace on this
     * node. The leader is used as the reference of what a normal boot produces, so that this check does not have to
     * repeat the list of the system tables.
     */
    private static void assertSystemTablesAreAvailableOnReplica(Server leader, Server replica) throws Exception {
        Set<String> systemTablesOnLeader = listTables(leader, true);
        Set<String> systemTablesOnReplica;
        Set<String> userTablesOnReplica;
        try {
            systemTablesOnReplica = listTables(replica, true);
            userTablesOnReplica = listTables(replica, false);
        } catch (StatementExecutionException notAvailable) {
            fail("the system tables of " + RESTORED_TABLESPACE + " are not available on the joining replica, so that"
                    + " tablespace cannot be introspected on this node any more\n" + stackTraceOf(notAvailable));
            return;
        }
        assertEquals("the joining replica does not expose the same system tables as the leader",
                systemTablesOnLeader, systemTablesOnReplica);
        assertTrue("the user table is not listed in " + RESTORED_TABLESPACE + ".systables on the joining replica,"
                + " only " + userTablesOnReplica, userTablesOnReplica.contains(TABLE_NAME));
    }

    /**
     * Lists either the system tables or the user tables of the restored tablespace, as seen by one node.
     */
    private static Set<String> listTables(Server server, boolean systemTables) throws Exception {
        Set<String> result = new TreeSet<>();
        // the default tablespace of the cluster is not replicated on the joining node, so it cannot be
        // used as the default tablespace of the query
        try (DataScanner scan = TestUtils.scanWithDefaultTableSpace(server.getManager(), RESTORED_TABLESPACE,
                "SELECT table_name,systemtable FROM " + RESTORED_TABLESPACE + ".systables", Collections.emptyList())) {
            for (DataAccessor row : scan.consume()) {
                if (Boolean.parseBoolean(row.get("systemtable").toString()) == systemTables) {
                    result.add(row.get("table_name").toString());
                }
            }
        }
        return result;
    }

    private static void waitForKeyOnReplica(Server replica, int key) throws Exception {
        waitForKeyOnReplica(replica, TABLE_NAME, key);
    }

    private static void waitForKeyOnReplica(Server replica, String table, int key) throws Exception {
        for (int i = 0; i < 600; i++) {
            if (existsOnReplica(replica, table, key)) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private static boolean existsOnReplica(Server replica, int key) throws Exception {
        return existsOnReplica(replica, TABLE_NAME, key);
    }

    private static boolean existsOnReplica(Server replica, String table, int key) throws Exception {
        return replica
                .getManager()
                .get(new GetStatement(RESTORED_TABLESPACE, table, Bytes.from_int(key), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)
                .found();
    }

    private static int countRows(HDBConnection connection) throws Exception {
        return connection
                .executeScan(RESTORED_TABLESPACE, "SELECT * FROM " + RESTORED_TABLESPACE + "." + TABLE_NAME, true,
                        Collections.emptyList(), 0, 0, 100, true)
                .consume()
                .size();
    }

    private static String stackTraceOf(Throwable error) {
        StringWriter writer = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(writer)) {
            error.printStackTrace(printWriter);
        }
        return writer.toString();
    }

    /**
     * Watches the logs for the two ways in which the joining replica can react to a data change entry that refers to an
     * unknown table: the old NullPointerException and the request for a full download of the data of the tablespace.
     */
    private static final class RecoveryEvents extends Handler {

        final AtomicReference<Throwable> missingTableManager = new AtomicReference<>();

        final AtomicReference<Throwable> fullRecoveryNeeded = new AtomicReference<>();

        final AtomicReference<Throwable> unknownTableDuringRecovery = new AtomicReference<>();

        final AtomicReference<Throwable> restoredFromSnapshot = new AtomicReference<>();

        final AtomicReference<Throwable> recoverableConditionReportedAsLogFailure = new AtomicReference<>();

        @Override
        public void publish(LogRecord record) {
            for (Throwable cursor = record.getThrown(); cursor != null; cursor = cursor.getCause()) {
                if (isMissingTableManagerFailure(cursor)) {
                    missingTableManager.compareAndSet(null, cursor);
                }
                if (cursor instanceof FullRecoveryNeededException) {
                    fullRecoveryNeeded.compareAndSet(null, cursor);
                }
                if (isUnknownTableDuringRecovery(cursor)) {
                    unknownTableDuringRecovery.compareAndSet(null, cursor);
                }
                if (isRestoredFromSnapshot(cursor)) {
                    restoredFromSnapshot.compareAndSet(null, cursor);
                }
                if (isLogFailure(record, cursor)) {
                    recoverableConditionReportedAsLogFailure.compareAndSet(null, cursor);
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        /**
         * The original defect: a NullPointerException raised by {@code TableSpaceManager.apply} while looking up the
         * table manager of a log entry.
         */
        private static boolean isMissingTableManagerFailure(Throwable error) {
            if (!(error instanceof NullPointerException)) {
                return false;
            }
            for (StackTraceElement frame : error.getStackTrace()) {
                if ("herddb.core.TableSpaceManager".equals(frame.getClassName())
                        && "apply".equals(frame.getMethodName())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * The last line of defence: the log holds a change for a table that does not exist on this node. The marker
         * the restore writes on the log makes the node give up well before this point, so this is what a node meets
         * only when the log of the restored tablespace carries no marker at all.
         */
        private static boolean isUnknownTableDuringRecovery(Throwable error) {
            return error instanceof FullRecoveryNeededException
                    && error.getMessage() != null
                    && error.getMessage().contains("refers to table " + TABLE_NAME)
                    && error.getMessage().contains("does not exist on this node");
        }

        /**
         * The recovery path this test is about: the log says that the content of the tablespace was replaced by a
         * snapshot, so the whole content of the tablespace has to be downloaded from the leader.
         */
        private static boolean isRestoredFromSnapshot(Throwable error) {
            return error instanceof FullRecoveryNeededException
                    && error.getMessage() != null
                    && error.getMessage().contains("restore of tablespace " + RESTORED_TABLESPACE)
                    && error.getMessage().contains("FINISHED");
        }

        /**
         * A commit log reporting the request for a full download as a fatal error of the log. The commit log
         * implementations mark themselves as failed exactly where they log this, and they never clear that flag, so
         * this is the observable side of a log that will make the tablespace manager fail even after the download
         * succeeded.
         */
        private static boolean isLogFailure(LogRecord record, Throwable error) {
            return error instanceof FullRecoveryNeededException
                    && Level.SEVERE.equals(record.getLevel())
                    && record.getSourceClassName() != null
                    && record.getSourceClassName().endsWith("CommitLog");
        }
    }
}
