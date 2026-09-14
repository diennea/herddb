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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.CommitLog;
import herddb.log.CommitLogListener;
import herddb.log.CommitLogManager;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.storage.DataStorageManagerException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.Test;

/**
 * Dumping a tablespace locks it and, when the commit log is part of the dump, hooks a listener onto that log. Both
 * have to be given back whatever happens to the dump, and a dump can fail before it has sent a single byte: the
 * checkpoint it starts from is not always taken, a restore that is replacing the content of the very same tablespace
 * being one of the reasons. A lock stamp that is never released blocks every later write, DDL, checkpoint and
 * follower of that tablespace for as long as the process lives.
 */
public class DumpTableSpaceLockTest {

    private static final String NODE_ID = "localhost";

    private static final String TABLESPACE = "dumpedts";

    private static final String TABLE_NAME = "t1";

    private static final int BOOT_TIMEOUT = 60000;

    /**
     * How long an operation that needs the lock of the tablespace is given before we call the lock lost.
     */
    private static final int LOCK_TIMEOUT = 20000;

    @Test
    public void testDumpThatCannotTakeItsCheckpointGivesTheTableSpaceBack() throws Exception {
        AtomicInteger attachedListeners = new AtomicInteger();
        CommitLogManager logManager = new CommitLogManager() {

            private final MemoryCommitLogManager plainLogs = new MemoryCommitLogManager();

            @Override
            public CommitLog createCommitLog(
                    String tableSpaceUUID, String tableSpaceName, String localNodeId
            ) throws LogNotAvailableException {
                CommitLog log = plainLogs.createCommitLog(tableSpaceUUID, tableSpaceName, localNodeId);
                if (!TABLESPACE.equals(tableSpaceName)) {
                    return log;
                }
                return new LogCountingItsListeners(log, attachedListeners);
            }
        };

        DBManager manager = new DBManager(NODE_ID, new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), logManager, null, null);
        // closing the manager needs the lock of every tablespace, so a lock this test failed to get back would
        // hang the whole suite here instead of reporting anything: it is given a while and then left behind
        try {
            manager.start();
            manager.waitForBootOfLocalTablespaces(BOOT_TIMEOUT);
            manager.executeStatement(
                    new CreateTableSpaceStatement(TABLESPACE, Collections.singleton(NODE_ID), NODE_ID, 1,
                            BOOT_TIMEOUT, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(TABLESPACE);
            // a restore is replacing the content of this tablespace: no checkpoint may be taken while it runs, so
            // the dump below cannot even get started. Two legitimate operations, a backup and a restore, running at
            // the same time on the same node.
            // The tablespace holds no table of its own, because that is the only tablespace a restore can be opened
            // on: a restore replaces the whole content of a tablespace, and the markers it leaves on the log are
            // read as "this tablespace exists only because of that restore"
            tableSpaceManager.beginRestore();

            try {
                // the channel is never reached: the dump gives up on the checkpoint, well before it sends anything
                tableSpaceManager.dumpTableSpace("adumpid", null, 10, true);
                fail("the dump was expected to fail, the checkpoint it starts from cannot be taken while a restore"
                        + " is replacing the content of the tablespace");
            } catch (DataStorageManagerException expected) {
            }

            assertTrue("the dump failed and left the tablespace locked: every later write, DDL, checkpoint and"
                    + " follower of " + TABLESPACE + " is blocked for as long as this process lives",
                    lockOfTheTableSpaceIsFree(tableSpaceManager));
            assertFalse("the dump failed and left its listener attached to the commit log: every entry written from"
                    + " now on is copied into a list nobody is ever going to read",
                    attachedListeners.get() > 0);
        } finally {
            closeWithoutHangingTheSuite(manager);
        }
    }

    /**
     * Closes a database manager, giving up on it if it does not come back. Closing needs the lock of every
     * tablespace, so a manager holding a lock nobody released never closes, and a test about exactly that must not
     * turn into a suite that hangs.
     */
    private static void closeWithoutHangingTheSuite(DBManager manager) throws Exception {
        Thread closing = new Thread(manager::close, "closing-" + NODE_ID);
        closing.setDaemon(true);
        closing.start();
        closing.join(LOCK_TIMEOUT);
    }

    /**
     * Asks for an operation that needs the write lock of the tablespace and waits a while for it. Restoring a table
     * is one such operation, and it is one a restore that has just declared itself is expected to be able to run.
     */
    private static boolean lockOfTheTableSpaceIsFree(TableSpaceManager tableSpaceManager) throws Exception {
        Thread waitingForTheLock = new Thread(() -> {
            try {
                tableSpaceManager.beginRestoreTable(table().serialize(), new LogSequenceNumber(1, 1));
            } catch (RuntimeException whatever) {
                // the lock is what this is about, not what the operation makes of it
            }
        }, "waiting-for-the-lock-of-" + TABLESPACE);
        waitingForTheLock.setDaemon(true);
        waitingForTheLock.start();
        waitingForTheLock.join(LOCK_TIMEOUT);
        return !waitingForTheLock.isAlive();
    }

    private static Table table() {
        return Table
                .builder()
                .tablespace(TABLESPACE)
                .name(TABLE_NAME)
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
    }

    /**
     * A commit log that keeps count of the listeners hooked onto it, so that a test can tell whether the ones that
     * are attached for the duration of an operation are detached when that operation is over.
     */
    private static final class LogCountingItsListeners extends CommitLog {

        private final CommitLog log;

        private final AtomicInteger attachedListeners;

        LogCountingItsListeners(CommitLog log, AtomicInteger attachedListeners) {
            this.log = log;
            this.attachedListeners = attachedListeners;
        }

        @Override
        public void attachCommitLogListener(CommitLogListener l) {
            attachedListeners.incrementAndGet();
            log.attachCommitLogListener(l);
        }

        @Override
        public void removeCommitLogListener(CommitLogListener l) {
            attachedListeners.decrementAndGet();
            log.removeCommitLogListener(l);
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
}
