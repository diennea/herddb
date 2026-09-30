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

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.backup.DumpedLogEntry;
import herddb.log.CommitLogResult;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A log entry that changes a table this node does not have is reported instead of being dereferenced, and the report
 * has to say where that entry is. Naming the position is the delicate part: outside recovery a write can still be
 * waiting for the log to acknowledge it, and asking such a write where it landed blocks the writer until the answer
 * comes and throws when the answer is a failure. Building the message must never be the thing that stalls the write
 * path, and it must never replace the problem being reported with an unrelated one.
 */
public class LogEntryForAMissingTableTest {

    private static final String MISSING_TABLE_NAME = "gone";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testEntryForAMissingTableIsReportedWithoutWaitingForTheLog() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            // a write the log has taken over and not acknowledged yet, whose acknowledgement then fails. This is
            // what a replicated log really does: the position of a write is answered asynchronously
            CompletableFuture<LogSequenceNumber> notAcknowledgedYet = new CompletableFuture<>();
            notAcknowledgedYet.completeExceptionally(new LogNotAvailableException("the log never answered"));
            CommitLogResult deferred = new CommitLogResult(notAcknowledgedYet, true, false);

            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            try {
                tableSpaceManager.apply(deferred, entryForTheMissingTable(), false);
                fail("a log entry that changes a table this node does not have was applied");
            } catch (DataStorageManagerException expected) {
                assertTrue("the failure does not name the table that is missing: " + expected,
                        String.valueOf(expected.getMessage()).contains(MISSING_TABLE_NAME));
            } catch (LogNotAvailableException wrongProblem) {
                fail("building the message about a table that does not exist asked the log where it wrote an entry"
                        + " it has not acknowledged yet, so what came out is a problem of the log and not the"
                        + " missing table, and on a log that is merely slow the write path would have blocked"
                        + " instead: " + wrongProblem);
            }
        }
    }

    /**
     * Control case: when the position is already known, it is the position that is named. The message is worth
     * little if it never says where the entry that cannot be applied is.
     */
    @Test
    public void testEntryForAMissingTableNamesItsPositionWhenItIsKnown() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            LogSequenceNumber position = new LogSequenceNumber(7, 42);
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            try {
                tableSpaceManager.apply(new CommitLogResult(position, false, true), entryForTheMissingTable(), false);
                fail("a log entry that changes a table this node does not have was applied");
            } catch (DataStorageManagerException expected) {
                assertTrue("the failure does not say where the entry that cannot be applied is: " + expected,
                        String.valueOf(expected.getMessage()).contains(position.toString()));
            }
        }
    }

    /**
     * The tail of the log a dump carries is replayed like the log of a boot, and it can name a table the dump does
     * not carry: a table created inside a transaction that was still open when the dump was taken is never part of
     * a checkpoint and is therefore never streamed, while the transaction that created it is. That entry has to be
     * reported for what it is. It is not this node's local data that is missing something, so there is nothing to
     * download from anybody, and telling the operator who is restoring a backup that a full download of the data of
     * the tablespace from the leader is needed sends them after a node that has nothing to do with it.
     */
    @Test
    public void testEntryOfARestoredDumpForAMissingTableIsNotBlamedOnTheLocalData() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            LogSequenceNumber position = new LogSequenceNumber(7, 42);
            DumpedLogEntry dumped = new DumpedLogEntry(position, entryForTheMissingTable().serialize());
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            try {
                tableSpaceManager.restoreRawDumpedEntryLogs(Collections.singletonList(dumped));
                fail("an entry of the dump that changes a table the dump does not carry was applied");
            } catch (FullRecoveryNeededException wrongAdvice) {
                fail("the restore of a dump that does not describe its own content was reported as local data that"
                        + " has to be replaced by a download from the leader, which is not what happened and not"
                        + " something anybody can act on: " + wrongAdvice.getMessage());
            } catch (DataStorageManagerException expected) {
                assertTrue("the failure does not name the table that is missing: " + expected,
                        String.valueOf(expected.getMessage()).contains(MISSING_TABLE_NAME));
                assertTrue("the failure does not say that it is the dump that is incomplete: " + expected,
                        String.valueOf(expected.getMessage()).contains("dump"));
            }
        }
    }

    private static LogEntry entryForTheMissingTable() {
        Table table = Table
                .builder()
                .tablespace(TableSpace.DEFAULT)
                .name(MISSING_TABLE_NAME)
                .column("c", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
        return LogEntryFactory.delete(table, Bytes.from_int(1), null);
    }
}
