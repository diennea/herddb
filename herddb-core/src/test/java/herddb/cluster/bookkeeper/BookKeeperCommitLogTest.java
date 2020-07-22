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
package herddb.cluster.bookkeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.TableSpace;
import herddb.server.ServerConfiguration;
import herddb.utils.TestUtils;
import herddb.utils.ZKTestEnv;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BookKeeperCommitLogTest {

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
    public void testSimpleReadWrite() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            LogSequenceNumber lsn1;
            LogSequenceNumber lsn2;
            LogSequenceNumber lsn3;
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.startWriting(1);
                lsn1 = writer.log(LogEntryFactory.beginTransaction(1), true).getLogSequenceNumber();
                lsn2 = writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber();
                lsn3 = writer.log(LogEntryFactory.beginTransaction(3), true).getLogSequenceNumber();
                assertTrue(lsn1.after(LogSequenceNumber.START_OF_TIME));
                assertTrue(lsn2.after(lsn1));
                assertTrue(lsn3.after(lsn2));
            }

            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (a, b) -> {
                    if (b.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(a, b));
                    }
                }, false);
                assertEquals(3, list.size());
                assertEquals(lsn1, list.get(0).getKey());
                assertEquals(lsn2, list.get(1).getKey());
                assertEquals(lsn3, list.get(2).getKey());
            }

        }
    }

    @Test
    public void testSimpleFence() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            LogSequenceNumber lsn1;
            LogSequenceNumber lsn2;
            LogSequenceNumber lsn3;
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.startWriting(1);
                lsn1 = writer.log(LogEntryFactory.beginTransaction(1), true).getLogSequenceNumber();
                lsn2 = writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber();

                // a new leader starts, from START_OF_TIME
                try (BookkeeperCommitLog writer2 = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                    writer2.recovery(LogSequenceNumber.START_OF_TIME, (a, b) -> {}, true);
                    writer2.startWriting(1);
                    lsn3 = writer2.log(LogEntryFactory.beginTransaction(3), true).getLogSequenceNumber();
                }

                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> FutureUtils.result(writer.log(LogEntryFactory.beginTransaction(3), true).logSequenceNumber));

                assertTrue(writer.isFailed());

                assertTrue(lsn1.after(LogSequenceNumber.START_OF_TIME));
                assertTrue(lsn2.after(lsn1));

                // written by second writer
                assertTrue(lsn3.after(lsn2));
            }

            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(lsn, entry));
                    }
                }, false);
                assertEquals(3, list.size());
                assertEquals(lsn1, list.get(0).getKey());
                assertEquals(lsn2, list.get(1).getKey());
                assertEquals(lsn3, list.get(2).getKey());
            }

        }
    }

    @Test
    public void testWriteAsync() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            CommitLogResult res1;
            CommitLogResult res2;
            CommitLogResult res3;

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.startWriting(1);
                res1 = writer.log(LogEntryFactory.beginTransaction(1), false);
                res2 = writer.log(LogEntryFactory.beginTransaction(2), false);
                res3 = writer.log(LogEntryFactory.beginTransaction(3), true);
                assertTrue(res1.deferred);
                assertFalse(res1.sync);
                assertTrue(res2.deferred);
                assertFalse(res2.sync);
                assertFalse(res3.deferred);
                assertTrue(res3.sync);
                assertNull(res1.getLogSequenceNumber());
                assertNull(res2.getLogSequenceNumber());
                assertNotNull(res3.getLogSequenceNumber());
            }

            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(lsn, entry));
                    }
                }, false);
                assertEquals(3, list.size());

                assertTrue(list.get(0).getKey().after(LogSequenceNumber.START_OF_TIME));
                assertTrue(list.get(1).getKey().after(list.get(0).getKey()));
                assertTrue(list.get(2).getKey().after(list.get(1).getKey()));
            }

        }
    }

    @Test
    public void testBookieFailureSyncWrites() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.startWriting(1);

                writer.log(LogEntryFactory.beginTransaction(1), true).getLogSequenceNumber();

                this.testEnv.pauseBookie();
                // this is deemed to fail and we will close the log
                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber()
                );
                assertTrue(writer.isFailed());

                // this one will fail as well
                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber()
                );

                // no way to recover this instance of BookkeeperCommitLog
                // we will bounce the leader and it will restart with a full recovery
                // in a production env you do not have only one bookie, and the failure
                // of a single bookie will be handled with an ensemble change
                this.testEnv.resumeBookie();

                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber()
                );
            }

            // check expected reads
            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(lsn, entry));
                    }
                }, false);
                assertEquals(1, list.size());

                assertTrue(list.get(0).getKey().after(LogSequenceNumber.START_OF_TIME));
            }

        }
    }

    @Test
    public void testBookieFailureAsyncWrites() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            man.start();
            logManager.start();

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.startWriting(1);

                // many async writes
                writer.log(LogEntryFactory.beginTransaction(1), false).getLogSequenceNumber();
                writer.log(LogEntryFactory.beginTransaction(1), false).getLogSequenceNumber();
                writer.log(LogEntryFactory.beginTransaction(1), false).getLogSequenceNumber();
                // one sync write
                writer.log(LogEntryFactory.beginTransaction(1), true).getLogSequenceNumber();

                this.testEnv.pauseBookie();
                // this is deemed to fail and we will close the log

                // writer won't see the failure, because this is a deferred write
                writer.log(LogEntryFactory.beginTransaction(2), false).getLogSequenceNumber();

                // this one will fail as well, and we will catch the error, because this is a sync write
                // like a "transaction commit"
                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber()
                );

                // no way to recover this instance of BookkeeperCommitLog
                // we will bounce the leader and it will restart with a full recovery
                // in a production env you do not have only one bookie, and the failure
                // of a single bookie will be handled with an ensemble change
                this.testEnv.resumeBookie();

                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> writer.log(LogEntryFactory.beginTransaction(2), true).getLogSequenceNumber()
                );
            }

            // check expected reads
            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (lsn, entry) -> {
                    if (entry.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(lsn, entry));
                    }
                }, false);
                assertEquals(4, list.size());
            }

        }
    }

    @Test
    public void testMaxLedgerSize() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        final int maxLedgerSize = 1024;
        final LogEntry entry = LogEntryFactory.beginTransaction(1);
        final int estimateSize = entry.serialize().length;
        final int numberOfLedgers = 10;
        final int numberOfEntries = 1 + numberOfLedgers * maxLedgerSize / estimateSize;
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setMaxLedgerSizeBytes(maxLedgerSize);
            man.start();
            logManager.start();
            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                // do not pollute the count with NOOP entries
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);
                for (int i = 0; i < numberOfEntries; i++) {
                    writer.log(entry, false);
                }
                writer.log(entry, true).getLogSequenceNumber();
            }

            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                Set<Long> ledgerIds = new HashSet<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (a, b) -> {
                    if (b.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(a, b));
                        ledgerIds.add(a.ledgerId);
                    }
                }, false);
                assertEquals(numberOfEntries + 1, list.size());
                assertEquals(numberOfLedgers, ledgerIds.size());
            }

        }
    }

    @Test
    public void testMaxLedgerSizeWithTemporaryError() throws Exception {
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        final int maxLedgerSize = 1024;
        final LogEntry entry = LogEntryFactory.beginTransaction(1);
        final int estimateSize = entry.serialize().length;
        final int numberOfLedgers = 10;
        final int numberOfEntries = 1 + numberOfLedgers * maxLedgerSize / estimateSize;
        System.out.println("writing " + numberOfEntries + " entries");
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setMaxLedgerSizeBytes(maxLedgerSize);
            man.start();
            logManager.start();

            assertTrue(numberOfEntries > 70);

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                // do not pollute the count with NOOP entries
                writer.setWriteLedgerHeader(false);
                writer.startWriting(1);
                for (int i = 0; i < numberOfEntries; i++) {
                    writer.log(entry, false);
                    if (i == 70) {
                        // stop bookie, but writes will continue to be acklowledged
                        testEnv.pauseBookie();
                    }
                }

                // even if we restart the bookie we must not be able to acknowledge the write
                testEnv.resumeBookie();
                TestUtils.assertThrows(LogNotAvailableException.class, ()
                        -> writer.log(entry, true).getLogSequenceNumber()
                );
            }

            try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                List<Map.Entry<LogSequenceNumber, LogEntry>> list = new ArrayList<>();
                reader.recovery(LogSequenceNumber.START_OF_TIME, (a, b) -> {
                    if (b.type != LogEntryType.NOOP) {
                        list.add(new AbstractMap.SimpleImmutableEntry<>(a, b));
                    }
                }, false);
                assertTrue("unexpected number of entries on reader: " + list.size(), list.size() <= 80);
            }

        }
    }

    @Test
    public void testFollowEmptyLedgerBookieDown() throws Exception {
        String secondBookie = testEnv.startNewBookie();
        final String tableSpaceUUID = UUID.randomUUID().toString();
        final String name = TableSpace.DEFAULT;
        final String nodeid = "nodeid";
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
                BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE)) {
            logManager.setEnsemble(2);
            logManager.setWriteQuorumSize(2);
            logManager.setAckQuorumSize(2);
            man.start();
            logManager.start();

            try (BookkeeperCommitLog writer = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                writer.startWriting(1);

                // create a ledger, up to 0.14.x no "logical" write happens, so Bookies are not aware of the
                // the ledger
                // stop one bookie
                testEnv.stopBookie(secondBookie);

                // start a reader, it will see the ledger "OPEN" and it will try to read from all of the Bookies in the tail of the ledger
                // one Bookie would answer "NoSuchLedger" and the other bookie is down,
                // but since 0.15.0 we are now writing a NOOP entry at the beginning of the ledger in order to workaround this issue.
                try (BookkeeperCommitLog reader = logManager.createCommitLog(tableSpaceUUID, name, nodeid);) {
                    reader.recovery(LogSequenceNumber.START_OF_TIME, (a, b) -> {
                    }, false);
                }
            }

        }
    }

}
