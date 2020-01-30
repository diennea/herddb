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

import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.codec.RecordSerializer;
import herddb.core.ActivatorRunRequest;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.ZKTestEnv;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * We are going to fence forcibly a ledger during the normal execution
 *
 * @author enrico.olivelli
 */
public class BookkeeperFailuresTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookie();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    private BookKeeper createBookKeeper() throws Exception {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setZkServers(testEnv.getAddress());
        clientConfiguration.setZkLedgersRootPath(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
        return BookKeeper.forConfig(clientConfiguration).build();
    }

    @Test
    public void testFencing() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            try (BookKeeper bk = createBookKeeper()) {
                try (LedgerHandle fenceLedger = bk.openLedger(ledgerId,
                        BookKeeper.DigestType.CRC32C, "herddb".getBytes(StandardCharsets.UTF_8))) {
                }
            }

            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException expected) {
            }

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.
                        isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(100);
            }

            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                        TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                    TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList())) {
                assertEquals(4, scan.consume().size());
            }
        }
    }

    @Test
    public void testFencingDuringTransaction() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            StatementExecutionResult executeStatement = server.getManager().executeUpdate(new InsertStatement(
                    TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long transactionId = executeStatement.transactionId;
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            try (BookKeeper bk = createBookKeeper()) {
                try (LedgerHandle fenceLedger = bk.openLedger(ledgerId,
                        BookKeeper.DigestType.CRC32C, "herddb".getBytes(StandardCharsets.UTF_8))) {
                }
            }

            // transaction will continue and see the failure only the time of the commit
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));

            try {
                server.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, transactionId),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException expected) {
            }

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.
                        isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(100);
            }
            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                        TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                    TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }
        }
    }

    @Test
    public void testBookieNoAvailableNoTransaction() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            testEnv.stopBookie();

            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException expected) {
            }

            testEnv.startBookie(false);

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.
                        isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(100);
            }

            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                        TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                    TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList())) {
                assertEquals(4, scan.consume().size());
            }
        }
    }

    @Test
    public void testBookieNotAvailableDuringTransaction() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();

            // create table is done out of the transaction (this is very like autocommit=true)
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            StatementExecutionResult executeStatement = server.getManager().executeUpdate(new InsertStatement(
                    TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long transactionId = executeStatement.transactionId;

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            Transaction transaction = tableSpaceManager.getTransactions().stream().filter(t -> t.transactionId == transactionId).findFirst().get();
            // Transaction will synch, so every addEntry will be acked, but will not be "confirmed" yet
            transaction.sync();

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList(),
                    new TransactionContext(transactionId))) {
                assertEquals(3, scan.consume().size());
            }

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList(),
                    TransactionContext.NO_TRANSACTION)) {
                // no record, but the table exists!
                assertEquals(0, scan.consume().size());
            }

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            testEnv.stopBookie();

            // transaction will continue and see the failure only the time of the commit
            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(transactionId));
                System.out.println("Insert of c,4 OK"); // this will piggyback the LAC for the transaction
            } catch (StatementExecutionException expected) {
                System.out.println("Insert of c,4 failed " + expected);
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }
            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(transactionId));
                System.out.println("Insert of c,5 OK");  // this will piggyback the LAC for the transaction
            } catch (StatementExecutionException expected) {
                System.out.println("Insert of c,5 failed " + expected);
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }
            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(transactionId));
                System.out.println("Insert of c,6 OK");  // this will piggyback the LAC for the transaction
            } catch (StatementExecutionException expected) {
                System.out.println("Insert of c,6 failed " + expected);
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }

            try {
                server.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, transactionId),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                // this will fail alweays
                fail();
            } catch (StatementExecutionException expected) {
                System.out.println("Commit failed as expected:" + expected);
            }

            testEnv.startBookie(false);

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.
                        isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(100);
            }

            try (BookKeeper bk = createBookKeeper();
                 LedgerHandle handle = bk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32C, "herddb".
                         getBytes(StandardCharsets.UTF_8))) {
                BookKeeperAdmin admin = new BookKeeperAdmin(bk);
                try {
                    LedgerMetadata ledgerMetadata = admin.getLedgerMetadata(handle);
                    System.out.println("current ledger metadata before recovery: " + ledgerMetadata);
                } finally {
                    admin.close();
                }
            }

            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                        TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                    TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            // the insert should succeed because the trasaction has been rolledback automatically
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }
        }
    }

    @Test
    public void testBookieNotAvailableDuringTransactionWithRollback() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();

            // create table is done out of the transaction (this is very like autocommit=true)
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

             // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            // no bookie
            // but the client will fill the transaction with data and see the error only during commit
            testEnv.stopBookie();

            // start a transaction
            StatementExecutionResult executeStatement = server.getManager().executeUpdate(new InsertStatement(
                    TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long transactionId = executeStatement.transactionId;

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(transactionId));
                System.out.println("Insert of c,4 OK"); // this will piggyback the LAC for the transaction
            } catch (StatementExecutionException expected) {
                System.out.println("Insert of c,4 failed " + expected);
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }
            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(transactionId));
                System.out.println("Insert of c,5 OK");  // this will piggyback the LAC for the transaction
            } catch (StatementExecutionException expected) {
                System.out.println("Insert of c,5 failed " + expected);
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }
            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                                makeRecord(table, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        new TransactionContext(transactionId));
                System.out.println("Insert of c,6 OK");  // this will piggyback the LAC for the transaction
            } catch (StatementExecutionException expected) {
                System.out.println("Insert of c,6 failed " + expected);
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }

             // verify that the commit operation is reported as failed to the client
            try {
                server.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, transactionId),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException expected) {
                System.out.println("Commit failed as expected:" + expected);
            }

            // start the bookie
            testEnv.startBookie(false);

            // this is a bit hacky but there is a race and BookKeeperCommit automatically closes
            // itself in background when BK write errors occour
            log.resetClosedFlagForTests();
            log.rollNewLedger();

            // the client sees an error, and blindly sends a rollback
            // this rollback should not go to the log
            // otherwise during recovery we will see a rollback for a transaction that never began
            try {
                server.getManager().executeStatement(new RollbackTransactionStatement(TableSpace.DEFAULT, transactionId),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
             } catch (StatementExecutionException expected) {
                System.out.println("Rollback failed as expected:" + expected);
                assertEquals("no such transaction "+transactionId+" in tablespace "+TableSpace.DEFAULT, expected.getMessage());
            }

             // start a follower, it must be able to boot
            server.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                            new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            ServerConfiguration serverconfig_2 = new ServerConfiguration(folder.newFolder().toPath());
            serverconfig_2.set(ServerConfiguration.PROPERTY_NODEID, "server2");
            serverconfig_2.set(ServerConfiguration.PROPERTY_PORT, 7868);
            serverconfig_2.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
            serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

            try (Server server2 = new Server(serverconfig_2)) {
                server2.start();
                server2.waitForTableSpaceBoot(TableSpace.DEFAULT, false);
            }
        }
    }

    @Test
    public void testLedgerClosedError() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // we do not want auto-recovery in ase of log failures
            server.getManager().setActivatorPauseStatus(true);

            assertEquals(ledgerId, log.getWriter().getOut().getId());

            // force close of the LedgerHandle
            // this may happen internally in BK in case of internal errors
            log.getWriter().getOut().close();

            // we should recover
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                            makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            assertNotEquals(ledgerId, log.getWriter().getOut().getId());

            ServerConfiguration serverconfig_2 = new ServerConfiguration(folder.newFolder().toPath());
            serverconfig_2.set(ServerConfiguration.PROPERTY_NODEID, "server2");
            serverconfig_2.set(ServerConfiguration.PROPERTY_PORT, 7868);
            serverconfig_2.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
            serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
            serverconfig_2.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

            // set server2 as new leader
            server.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                            new HashSet<>(Arrays.asList("server1", "server2")), "server2", 2, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // stop server1
            server.close();

            // boot a new leader, it will recover from bookkeeper
            try (Server server2 = new Server(serverconfig_2)) {
                server2.start();
                // wait for the boot of the new leader
                server2.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

                // the server must have all of the data
                try (DataScanner scan = scan(server2.getManager(), "SELECT * FROM t1", Collections.emptyList())) {
                    List<DataAccessor> consume = scan.consume();
                    assertEquals(4, consume.size());
                }
            }
        }
    }
}
