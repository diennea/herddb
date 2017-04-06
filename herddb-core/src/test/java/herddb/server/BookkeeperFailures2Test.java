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

import herddb.cluster.BookkeeperCommitLog;
import herddb.codec.RecordSerializer;
import herddb.core.ActivatorRunRequest;
import herddb.core.TableSpaceManager;
import static herddb.core.TestUtils.scan;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.ZKTestEnv;
import java.util.Collections;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * We are going to fence forcibly a ledger during the normal execution
 *
 * @author enrico.olivelli
 */
public class BookkeeperFailures2Test {

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.FINEST;
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
            }
        });
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        SimpleFormatter f = new SimpleFormatter();
        ch.setFormatter(f);
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }

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
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            StatementExecutionResult executeStatement = server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long transactionId = executeStatement.transactionId;

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(transactionId));

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(transactionId));
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 1);

            Transaction transaction = tableSpaceManager.getTransactions().stream().filter(t -> t.transactionId == transactionId).findFirst().get();
            transaction.synch();

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList(), new TransactionContext(transactionId));) {
                assertEquals(3, scan.consume().size());
            }

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList(), TransactionContext.NO_TRANSACTION);) {
                // no record, but the table exists!
                assertEquals(0, scan.consume().size());
            }

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            testEnv.stopBookie();

            // transaction will continue and see the failure only the time of the commit
            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(transactionId));
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(transactionId));
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(transactionId));
            } catch (StatementExecutionException expected) {
                // in can happen that the log gets closed
                assertEquals(herddb.log.LogNotAvailableException.class, expected.getCause().getClass());
            }

            try {
                server.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                // this will fail alweays
                fail();
            } catch (StatementExecutionException expected) {
            }

            testEnv.startBookie(false);

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(100);
            }
            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList());) {
                assertEquals(1, scan.consume().size());
            }
        }
    }
}
