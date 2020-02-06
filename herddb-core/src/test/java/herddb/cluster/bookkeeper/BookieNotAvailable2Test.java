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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.cluster.BookkeeperCommitLog;
import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

public class BookieNotAvailable2Test extends BookkeeperFailuresBase {

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
                assertEquals("no such transaction " + transactionId + " in tablespace " + TableSpace.DEFAULT, expected.getMessage());
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
}
