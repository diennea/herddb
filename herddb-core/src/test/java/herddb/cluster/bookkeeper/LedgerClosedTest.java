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

import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.LedgersInfo;
import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper;
import org.junit.Test;

public class LedgerClosedTest extends BookkeeperFailuresBase {

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

    @Test
    public void uselessLedgerDroppedError() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());


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

        }
        // restart
        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
        }
        // restart
        LedgersInfo actualLedgersList;
        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            actualLedgersList = log.getActualLedgersList();
            assertEquals(3, actualLedgersList.getActiveLedgers().size());
            System.out.println("actualLedgersList: " + actualLedgersList);
            long lastLedgerId = log.getLastLedgerId();
            assertEquals(lastLedgerId, actualLedgersList.getActiveLedgers().get(actualLedgersList.getActiveLedgers().size() - 1).longValue());

            try (BookKeeper bk = createBookKeeper()) {
                long ledgerIdToDrop = actualLedgersList.getActiveLedgers().get(0);
                System.out.println("dropping " + ledgerIdToDrop);
                bk.newDeleteLedgerOp().withLedgerId(ledgerIdToDrop).execute().get();
            }
        }
        // restart again,
        // the server should boot even if the ledger does not exist anymore
        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
        }
    }
}
