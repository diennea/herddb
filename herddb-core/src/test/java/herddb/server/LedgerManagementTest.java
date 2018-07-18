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
import herddb.cluster.LedgersInfo;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.utils.DataAccessor;
import herddb.utils.ZKTestEnv;

/**
 * Booting two servers, one table space
 *
 * @author enrico.olivelli
 */
public class LedgerManagementTest {

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
    public void test_ledgerlist() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0); // only explicit checkpoint
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 100);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            LedgersInfo actualLedgersList = log.getActualLedgersList();
            assertEquals(0, log.getLastLedgerId());
            assertEquals(1, actualLedgersList.getActiveLedgers().size());
            assertTrue(actualLedgersList.getActiveLedgers().contains(0L));
            System.out.println("actualLedgersList:" + actualLedgersList + " lastLedgerId " + log.getLastLedgerId());

        }
        Thread.sleep(100);
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            LedgersInfo actualLedgersList = log.getActualLedgersList();
            System.out.println("actualLedgersList:" + actualLedgersList + " lastLedgerId " + log.getLastLedgerId());
            assertEquals(1, log.getLastLedgerId());
            assertEquals(2, actualLedgersList.getActiveLedgers().size());
            assertTrue(actualLedgersList.getActiveLedgers().contains(0L));
            assertTrue(actualLedgersList.getActiveLedgers().contains(1L));
        }
        Thread.sleep(100);
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            LedgersInfo actualLedgersList = log.getActualLedgersList();
            System.out.println("actualLedgersList:" + actualLedgersList + " lastLedgerId " + log.getLastLedgerId());
            assertEquals(2, log.getLastLedgerId());
            assertEquals(3, actualLedgersList.getActiveLedgers().size());
            assertTrue(actualLedgersList.getActiveLedgers().contains(0L));
            // ledger id 1 dropped at restart
            assertTrue(actualLedgersList.getActiveLedgers().contains(2L));
            server_1.getManager().checkpoint();

            LedgersInfo actualLedgersList2 = log.getActualLedgersList();
            System.out.println("actualLedgersList2:" + actualLedgersList2 + " lastLedgerId " + log.getLastLedgerId());
            assertEquals(2, log.getLastLedgerId());
            assertEquals(1, actualLedgersList2.getActiveLedgers().size());
            // ledger id 0 dropped at checkpoint 
            assertTrue(actualLedgersList2.getActiveLedgers().contains(2L));
        }
        Thread.sleep(100);
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            LedgersInfo actualLedgersList = log.getActualLedgersList();
            System.out.println("actualLedgersList:" + actualLedgersList + " lastLedgerId " + log.getLastLedgerId());
            assertEquals(3, log.getLastLedgerId());
            assertEquals(2, actualLedgersList.getActiveLedgers().size());
            // ledger id 0 dropped at checkpoint 
            assertTrue(actualLedgersList.getActiveLedgers().contains(2L));
            assertTrue(actualLedgersList.getActiveLedgers().contains(3L));
        }
    }
}
