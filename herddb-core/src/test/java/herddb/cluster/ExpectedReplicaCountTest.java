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
package herddb.cluster;

import static org.junit.Assert.assertEquals;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.core.TestUtils;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests about expectedreplicacount persistence guarantees
 *
 * @author enrico.olivelli
 */
public class ExpectedReplicaCountTest {

    private static final Logger LOG = Logger.getLogger(ExpectedReplicaCountTest.class.getName());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
        // as expectedreplicacount is 2 we need at least two bookies
        testEnv.startNewBookie();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testDisklessClusterReplication() throws Exception {

        TestStatsProvider statsProvider = new TestStatsProvider();

        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            TestUtils.execute(server_1.getManager(),
                    "CREATE TABLESPACE 'ttt','leader:" + server_1.getNodeId() + "','expectedreplicacount:2'",
                    Collections.emptyList());

            // perform some writes
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
            clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
            clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
            clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

            StatsLogger logger = statsProvider.getStatsLogger("ds");
            try (HDBClient client1 = new HDBClient(clientConfiguration, logger)) {
                try (HDBConnection connection = client1.openConnection()) {

                    // create table and insert data
                    connection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLE ttt.t1(k1 int primary key, n1 int)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(1,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(2,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(3,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());

                    // flush data pages to BK
                    server_1.getManager().checkpoint();

                    Set<Long> initialLedgers = new HashSet<>();
                    // verify that every ledger has ensemble size 2
                    try (BookKeeper bk = createBookKeeper();) {
                        BookKeeperAdmin admin = new BookKeeperAdmin(bk);
                        for (long lId : admin.listLedgers()) {
                            LedgerMetadata md = bk.getLedgerManager().readLedgerMetadata(lId).get().getValue();
                            if ("ttt".equals(new String(md.getCustomMetadata().get("tablespaceuuid"), StandardCharsets.UTF_8))) {
                                assertEquals(2, md.getEnsembleSize());
                                assertEquals(2, md.getWriteQuorumSize());
                                assertEquals(2, md.getAckQuorumSize());
                                initialLedgers.add(lId);
                            }
                        }
                    }

                    BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager("ttt").getLog();
                    final long currentLedgerId = log.getWriter().getLedgerId();

                    // downsize to expectedreplicacount = 1
                    TestUtils.execute(server_1.getManager(),
                            "ALTER TABLESPACE 'ttt','leader:" + server_1.getNodeId() + "','expectedreplicacount:1'",
                            Collections.emptyList());

                    // the TableSpaceManager will roll a new ledger
                    herddb.utils.TestUtils.waitForCondition(() -> {
                        if (log.getWriter() == null) {
                            return false;
                        }
                        long newLedgerId = log.getWriter().getLedgerId();
                        return newLedgerId != currentLedgerId;
                    }, herddb.utils.TestUtils.NOOP, 100);

                    // write some other record
                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(4,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());

                    // flush data pages
                    server_1.getManager().checkpoint();

                    // verify that every ledger has ensemble size 2 or 1
                    try (BookKeeper bk = createBookKeeper();) {
                        BookKeeperAdmin admin = new BookKeeperAdmin(bk);
                        for (long lId : admin.listLedgers()) {
                            LedgerMetadata md = bk.getLedgerManager().readLedgerMetadata(lId).get().getValue();
                            if ("ttt".equals(new String(md.getCustomMetadata().get("tablespaceuuid"), StandardCharsets.UTF_8))) {
                                if (initialLedgers.contains(lId)) {
                                    assertEquals(2, md.getEnsembleSize());
                                    assertEquals(2, md.getWriteQuorumSize());
                                    assertEquals(2, md.getAckQuorumSize());
                                } else {
                                    assertEquals(1, md.getEnsembleSize());
                                    assertEquals(1, md.getWriteQuorumSize());
                                    assertEquals(1, md.getAckQuorumSize());
                                }
                            }
                        }
                    }

                }
            }
        }
    }

    protected BookKeeper createBookKeeper() throws Exception {
        org.apache.bookkeeper.conf.ClientConfiguration clientConfiguration = new org.apache.bookkeeper.conf.ClientConfiguration();
        clientConfiguration.setZkServers(testEnv.getAddress());
        clientConfiguration.setZkLedgersRootPath(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
        return BookKeeper.forConfig(clientConfiguration).build();
    }
}
