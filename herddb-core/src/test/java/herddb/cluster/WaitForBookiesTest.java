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
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.TestUtils;
import herddb.utils.ZKTestEnv;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Booting two servers, one table space, bookies are not available at cluster bootstrap.
 *
 * @author enrico.olivelli
 */
public class WaitForBookiesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        // start only one bookie
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testBootstrapClusterWaitForBookies() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT, 2);

        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_WAIT_CLUSTER_READY_TIMEOUT, 0);

        try (Server server_1 = new Server(serverconfig_1)) {
            // boot should fail, as we need two bookies
            server_1.start();
            server_1.getManager().setActivatorPauseStatus(true);
            Exception err = TestUtils.expectThrows(Exception.class, () -> {
                server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, 5000, true);
            });
            assertEquals("TableSpace " + TableSpace.DEFAULT + " not started within 5000 ms", err.getMessage());
        }

        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_WAIT_CLUSTER_READY_TIMEOUT, 10000);
        try (Server server_1 = new Server(serverconfig_1)) {                         
            server_1.start();
            // add a second bookie after starting the server
            testEnv.startNewBookie();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, 5000, true);
                       
            // verify server is working at it is able to write to the WAL           
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try ( DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
            }

        }

    }

}
