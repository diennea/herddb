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

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.util.Collections;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests about retrying queries in case of network address change.
 *
 * @author enrico.olivelli
 */
public class ServerWithDynamicPortTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerWithDynamicPortTest.class.getName());

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
    public void test() throws Exception {

        TestStatsProvider statsProvider = new TestStatsProvider();

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
        clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        StatsLogger logger = statsProvider.getStatsLogger("ds");
        try (HDBClient client1 = new HDBClient(clientConfiguration, logger)) {
            try (HDBConnection connection = client1.openConnection()) {

                try (Server server_1 = new Server(serverconfig_1)) {
                    server_1.start();
                    server_1.waitForStandaloneBoot();

                    // create table and insert data
                    connection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLE t1(k1 int primary key, n1 int)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO t1(k1,n1) values(1,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                }

                // change port
                serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7868);
                try (Server server_1 = new Server(serverconfig_1)) {
                    server_1.start();
                    server_1.waitForStandaloneBoot();

                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO t1(k1,n1) values(2,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO t1(k1,n1) values(3,1)",
                            TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                }
            }

        }
    }
}
