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
package herddb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.client.impl.UnreachableServerException;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.network.ServerHostData;
import herddb.utils.TestUtils;
import herddb.utils.ZKTestEnv;
import java.io.IOException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZookeeperClientSideMetadataProviderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void test() throws Exception {
        try (ZookeeperMetadataStorageManager server = new ZookeeperMetadataStorageManager(testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
                ZookeeperClientSideMetadataProvider prov = new ZookeeperClientSideMetadataProvider(testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());) {
            server.start();
            assertTrue(server.ensureDefaultTableSpace("node1", "node1", 5000, 1));
            server.registerNode(NodeMetadata
                    .builder()
                    .host("test-node")
                    .port(1234)
                    .ssl(true)
                    .nodeId("node1")
                    .build());

            ServerHostData currentData = prov.getServerHostData("node1");
            assertEquals("test-node", currentData.getHost());
            assertEquals(1234, currentData.getPort());
            assertTrue(currentData.isSsl());

            assertEquals("node1", prov.getTableSpaceLeader(TableSpace.DEFAULT));

            server.registerNode(NodeMetadata
                    .builder()
                    .host("test-node-newhost")
                    .port(1234)
                    .ssl(true)
                    .nodeId("node1")
                    .build());

            prov.requestMetadataRefresh(new UnreachableServerException("error", new IOException(), "node1"));
            assertEquals("node1", prov.getTableSpaceLeader(TableSpace.DEFAULT));

            currentData = prov.getServerHostData("node1");
            assertEquals("test-node-newhost", currentData.getHost());
            assertEquals(1234, currentData.getPort());
            assertTrue(currentData.isSsl());

            prov.requestMetadataRefresh(new Exception());

            currentData = prov.getServerHostData("node1");
            assertEquals("test-node-newhost", currentData.getHost());
            assertEquals(1234, currentData.getPort());
            assertTrue(currentData.isSsl());
            assertEquals("node1", prov.getTableSpaceLeader(TableSpace.DEFAULT));

            final ZooKeeper currentZooKeeper = prov.getZooKeeper();

            // expire session
            currentZooKeeper.getTestable().injectSessionExpiration();

            // wait for a new handle to be created
            TestUtils.waitForCondition(() -> {
                ZooKeeper zk = prov.getZooKeeper();
                return zk != currentZooKeeper;
            }, TestUtils.NOOP, 100);

            // clean cache
            prov.requestMetadataRefresh(new UnreachableServerException("error", new IOException(), "node1"));

            assertEquals("node1", prov.getTableSpaceLeader(TableSpace.DEFAULT));
            prov.getServerHostData("node1");
            assertEquals("test-node-newhost", currentData.getHost());
            assertEquals(1234, currentData.getPort());
            assertTrue(currentData.isSsl());
            assertEquals("node1", prov.getTableSpaceLeader(TableSpace.DEFAULT));


        }
    }

}
