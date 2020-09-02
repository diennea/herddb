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

import static org.junit.Assert.assertEquals;
import herddb.core.TestUtils;
import herddb.model.TableSpace;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import herddb.utils.ZKTestEnv;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DisklessClusterBootReplicatedTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
        testEnv.startNewBookie();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testReplicaCountForDefaultTableSpace() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT, 2);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        String nodeId1;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);
            nodeId1 = server_1.getNodeId();

            DataAccessor tablespace = TestUtils.scan(server_1.getManager(), "SELECT * FROM systablespaces", Collections.emptyList()).consumeAndClose().get(0);
            assertEquals(60_000L, (long) tablespace.get("maxleaderinactivitytime"));
            assertEquals(RawString.of("*"), tablespace.get("replica"));
            assertEquals(RawString.of(server_1.getNodeId()), tablespace.get("leader"));
            assertEquals(2, tablespace.get("expectedreplicacount"));

            TestUtils.execute(server_1.getManager(), "CREATE TABLE tt(n1 string primary key, n2 int)", Collections.emptyList());
            TestUtils.execute(server_1.getManager(), "CREATE INDEX aa ON tt(n2)", Collections.emptyList());
            TestUtils.execute(server_1.getManager(), "INSERT INTO tt(n1,n2) values('a',1)", Collections.emptyList());

            server_1.getManager().checkpoint();
        }
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, true);
        }
    }

}
