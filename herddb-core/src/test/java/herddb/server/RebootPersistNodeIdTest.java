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

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Demonstates the usage of the update "newvalue" facility to implement atomic-counters
 *
 * @author enrico.olivelli
 */
public class RebootPersistNodeIdTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        String nodeId;

        Path dataPath = folder.newFolder().toPath();
        {
            ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(dataPath);
            assertTrue(serverConfiguration.getString(ServerConfiguration.PROPERTY_NODEID, "").isEmpty());
            try (Server server = new Server(serverConfiguration)) {
                server.start();
                nodeId = server.getNodeId();
            }
        }
        {
            ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(dataPath);
            assertTrue(serverConfiguration.getString(ServerConfiguration.PROPERTY_NODEID, "").isEmpty());
            try (Server server = new Server(serverConfiguration)) {
                server.start();
                assertEquals(nodeId, server.getNodeId());
            }
        }
    }
}
