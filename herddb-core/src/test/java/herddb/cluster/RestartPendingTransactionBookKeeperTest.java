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

import herddb.core.DBManager;
import herddb.core.RestartPendingTransactionBase;
import herddb.file.FileDataStorageManager;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.nio.file.Path;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;

public class RestartPendingTransactionBookKeeperTest extends RestartPendingTransactionBase {

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

    @Override
    protected DBManager buildDBManager(
            String nodeId,
            Path metadataPath,
            Path dataPath,
            Path logsPath,
            Path tmoDir) throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        ZookeeperMetadataStorageManager zk = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
        DBManager manager = new DBManager(nodeId,
                zk,
                new FileDataStorageManager(dataPath),
                new BookkeeperCommitLogManager(zk, serverconfig_1, NullStatsLogger.INSTANCE),
                tmoDir, null);
        return manager;
    }
}
