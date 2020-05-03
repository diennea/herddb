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
package herddb.cluster.bookkeeper;

import herddb.cluster.BookKeeperDataStorageManager;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.DBManager;
import herddb.core.RestartTestBase;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.nio.file.Path;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;

public class BookKeeperDataStorageManagerRestartTest extends RestartTestBase {

    ZKTestEnv testEnv;

    @Before
    public void setup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder("zk").toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void stop() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
        testEnv = null;
    }

    protected DBManager buildDBManager(String nodeId, Path metadataPath, Path dataPath, Path logsPath, Path tmoDir) {
        ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
                testEnv.getTimeout(), testEnv.getPath());
        ServerConfiguration serverConfiguration = new ServerConfiguration();
        BookkeeperCommitLogManager logManager = new BookkeeperCommitLogManager(man, serverConfiguration, NullStatsLogger.INSTANCE);
        BookKeeperDataStorageManager dataManager = new BookKeeperDataStorageManager(tmoDir, man, logManager);
        return new DBManager(nodeId, man, dataManager, logManager, tmoDir, null);
    }

}
