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

package herddb.core;

import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.file.FileDataStorageManager;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.io.File;
import java.nio.file.Path;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Tests using multiple nodes
 *
 * @author enrico.olivelli
 */
public abstract class ReplicatedLogtestcase {

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

    protected DBManager startDBManager(String nodeId) throws Exception {
        File nodeDirectory = new File(folder.getRoot(), nodeId + "");
        nodeDirectory.mkdirs();
        Path path = nodeDirectory.toPath();
        ZookeeperMetadataStorageManager metadataStorageManager = new ZookeeperMetadataStorageManager(testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
        BookkeeperCommitLogManager commitLogManager = new BookkeeperCommitLogManager(metadataStorageManager, new ServerConfiguration(), new NullStatsLogger());
        FileDataStorageManager dataStorageManager = new FileDataStorageManager(path);
        System.setErr(System.out);
        DBManager manager = new DBManager(nodeId, metadataStorageManager, dataStorageManager, commitLogManager, folder.newFolder().toPath(), null);
        manager.start();
        return manager;
    }

}
