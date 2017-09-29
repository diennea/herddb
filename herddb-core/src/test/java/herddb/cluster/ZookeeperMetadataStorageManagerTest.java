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

import herddb.metadata.MetadataStorageManagerException;
import herddb.model.TableSpace;
import herddb.utils.ZKTestEnv;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZookeeperMetadataStorageManagerTest {

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
    public void testSessionExpired() throws Exception {
        try (ZookeeperMetadataStorageManager man = new ZookeeperMetadataStorageManager(testEnv.getAddress(),
            testEnv.getTimeout(), testEnv.getPath());) {
            man.start();

            TableSpace tableSpace = TableSpace
                .builder()
                .leader("test")
                .replica("test")
                .name(TableSpace.DEFAULT)
                .build();
            man.registerTableSpace(tableSpace);
            assertEquals(1, man.listTableSpaces().size());
            ZooKeeper actual = man.getZooKeeper();
            long sessionId = actual.getSessionId();
            byte[] passwd = actual.getSessionPasswd();
            expireZkSession(sessionId, passwd);
            for (int i = 0; i < 10; i++) {
                try {
                    man.listTableSpaces();
                    fail("session should be expired or not connected");
                } catch (MetadataStorageManagerException ok) {
                    System.out.println("ok: " + ok);
                    assertTrue(ok.getCause() instanceof KeeperException.ConnectionLossException
                        || ok.getCause() instanceof KeeperException.SessionExpiredException);
                    if (ok.getCause() instanceof KeeperException.SessionExpiredException) {
                        break;
                    }
                }
                Thread.sleep(500);
            }
            assertNotSame(actual, man.getZooKeeper());
            assertEquals(1, man.listTableSpaces().size());

            assertNotNull(tableSpace = man.describeTableSpace(TableSpace.DEFAULT));
            man.dropTableSpace(TableSpace.DEFAULT, tableSpace);
        }
    }

    private void expireZkSession(long sessionId, byte[] passwd) throws InterruptedException, IOException {
        CountDownLatch waitConnection = new CountDownLatch(1);
        try (ZooKeeper zooKeeper = new ZooKeeper(testEnv.getAddress(), testEnv.getTimeout(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getState()) {
                    case SyncConnected:
                        waitConnection.countDown();
                        break;
                }
            }
        }, sessionId, passwd)) {
            assertTrue(waitConnection.await(1, TimeUnit.MINUTES));
        }
    }

}
