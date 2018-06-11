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
package herddb.benchs.simple;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import herddb.benchs.BaseBench;
import herddb.benchs.UpdateByPKOperation;
import herddb.server.ServerConfiguration;

/**
 * Simple concurrent reads and writes on a single table
 *
 * @author enrico.olivelli
 */
public class ConcurrentUpdatesBookKeeperTest extends BaseBench {

    public ConcurrentUpdatesBookKeeperTest() {
        super(20,
            1000000,
            100000,
            2);
        addOperation(new UpdateByPKOperation());
    }

    private static TestingServer zooKeeperServer;

    @BeforeClass
    public static void startZooKeeper() throws Exception {
        zooKeeperServer = new TestingServer(true);
    }

    @AfterClass
    public static void stopZooKeeper() throws Exception {
        if (zooKeeperServer != null) {
            zooKeeperServer.close();
        }
    }

    @Override
    protected void makeServerConfiguration() throws IOException {
        super.makeServerConfiguration();
        serverConfiguration.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverConfiguration.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, zooKeeperServer.getConnectString());
        serverConfiguration.set(ServerConfiguration.PROPERTY_BOOKKEEPER_START, true);
        serverConfiguration.set("bookie.allowLoopback", true);
    }

    @Test
    public void run() throws Exception {
        generateData();
        performOperations();
        waitForResults();
        restartServer();
    }

}
