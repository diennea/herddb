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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.model.TableSpace;

/**
 * Basic server/client boot test
 *
 * @author enrico.olivelli
 */
public class RollbackOnCloseConnectionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.getNetworkServer().setEnableJVMNetwork(false);
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long tx;

                try (HDBConnection connection = client.openConnection()) {
                    long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                    Assert.assertEquals(1, resultCreateTable);
                    tx = connection.beginTransaction(TableSpace.DEFAULT);
                    long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList("test", 1, 2)).updateCount;
                    Assert.assertEquals(1, countInsert);
                    assertTrue(server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getOpenTransactions().contains(tx));
                }
                
                for (int i = 0; i < 100; i++) {
                    if (!server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getOpenTransactions().contains(tx)) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertFalse(server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getOpenTransactions().contains(tx));

            }
        }
    }
}
