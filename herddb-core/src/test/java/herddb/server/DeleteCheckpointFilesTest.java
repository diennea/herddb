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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.core.TestUtils;
import herddb.model.DataScanner;
import herddb.model.TableSpace;

/**
 * Tests on clear at boot
 *
 * @author enrico.olivelli
 */
public class DeleteCheckpointFilesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        ServerConfiguration serverConfiguration = new ServerConfiguration(folder.newFolder().toPath());
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                server.waitForStandaloneBoot();
                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                    "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long resultCreateIndex = connection.executeUpdate(TableSpace.DEFAULT,
                    "CREATE HASH INDEX myhashindex on mytable (n2)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateIndex);

                long resultCreateIndex2 = connection.executeUpdate(TableSpace.DEFAULT,
                    "CREATE BRIN INDEX mybrinindex on mytable (n1)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateIndex2);

                Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, false, Arrays.asList("test_0", 1, 2)).updateCount);
                Map<String, Object> newValue = connection.executeUpdate(TableSpace.DEFAULT, "UPDATE mytable set n1= n1+1 where id=?", 0, true, Arrays.asList("test_0")).newvalue;
                assertEquals(Long.valueOf(2), newValue.get("n1"));

                server.getManager().checkpoint();
                server.getManager().checkpoint();
                Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, false, Arrays.asList("test_1", 2, 2)).updateCount);

                server.getManager().checkpoint();

                try (DataScanner scan = TestUtils.scan(server.getManager(), "SELECT * FROM mytable WHERE n2 = ?", Arrays.asList(2));) {
                    assertEquals(2, scan.consume().size());
                }

                try (DataScanner scan = TestUtils.scan(server.getManager(), "SELECT * FROM mytable WHERE n1 = ?", Arrays.asList(2));) {
                    assertEquals(2, scan.consume().size());
                }

            }
        }

        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();

            try (DataScanner scan = TestUtils.scan(server.getManager(), "SELECT * FROM mytable WHERE n2 = ?", Arrays.asList(2));) {
                assertEquals(2, scan.consume().size());
            }

            try (DataScanner scan = TestUtils.scan(server.getManager(), "SELECT * FROM mytable WHERE n1 = ?", Arrays.asList(2));) {
                assertEquals(2, scan.consume().size());
            }
        }
    }
}
