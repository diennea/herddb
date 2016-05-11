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

import herddb.client.ClientConfiguration;
import herddb.client.HDBConnection;
import herddb.client.HDBClient;
import herddb.model.TableSpace;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic server/client boot test
 *
 * @author enrico.olivelli
 */
public class SimpleClientScanTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, Collections.emptyList());
                Assert.assertEquals(1, resultCreateTable);

                for (int i = 0; i < 99; i++) {
                    Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, Arrays.asList("test_" + i, 1, 2)));
                }

                assertEquals(99, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", Collections.emptyList(), 0).consume().size());

                // maxRows
                assertEquals(17, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", Collections.emptyList(), 17).consume().size());

                // empty result set                
                assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable WHERE id='none'", Collections.emptyList(), 0).consume().size());

                // single fetch result test
                assertEquals(1, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable WHERE id='test_1'", Collections.emptyList(), 0).consume().size());

            }
        }
    }

}
