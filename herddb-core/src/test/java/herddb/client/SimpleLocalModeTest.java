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
package herddb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic server/client local mode boot test
 *
 * @author enrico.olivelli
 */
public class SimpleLocalModeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(new ServerConfiguration(baseDir)
                .set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL))) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath())
                    .set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_LOCAL);
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true,
                        Collections.emptyList()).updateCount;
                assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?),(?,?,?)", tx, false, true, Arrays.asList("test", 1, 2, "test2", 2, 3)).updateCount;
                assertEquals(2, countInsert);

                GetResult res = connection.executeGet(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'", tx, true, Collections.emptyList());
                Map<RawString, Object> record = res.data;
                assertNotNull(record);
                assertEquals(tx, res.transactionId);

                GetResult res2 = connection.executeGet(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test_not_exists'", tx, true, Collections.emptyList());
                assertNull(res2.data);
                assertEquals(tx, res2.transactionId);

                connection.rollbackTransaction(TableSpace.DEFAULT, tx);

                res = connection.executeGet(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'", TransactionContext.AUTOTRANSACTION_ID, true, Collections.emptyList());
                assertNull(res.data);
                assertTrue(res.transactionId > 0);
                connection.rollbackTransaction(TableSpace.DEFAULT, res.transactionId);

                tx = connection.beginTransaction(TableSpace.DEFAULT);
                int countStatements = connection.executeUpdates(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList(Arrays.asList("test", 1, 2), Arrays.asList("test2", 2, 3))).size();
                assertEquals(2, countStatements);

                // fetch size = 1, we are going to issue two requests to the server in order to download the full resultset
                try (ScanResultSet scan = connection
                        .executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable where id in ('test','test2') order by id", true, Arrays.asList(), tx, -1, 1, true);) {
                    List<Map<String, Object>> results = scan.consume();
                    assertEquals(2, results.size());
                    assertEquals(RawString.of("test"), results.get(0).get("id"));
                    assertEquals(RawString.of("test2"), results.get(1).get("id"));
                }

            }
        }
    }

}
