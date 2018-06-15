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
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.DMLResult;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.model.TableSpace;
import herddb.utils.RawString;
import static org.junit.Assert.assertTrue;

/**
 * Basic server/client boot test
 *
 * @author enrico.olivelli
 */
public class SimpleClientServerTest {
//
//         @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.FINEST;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter();
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();;
        String _baseDir = baseDir.toString();
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, 100));                

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, Arrays.asList("test", 1, 2)).updateCount;
                Assert.assertEquals(1, countInsert);
                long countInsert2 = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, Arrays.asList("test2", 2, 3)).updateCount;
                Assert.assertEquals(1, countInsert2);

                GetResult res = connection.executeGet(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'", tx, Collections.emptyList());
                Map<RawString, Object> record = res.data;
                Assert.assertNotNull(record);
                
                assertEquals("test", record.get(RawString.of("id")));
                assertEquals(Long.valueOf(1), record.get(RawString.of("n1")));
                assertEquals(Integer.valueOf(2), record.get(RawString.of("n2")));

                List<DMLResult> executeUpdates = connection.executeUpdates(TableSpace.DEFAULT,
                        "UPDATE mytable set n2=? WHERE id=?", tx,
                        false,
                        Arrays.asList(
                                Arrays.asList(1, "test"),
                                Arrays.asList(2, "test2"),
                                Arrays.asList(3, "test_not_exists")
                        )
                );
                assertEquals(3, executeUpdates.size());
                assertEquals(1, executeUpdates.get(0).updateCount);
                assertEquals(1, executeUpdates.get(1).updateCount);
                assertEquals(0, executeUpdates.get(2).updateCount);
                assertEquals(tx, executeUpdates.get(0).transactionId);
                assertEquals(tx, executeUpdates.get(1).transactionId);
                assertEquals(tx, executeUpdates.get(2).transactionId);
                connection.commitTransaction(TableSpace.DEFAULT, tx);

                try (ScanResultSet scan = connection.executeScan(server.getManager().getVirtualTableSpaceId(), "SELECT * FROM sysconfig", Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        String name = (String) aa.get("name");
                        assertEquals("server.base.dir", name);
                        String value = (String) aa.get("value");
                        assertEquals(_baseDir, value);
                    }
                }

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM " + server.getManager().getVirtualTableSpaceId() + ".sysclients", Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {

                        assertEquals("jvm-local", aa.get("address"));
                        assertNotNull(aa.get("id"));
                        assertEquals(ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT, aa.get("username"));
                        assertNotNull(aa.get("connectionts"));
                    }
                    assertEquals(1, all.size());
                }

            }
        }
    }
}
