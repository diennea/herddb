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
import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.model.MissingJDBCParameterException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.utils.RawString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList("test", 1, 2)).updateCount;
                Assert.assertEquals(1, countInsert);
                long countInsert2 = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList("test2", 2, 3)).updateCount;
                Assert.assertEquals(1, countInsert2);

                {
                    GetResult res = connection.executeGet(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", tx, true, Collections.emptyList());
                    Map<RawString, Object> record = res.data;
                    Assert.assertNotNull(record);

                    assertEquals(RawString.of("test"), record.get(RawString.of("id")));
                    assertEquals(Long.valueOf(1), record.get(RawString.of("n1")));
                    assertEquals(Integer.valueOf(2), record.get(RawString.of("n2")));
                }

                {
                    server.getManager().getPreparedStatementsCache().clear();
                    GetResult res = connection.executeGet(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", tx, true, Collections.emptyList());
                    Map<RawString, Object> record = res.data;
                    Assert.assertNotNull(record);
                }

                List<DMLResult> executeUpdates = connection.executeUpdates(TableSpace.DEFAULT,
                        "UPDATE mytable set n2=? WHERE id=?", tx,
                        false,
                        true,
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

                try (ScanResultSet scan = connection.executeScan(server.getManager().getVirtualTableSpaceId(), "SELECT * FROM sysconfig", true, Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        RawString name = (RawString) aa.get("name");
                        assertEquals(RawString.of("server.base.dir"), name);
                        RawString value = (RawString) aa.get("value");
                        assertEquals(RawString.of(_baseDir), value);
                    }
                }

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM " + server.getManager().getVirtualTableSpaceId() + ".sysclients", true, Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {

                        assertEquals(RawString.of("jvm-local"), aa.get("address"));
                        assertNotNull(aa.get("id"));
                        assertEquals(RawString.of(ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT), aa.get("username"));
                        assertNotNull(aa.get("connectionts"));
                    }
                    assertEquals(1, all.size());
                }

                List<DMLResult> executeUpdatesWithoutParams = connection.executeUpdates(TableSpace.DEFAULT,
                        "UPDATE mytable set n2=1 WHERE id='test'", -1,
                        false, true,
                        Arrays.asList(
                                // empty list of parameters
                                Arrays.asList()
                        )
                );
                assertEquals(1, executeUpdatesWithoutParams.size());
                assertEquals(1, executeUpdatesWithoutParams.get(0).updateCount);

                // missing JDBC parameter
                try {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", TransactionContext.NOTRANSACTION_ID, false, true, Arrays.asList("test"));
                    fail("cannot issue a query without setting each parameter");
                } catch (HDBException ok) {
                    assertTrue(ok.getMessage().contains(MissingJDBCParameterException.class.getName()));
                }

            }
        }
    }

    /**
     * Testing that if the server discards the query the client will resend the
     * PREPARE_STATEMENT COMMAND
     *
     * @throws Exception
     */
    @Test
    public void testCachePreparedStatementsPrepareSqlAgain() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                {
                    long tx = connection.beginTransaction(TableSpace.DEFAULT);
                    long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList("test", 1, 2)).updateCount;
                    Assert.assertEquals(1, countInsert);
                    long countInsert2 = connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList("test2", 2, 3)).updateCount;
                    Assert.assertEquals(1, countInsert2);
                    connection.commitTransaction(TableSpace.DEFAULT, tx);
                }

                // SCAN
                {

                    try (ScanResultSet res = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", true, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 100, 100);) {
                        assertEquals(1, res.consume().size());
                    }
                }
                server.getManager().getPreparedStatementsCache().clear();
                {

                    try (ScanResultSet res = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", true, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 100, 100);) {
                        assertEquals(1, res.consume().size());
                    }
                }

                // GET
                {
                    GetResult res = connection.executeGet(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", TransactionContext.NOTRANSACTION_ID, true, Collections.emptyList());
                    Map<RawString, Object> record = res.data;
                    Assert.assertNotNull(record);

                    assertEquals(RawString.of("test"), record.get(RawString.of("id")));
                    assertEquals(Long.valueOf(1), record.get(RawString.of("n1")));
                    assertEquals(Integer.valueOf(2), record.get(RawString.of("n2")));
                }

                server.getManager().getPreparedStatementsCache().clear();
                {

                    GetResult res = connection.executeGet(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", TransactionContext.NOTRANSACTION_ID, true, Collections.emptyList());
                    Map<RawString, Object> record = res.data;
                    Assert.assertNotNull(record);
                }

                // EXECUTE UPDATES
                {
                    List<DMLResult> executeUpdates = connection.executeUpdates(TableSpace.DEFAULT,
                            "UPDATE mytable set n2=? WHERE id=?", TransactionContext.NOTRANSACTION_ID,
                            false,
                            true,
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

                }
                server.getManager().getPreparedStatementsCache().clear();
                {
                    List<DMLResult> executeUpdates = connection.executeUpdates(TableSpace.DEFAULT,
                            "UPDATE mytable set n2=? WHERE id=?", TransactionContext.NOTRANSACTION_ID,
                            false,
                            true,
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
                }
                // EXECUTE UPDATE

                {
                    DMLResult executeUpdate = connection.executeUpdate(TableSpace.DEFAULT,
                            "UPDATE mytable set n2=? WHERE id=?", TransactionContext.NOTRANSACTION_ID,
                            false,
                            true,
                            Arrays.asList(1, "test")
                    );

                    assertEquals(1, executeUpdate.updateCount);

                }
                server.getManager().getPreparedStatementsCache().clear();
                {
                    DMLResult executeUpdate = connection.executeUpdate(TableSpace.DEFAULT,
                            "UPDATE mytable set n2=? WHERE id=?", TransactionContext.NOTRANSACTION_ID,
                            false,
                            true,
                            Arrays.asList(1, "test")
                    );

                    assertEquals(1, executeUpdate.updateCount);
                }

            }
        }
    }

    @Test
    public void testExecuteUpdatesWithDDL() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                {

                    List<DMLResult> executeUpdates = connection.executeUpdates(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", TransactionContext.NOTRANSACTION_ID,
                            false,
                            true,
                            Arrays.asList(
                                    Collections.emptyList()
                            )
                    );
                    assertEquals(1, executeUpdates.size());
                    assertEquals(1, executeUpdates.get(0).updateCount);

                }

                {

                    List<DMLResult> executeUpdates = connection.executeUpdates(TableSpace.DEFAULT,
                            "DROP TABLE mytable", TransactionContext.NOTRANSACTION_ID,
                            false,
                            true,
                            Arrays.asList(
                                    Collections.emptyList()
                            )
                    );
                    assertEquals(1, executeUpdates.size());
                    assertEquals(1, executeUpdates.get(0).updateCount);

                }

            }
        }
    }
}
