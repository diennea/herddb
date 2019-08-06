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
import static org.junit.Assert.assertFalse;
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
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.utils.RawString;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import herddb.client.RoutedClientSideConnection;
import herddb.core.TableSpaceManager;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

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
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true,
                        Collections.emptyList()).updateCount;
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

                try (ScanResultSet scan = connection.executeScan(server.getManager().getVirtualTableSpaceId(),
                        "SELECT * FROM sysconfig", true, Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        RawString name = (RawString) aa.get("name");
                        assertEquals(RawString.of("server.base.dir"), name);
                        RawString value = (RawString) aa.get("value");
                        assertEquals(RawString.of(_baseDir), value);
                    }
                }

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM " + server.getManager().
                        getVirtualTableSpaceId() + ".sysclients", true, Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {

                        assertEquals(RawString.of("jvm-local"), aa.get("address"));
                        assertNotNull(aa.get("id"));
                        assertEquals(RawString.of(ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT), aa.get(
                                "username"));
                        assertNotNull(aa.get("connectionts"));
                    }
                    assertTrue(all.size() >= 1);
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
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", TransactionContext.NOTRANSACTION_ID, false,
                            true, Arrays.asList("test"));
                    fail("cannot issue a query without setting each parameter");
                } catch (HDBException ok) {
                    assertTrue(ok.getMessage().contains(MissingJDBCParameterException.class.getName()));
                }

                // simple rollback
                long txToRollback = connection.beginTransaction(TableSpace.DEFAULT);
                long countInsertToRollback = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", txToRollback, false, true, Arrays.asList(
                                "test123", 7, 8)).updateCount;
                Assert.assertEquals(1, countInsertToRollback);
                connection.rollbackTransaction(TableSpace.DEFAULT, txToRollback);
            }
        }
    }

    @Test
    public void testCachePreparedStatementsRestartServer() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
        clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
        try (HDBClient client = new HDBClient(clientConfiguration);) {
            try (HDBConnection connection1 = client.openConnection()) {

                try (Server server = new Server(new ServerConfiguration(baseDir))) {
                    server.start();
                    server.waitForStandaloneBoot();
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    assertTrue(connection1.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                    // this is 1 for the client and for the server
                    long resultCreateTable = connection1.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true /*usePreparedStatement*/,
                            Collections.emptyList()).updateCount;
                    Assert.assertEquals(1, resultCreateTable);

                    // this is 2 for the client and for the server
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10).close();

                    // this is 3 for the client and for the server
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT id FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10).close();
                }

                try (Server server = new Server(new ServerConfiguration(baseDir))) {
                    server.start();
                    server.waitForStandaloneBoot();
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    assertTrue(connection1.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                    // this is 1 for the server, the client will invalidate its cache for this statement
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT n1 FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10).close();

                    // this is 2 for the server
                    try (HDBConnection connection2 = client.openConnection()) {
                        connection2.executeUpdate(TableSpace.DEFAULT,
                                "UPDATE mytable set n1=2", 0, false, true /*usePreparedStatement*/,
                                Collections.emptyList());
                    }

                    // this would be 2 for connection1 (bug in 0.10.0), but for the server 2 is "UPDATE mytable set n1=2"
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10).close();

                }
            }
        }

    }

    /**
     * Testing that if the server discards the query the client will resend the PREPARE_STATEMENT COMMAND
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
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true,
                        Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                {
                    long tx = connection.beginTransaction(TableSpace.DEFAULT);
                    long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.asList("test", 1, 2)).updateCount;
                    Assert.assertEquals(1, countInsert);
                    long countInsert2 = connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false, true, Arrays.
                                    asList("test2", 2, 3)).updateCount;
                    Assert.assertEquals(1, countInsert2);
                    connection.commitTransaction(TableSpace.DEFAULT, tx);
                }

                // SCAN
                {

                    try (ScanResultSet res = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", true, Collections.emptyList(),
                            TransactionContext.NOTRANSACTION_ID, 100, 100);) {
                        assertEquals(1, res.consume().size());
                    }
                }
                server.getManager().getPreparedStatementsCache().clear();
                {

                    try (ScanResultSet res = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", true, Collections.emptyList(),
                            TransactionContext.NOTRANSACTION_ID, 100, 100);) {
                        assertEquals(1, res.consume().size());
                    }
                }

                // GET
                {
                    GetResult res = connection.executeGet(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", TransactionContext.NOTRANSACTION_ID, true,
                            Collections.emptyList());
                    Map<RawString, Object> record = res.data;
                    Assert.assertNotNull(record);

                    assertEquals(RawString.of("test"), record.get(RawString.of("id")));
                    assertEquals(Long.valueOf(1), record.get(RawString.of("n1")));
                    assertEquals(Integer.valueOf(2), record.get(RawString.of("n2")));
                }

                server.getManager().getPreparedStatementsCache().clear();
                {

                    GetResult res = connection.executeGet(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", TransactionContext.NOTRANSACTION_ID, true,
                            Collections.emptyList());
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
                            "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)",
                            TransactionContext.NOTRANSACTION_ID,
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

    @Test
    public void testSQLIntegrityViolation() throws Exception {
        Path baseDir = folder.newFolder().toPath();;
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id int primary key, s1 string)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                long countInsert = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,s1) values(?,?)", tx, false, true, Arrays.asList(1, "test1")).updateCount;
                Assert.assertEquals(1, countInsert);
                try {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,s1) values(?,?)", tx, false, true, Arrays.asList(1, "test2"));
                } catch (Exception ex) {
                    Assert.assertTrue(ex.getMessage().contains("SQLIntegrityConstraintViolationException"));
                }
                connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,s1) values(?,?)", tx, false, true, Arrays.asList(2, "test2"));

                connection.commitTransaction(TableSpace.DEFAULT, tx);

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM herd.mytable", true, Collections.
                        emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> rows = scan.consume();
                    int i = 0;
                    for (Map<String, Object> row : rows) {
                        if (i == 0) {
                            i++;
                            Assert.assertEquals(row.get("id"), 1);
                            Assert.assertEquals(row.get("s1"), "test1");
                        } else {
                            Assert.assertEquals(row.get("id"), 2);
                            Assert.assertEquals(row.get("s1"), "test2");
                        }
                    }
                }

            }
        }
    }
    private static final Logger LOG = Logger.getLogger(SimpleClientServerTest.class.getName());

    @Test
    public void testClientCloseOnConnectionAndResumeTransaction() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        AtomicInteger connectionToUse = new AtomicInteger();
        AtomicReference<RoutedClientSideConnection[]> connections = new AtomicReference<>();
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 2);
            clientConfiguration.set(ClientConfiguration.PROPERTY_TIMEOUT, 2000);
            try (HDBClient client = new HDBClient(clientConfiguration) {
                @Override
                public HDBConnection openConnection() {
                    HDBConnection con = new HDBConnection(this) {
                        @Override
                        protected RoutedClientSideConnection chooseConnection(RoutedClientSideConnection[] all) {
                            connections.set(all);
                            LOG.log(Level.INFO,
                                    "chooseConnection among " + all.length + " connections, getting " + connectionToUse);
                            return all[connectionToUse.get()];
                        }

                    };
                    registerConnection(con);
                    return con;
                }

            };
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                // force using the first connection of two
                connectionToUse.set(0);
                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id int primary key, s1 string)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                // transaction is bound to the first connection (in HerdDB 11.0.0)
                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,s1) values(?,?)", tx, false, true, Arrays.asList(1, "test1")).updateCount);

                // close the connection that initiated the transaction
                connections.get()[0].close();

                // give time to the server to close the connection
                Thread.sleep(100);

                // use the second connection, with the same transaction
                connectionToUse.set(1);

                connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,s1) values(?,?)", tx,
                        false, true,
                        Arrays.asList(2, "test1"));

            }
        }
    }

    @Test
    public void testClientAbandonedTransaction() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration config = new ServerConfiguration(baseDir);
        config.set(ServerConfiguration.PROPERTY_ABANDONED_TRANSACTIONS_TIMEOUT, 5000);
        try (Server server = new Server(config)) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id int primary key, s1 string)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,s1) values(?,?)", tx, false, true, Arrays.asList(1, "test1")).updateCount);

                assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                        "UPDATE mytable set s1=?", tx, false, true, Arrays.asList("test2")).updateCount);
                // the client dies, it won't use the transaction any more
            }

            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);

            assertFalse(tableSpaceManager.getTransactions().isEmpty());

            Thread.sleep(6000); // PROPERTY_ABANDONED_TRANSACTIONS_TIMEOUT+ 1000

            server.getManager().checkpoint();

            assertTrue(tableSpaceManager.getTransactions().isEmpty());

            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertEquals(0, connection.executeUpdate(TableSpace.DEFAULT,
                        "UPDATE mytable set s1=?", TransactionContext.NOTRANSACTION_ID, false, true, Arrays.asList(
                                "test3")).updateCount);
            }

        }
    }

}
