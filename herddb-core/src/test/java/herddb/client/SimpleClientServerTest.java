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

package herddb.client;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.client.impl.HDBOperationTimeoutException;
import herddb.core.TableSpaceManager;
import herddb.model.MissingJDBCParameterException;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerHostData;
import herddb.network.netty.NettyChannel;
import herddb.proto.Pdu;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.ServerSideConnectionPeer;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.Bytes;
import herddb.utils.RawString;
import herddb.utils.TestUtils;
import io.netty.channel.socket.SocketChannel;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic server/client boot test
 *
 * @author enrico.olivelli
 */
public class SimpleClientServerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        String _baseDir = baseDir.toString();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
                        "SELECT * FROM sysconfig", true, Collections.emptyList(), 0, 0, 10, true)) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        RawString name = (RawString) aa.get("name");
                        if (RawString.of("server.base.dir").equals(name)) {
                            assertEquals(RawString.of("server.base.dir"), name);
                            RawString value = (RawString) aa.get("value");
                            assertEquals(RawString.of(_baseDir), value);
                        } else if (RawString.of("server.port").equals(name)) {
                            RawString value = (RawString) aa.get("value");
                            assertEquals(RawString.of("0"), value);
                        } else {
                            assertEquals(RawString.of("server.node.id"), name);
                        }
                    }
                }

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM " + server.getManager().
                        getVirtualTableSpaceId() + ".sysclients", true, Collections.emptyList(), 0, 0, 10, true)) {
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
        try (HDBClient client = new HDBClient(clientConfiguration)) {
            try (HDBConnection connection1 = client.openConnection()) {

                try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
                            Collections.emptyList(), 0, 0, 10, true).close();

                    // this is 3 for the client and for the server
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT id FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10, true).close();
                }

                try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
                    server.start();
                    server.waitForStandaloneBoot();
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    assertTrue(connection1.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                    // this is 1 for the server, the client will invalidate its cache for this statement
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT n1 FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10, true).close();

                    // this is 2 for the server
                    try (HDBConnection connection2 = client.openConnection()) {
                        connection2.executeUpdate(TableSpace.DEFAULT,
                                "UPDATE mytable set n1=2", 0, false, true /*usePreparedStatement*/,
                                Collections.emptyList());
                    }

                    // this would be 2 for connection1 (bug in 0.10.0), but for the server 2 is "UPDATE mytable set n1=2"
                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable", true /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10, true).close();

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
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
                            TransactionContext.NOTRANSACTION_ID, 100, 100, true)) {
                        assertEquals(1, res.consume().size());
                    }
                }
                server.getManager().getPreparedStatementsCache().clear();
                {

                    try (ScanResultSet res = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable WHERE id='test'", true, Collections.emptyList(),
                            TransactionContext.NOTRANSACTION_ID, 100, 100, true)) {
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
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
                        emptyList(), 0, 0, 10, true)) {
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

    private static final Logger LOG = LoggerFactory.getLogger(SimpleClientServerTest.class.getName());

    @Test
    public void testClientCloseOnConnectionAndResumeTransaction() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        AtomicInteger connectionToUse = new AtomicInteger();
        AtomicReference<ClientSideConnectionPeer[]> connections = new AtomicReference<>();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
                        protected ClientSideConnectionPeer chooseConnection(ClientSideConnectionPeer[] all) {
                            connections.set(all);
                            LOG.info("chooseConnection among " + all.length + " connections, getting " + connectionToUse);
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
        ServerConfiguration config = newServerConfigurationWithAutoPort(baseDir);
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

    @Test
    public void testTimeoutDuringAuth() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration config = newServerConfigurationWithAutoPort(baseDir);
        final AtomicBoolean suspendProcessing = new AtomicBoolean(false);
        try (Server server = new Server(config) {
            @Override
            protected ServerSideConnectionPeer buildPeer(Channel channel) {
                return new ServerSideConnectionPeer(channel, this) {
                    @Override
                    public void requestReceived(Pdu message, Channel channel) {
                        if (suspendProcessing.get()) {
                            LOG.info("dropping message type " + message.type + " id " + message.messageId);
                            message.close();
                            return;
                        }
                        super.requestReceived(message, channel);
                    }

                };
            }

        }) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            clientConfiguration.set(ClientConfiguration.PROPERTY_TIMEOUT, 2000);
            clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);

            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id int primary key, s1 string)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                suspendProcessing.set(true);
                try (HDBConnection connection2 = client.openConnection()) {
                    // auth will timeout
                    try {
                        connection2.executeUpdate(TableSpace.DEFAULT,
                                "INSERT INTO mytable (id,s1) values(?,?)", TransactionContext.NOTRANSACTION_ID,
                                false, true, Arrays.asList(1, "test1"));
                        fail("insertion should fail");
                    } catch (Exception e) {
                        TestUtils.assertExceptionPresentInChain(e, HDBOperationTimeoutException.class);
                    }
                    suspendProcessing.set(false);
                    // new connection
                    assertEquals(1, connection2.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,s1) values(?,?)", TransactionContext.NOTRANSACTION_ID,
                            false, true, Arrays.asList(1, "test1")).updateCount);

                }
            }

        }
    }

    @Test
    public void testSimpleJoinFromNetwork() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration config = newServerConfigurationWithAutoPort(baseDir);
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

                for (int i = 0; i < 10; i++) {
                    assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,s1) values(?,?)", 0, false, true, Arrays.asList(i, "test1")).updateCount);
                }

                // test join with different
                try (ScanResultSet scanner =
                        connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable a"
                                + " INNER JOIN mytable b ON 1=1", true, Collections.emptyList(), 0, 0, 100000, true);) {
                    List<Map<String, Object>> resultSet = scanner.consume();
                    assertEquals(100, resultSet.size());
                }

                try (ScanResultSet scanner =
                        connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable a"
                                + " INNER JOIN mytable b ON 1=1", true, Collections.emptyList(), 0, 0, 1, true);) {
                    List<Map<String, Object>> resultSet = scanner.consume();
                    assertEquals(100, resultSet.size());
                }

                try (ScanResultSet scanner =
                        connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable a"
                                + " INNER JOIN mytable b ON 1=1", true, Collections.emptyList(), 0, 0, 10, true);) {
                    List<Map<String, Object>> resultSet = scanner.consume();
                    assertEquals(100, resultSet.size());
                }

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                try (ScanResultSet scanner =
                        connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable a"
                                + " INNER JOIN mytable b ON 1=1", true, Collections.emptyList(), tx, 0, 1, true);) {
                    List<Map<String, Object>> resultSet = scanner.consume();
                    assertEquals(100, resultSet.size());
                }
                connection.commitTransaction(TableSpace.DEFAULT, tx);
            }



        }
    }

    @Test
    public void testEnsureOpen() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        AtomicReference<ClientSideConnectionPeer[]> connections = new AtomicReference<>();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.getNetworkServer().setEnableJVMNetwork(false);
            server.getNetworkServer().setEnableRealNetwork(true);
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
            clientConfiguration.set(ClientConfiguration.PROPERTY_TIMEOUT, 2000);
            try (HDBClient client = new HDBClient(clientConfiguration) {
                @Override
                public HDBConnection openConnection() {
                    HDBConnection con = new HDBConnection(this) {
                        @Override
                        protected ClientSideConnectionPeer chooseConnection(ClientSideConnectionPeer[] all) {
                            connections.set(all);
                            return all[0];
                        }

                    };
                    registerConnection(con);
                    return con;
                }

            };
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                assertTrue(connection.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));


                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id int primary key, s1 string)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,s1) values(?,?)", 0, false, true, Arrays.asList(1, "test1")).updateCount);

                // assert we are using real network
                assertNotEquals(NettyChannel.ADDRESS_JVM_LOCAL, connections.get()[0].getChannel().getRemoteAddress());
                io.netty.channel.Channel socket = ((NettyChannel) connections.get()[0].getChannel()).getSocket();
                assertThat(socket, instanceOf(SocketChannel.class));

                // invalidate socket connection (simulate network error)
                socket.close().await();

                // ensure reconnection is performed (using prepared statement)
                connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable ", true, Collections.emptyList(), 0, 100, 1000, true).close();

                // assert we are using real network
                assertNotEquals(NettyChannel.ADDRESS_JVM_LOCAL, connections.get()[0].getChannel().getRemoteAddress());
                io.netty.channel.Channel socket2 = ((NettyChannel) connections.get()[0].getChannel()).getSocket();
                assertThat(socket2, instanceOf(SocketChannel.class));
                assertNotSame(socket2, socket);

                // invalidate socket connection (simulate network error)
                socket2.close().await();

                // ensure reconnection is performed (not using prepared statement)
                connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable ", false, Collections.emptyList(), 0, 100, 1000, true).close();


            }
        }
    }

    @Test
    public void testHandleReadTimeout() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
        clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
        final int timeout = 1000;
        final AtomicInteger channelCreatedCount = new AtomicInteger();
        clientConfiguration.set(ClientConfiguration.PROPERTY_NETWORK_TIMEOUT, timeout);
        try (HDBClient client = new HDBClient(clientConfiguration) {
            @Override
            Channel createChannelTo(ServerHostData server, ChannelEventListener eventReceiver) throws IOException {
                channelCreatedCount.incrementAndGet();
                return super.createChannelTo(server, eventReceiver);
            }

        }) {
            try (HDBConnection connection1 = client.openConnection()) {

                try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
                    server.start();
                    server.waitForStandaloneBoot();
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    assertTrue(connection1.waitForTableSpace(TableSpace.DEFAULT, Integer.MAX_VALUE));
                    channelCreatedCount.set(0);
                    long resultCreateTable = connection1.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, false /*usePreparedStatement*/,
                            Collections.emptyList()).updateCount;
                    Assert.assertEquals(1, resultCreateTable);
                    assertEquals(0, channelCreatedCount.get());

                    // wait for client side timeout
                    // the only connected socket will be disconnected
                    Thread.sleep(timeout + 1000);

                    connection1.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM mytable", false /*usePreparedStatement*/,
                            Collections.emptyList(), 0, 0, 10, true).close();

                    assertEquals(1, channelCreatedCount.get());
                }

            }
        }

    }

    @Test
    public void testKeepReadLocks() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
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
                connection.commitTransaction(TableSpace.DEFAULT, tx);

                // new transaction
                tx = connection.beginTransaction(TableSpace.DEFAULT);
                // do not keep locks
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'",  true, Collections.emptyList(), tx, 0, 10, false)) {
                    Map<String, Object> record = scan.consume().get(0);
                    assertEquals(RawString.of("test"), record.get("id"));
                    assertEquals(Long.valueOf(1), record.get("n1"));
                    assertEquals(Integer.valueOf(2), record.get("n2"));
                    Transaction transaction = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx);
                    assertNull(transaction.lookupLock("mytable", Bytes.from_string("test")));
                }

                // keep locks
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'",  true, Collections.emptyList(), tx, 0, 10, true)) {
                    Map<String, Object> record = scan.consume().get(0);
                     assertEquals(RawString.of("test"), record.get("id"));
                    assertEquals(Long.valueOf(1), record.get("n1"));
                    assertEquals(Integer.valueOf(2), record.get("n2"));
                    Transaction transaction = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx);
                    assertFalse(transaction.lookupLock("mytable", Bytes.from_string("test")).write);
                }

                // upgrade lock to write
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test' FOR UPDATE",  true, Collections.emptyList(), tx, 0, 10, false)) {
                    Map<String, Object> record = scan.consume().get(0);
                    assertEquals(RawString.of("test"), record.get("id"));
                    assertEquals(Long.valueOf(1), record.get("n1"));
                    assertEquals(Integer.valueOf(2), record.get("n2"));
                    Transaction transaction = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx);
                    assertTrue(transaction.lookupLock("mytable", Bytes.from_string("test")).write);
                }

                connection.rollbackTransaction(TableSpace.DEFAULT, tx);

                // new transaction
                tx = connection.beginTransaction(TableSpace.DEFAULT);

                // SELECT FOR UPDATE must hold WRITE LOCK even with keepLocks = false
                try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test' FOR UPDATE",  true, Collections.emptyList(), tx, 0, 10, false)) {
                    Map<String, Object> record = scan.consume().get(0);
                    assertEquals(RawString.of("test"), record.get("id"));
                    assertEquals(Long.valueOf(1), record.get("n1"));
                    assertEquals(Integer.valueOf(2), record.get("n2"));
                    Transaction transaction = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx);
                    assertTrue(transaction.lookupLock("mytable", Bytes.from_string("test")).write);
                }
            }
        }
    }

    @Test
    public void testAutoTransaction() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        String _baseDir = baseDir.toString();
        try (Server server = new Server(newServerConfigurationWithAutoPort(baseDir))) {
            server.start();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                DMLResult executeUpdateResult = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", TransactionContext.AUTOTRANSACTION_ID, false, true, Arrays.asList("test", 1, 2));

                long tx = executeUpdateResult.transactionId;
                long countInsert = executeUpdateResult.updateCount;
                Assert.assertEquals(1, countInsert);

                GetResult res = connection.executeGet(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'", tx, true, Collections.emptyList());
                Map<RawString, Object> record = res.data;
                Assert.assertNotNull(record);
                assertEquals(RawString.of("test"), record.get(RawString.of("id")));
                assertEquals(Long.valueOf(1), record.get(RawString.of("n1")));
                assertEquals(Integer.valueOf(2), record.get(RawString.of("n2")));

                connection.commitTransaction(TableSpace.DEFAULT, tx);

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM " + server.getManager().getVirtualTableSpaceId() + ".sysclients", true, Collections.emptyList(), 0, 0, 10, true)) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        assertEquals(RawString.of("jvm-local"), aa.get("address"));
                        assertEquals(RawString.of(ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT), aa.get("username"));
                        assertNotNull(aa.get("connectionts"));
                    }
                    assertTrue(all.size() >= 1);
                }

            }
        }
    }
}
