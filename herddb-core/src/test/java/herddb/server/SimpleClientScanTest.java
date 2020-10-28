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
package herddb.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.client.ClientConfiguration;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.model.TableSpace;
import herddb.utils.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;

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
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true,
                        Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                for (int i = 0; i < 99; i++) {
                    Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, false, true, Arrays.
                                    asList("test_" + i, 1, 2)).updateCount);
                }

                assertEquals(99, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", true, Collections.
                        emptyList(), 0, 0, 10, true).consume().size());

                // maxRows
                assertEquals(17, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", true, Collections.
                        emptyList(), 0, 17, 10, true).consume().size());

                // empty result set
                assertEquals(0, connection.
                        executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable WHERE id='none'", true, Collections.
                                emptyList(), 0, 0, 10, true).consume().size());

                // single fetch result test
                assertEquals(1, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable WHERE id='test_1'",
                        true, Collections.emptyList(), 0, 0, 10, true).consume().size());

                // agregation in transaction, this is trickier than what you can think
                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                assertEquals(1, connection.executeScan(TableSpace.DEFAULT,
                        "SELECT count(*) FROM mytable WHERE id='test_1'", true, Collections.emptyList(), tx, 0, 10, true).
                        consume().size());
                connection.rollbackTransaction(TableSpace.DEFAULT, tx);

                assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable WHERE id=?", true,
                        Arrays.<Object>asList((Object) null), 0, 0, 10, true).consume().size());

            }
        }
    }

    @Test
    public void scanMultiChunk() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 10); // more than one socket
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true,
                        Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                for (int i = 0; i < 99; i++) {
                    Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, false, true, Arrays.
                                    asList("test_" + i, 1, 2)).updateCount);
                }

                checkUseOnlyOneSocket(connection, server);

                checkNoScannersOnTheServer(server);

                checkCloseScannerOnConnectionClose(client, connection, server, true);

                checkNoScannersOnTheServer(server);
                checkCloseScannerOnConnectionClose(client, connection, server, false);

                checkNoScannersOnTheServer(server);

                checkCloseResultSetNotFullyScanned(connection, server, true);

                checkNoScannersOnTheServer(server);

                checkCloseResultSetNotFullyScanned(connection, server, false);

                checkNoScannersOnTheServer(server);

                checkCloseResultSetNotFullyScannedWithTransactionAndAggregation(connection, server, true);

                checkNoScannersOnTheServer(server);

                checkCloseResultSetNotFullyScannedWithTransactionAndAggregation(connection, server, false);

                checkNoScannersOnTheServer(server);

            }

            checkNoScannersOnTheServer(server);
        }
    }

    private void checkNoScannersOnTheServer(final Server server) throws Exception {
        TestUtils.waitForCondition(() -> {
            for (ServerSideConnectionPeer peer : server.getConnections().values()) {
                if (!(peer.getScanners().isEmpty())) {
                    return false;
                }
            }
            return true;
        }, TestUtils.NOOP, 1000, "there is at least one scanner");

    }

    private void checkCloseScannerOnConnectionClose(
            final HDBClient client, HDBConnection primary,
            final Server server, boolean withTransaction
    ) throws HDBException, InterruptedException, ClientSideMetadataProviderException {
        try (HDBConnection connection2 = client.openConnection()) {
            {
                // scan with fetchSize = 1, we will have 100 chunks
                ServerSideConnectionPeer peerWithScanner = null;
                long tx = withTransaction ? primary.beginTransaction(TableSpace.DEFAULT) : 0;
                try (ScanResultSet scan = connection2.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", true,
                        Collections.emptyList(), tx, 0, 1, true)) {
                    assertTrue(scan.hasNext());
                    System.out.println("next:" + scan.next());

                    for (ServerSideConnectionPeer peer : server.getConnections().values()) {
                        System.out.println("peer " + peer + " scanners: " + peer.getScanners());
                        if (!peer.getScanners().isEmpty()) {
                            if (peerWithScanner != null && peerWithScanner != peer) {
                                fail("Found more then one peer with an open scanner");
                            }
                            peerWithScanner = peer;
                            assertEquals(1, peer.getScanners().size());
                        }
                    }
                    assertNotNull(peerWithScanner);

                    // force close the connection
                    connection2.close();

                    // give time to the server to release resources
                    Thread.sleep(1000);

                    // the scanner must be closed
                    assertTrue(peerWithScanner.getScanners().isEmpty());

                }
                assertNotNull(peerWithScanner);
                if (withTransaction) {
                    primary.rollbackTransaction(TableSpace.DEFAULT, tx);
                }
            }
        }
    }

    private void checkUseOnlyOneSocket(final HDBConnection connection, final Server server) throws HDBException, ClientSideMetadataProviderException, InterruptedException {
        // scan with fetchSize = 1, we will have 100 chunks
        ServerSideConnectionPeer peerWithScanner = null;
        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", true, Collections.
                emptyList(), 0, 0, 1, true)) {
            while (scan.hasNext()) {
                System.out.println("next:" + scan.next());

                for (ServerSideConnectionPeer peer : server.getConnections().values()) {
                    System.out.println("peer " + peer + " scanners: " + peer.getScanners());
                    if (!peer.getScanners().isEmpty()) {
                        if (peerWithScanner != null && peerWithScanner != peer) {
                            fail("Found more then one peer with an open scanner");
                        }
                        peerWithScanner = peer;
                        assertEquals(1, peer.getScanners().size());
                    }
                }
                assertNotNull(peerWithScanner);
            }
        }
        assertNotNull(peerWithScanner);
    }

    private void checkCloseResultSetNotFullyScanned(
            final HDBConnection connection, final Server server,
            boolean withTransaction
    ) throws HDBException, ClientSideMetadataProviderException, InterruptedException {
        long tx = withTransaction ? connection.beginTransaction(TableSpace.DEFAULT) : 0;
        // scan with fetchSize = 1, we will have 100 chunks
        ServerSideConnectionPeer peerWithScanner = null;
        int chunks = 0;
        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM mytable", true, Collections.
                emptyList(), tx, 0, 1, true)) {

            while (scan.hasNext()) {
                System.out.println("next:" + scan.next());

                for (ServerSideConnectionPeer peer : server.getConnections().values()) {
                    System.out.println("peer " + peer + " scanners: " + peer.getScanners());
                    if (!peer.getScanners().isEmpty()) {
                        if (peerWithScanner != null && peerWithScanner != peer) {
                            fail("Found more then one peer with an open scanner");
                        }
                        peerWithScanner = peer;
                        assertEquals(1, peer.getScanners().size());
                    }
                }
                assertNotNull(peerWithScanner);

                if (chunks++ >= 5) {
                    break;
                }

            }
        }
        assertNotNull(peerWithScanner);
        assertEquals(6, chunks);
        if (withTransaction) {
            connection.rollbackTransaction(TableSpace.DEFAULT, tx);
        }
    }

    private void checkCloseResultSetNotFullyScannedWithTransactionAndAggregation(final HDBConnection connection,
                                                                                 final Server server,
                                                                                 boolean withTransaction) throws HDBException, ClientSideMetadataProviderException, InterruptedException {
        long tx = withTransaction ? connection.beginTransaction(TableSpace.DEFAULT) : 0;

        ServerSideConnectionPeer peerWithScanner = null;

        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                "SELECT count(*) FROM mytable "
                + "UNION ALL "
                + "SELECT count(*) FROM mytable", true, Collections.emptyList(), tx,
                0, 1, true)) {

            assertTrue(scan.hasNext());
            System.out.println("next:" + scan.next());

            for (ServerSideConnectionPeer peer : server.getConnections().values()) {
                System.out.println("peer " + peer + " scanners: " + peer.getScanners());
                if (!peer.getScanners().isEmpty()) {
                    if (peerWithScanner != null && peerWithScanner != peer) {
                        fail("Found more then one peer with an open scanner");
                    }
                    peerWithScanner = peer;
                    assertEquals(1, peer.getScanners().size());
                }
            }
            assertNotNull(peerWithScanner);

        }
        assertNotNull(peerWithScanner);

        if (withTransaction) {
            connection.rollbackTransaction(TableSpace.DEFAULT, tx);
        }
    }

}
