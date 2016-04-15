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
import herddb.client.ScanRecordConsumer;
import herddb.model.TableSpace;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
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

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, Arrays.asList("test_" + i, 1, 2)));
                }
                AtomicInteger readCount = new AtomicInteger();
                AtomicBoolean finishedCalled = new AtomicBoolean();
                assertTrue(connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable", Collections.emptyList(), new ScanRecordConsumer() {
                    @Override
                    public void finish() {
                        finishedCalled.set(true);
                    }

                    @Override
                    public boolean accept(Map<String, Object> record) throws Exception {                        
                        readCount.incrementAndGet();
                        return true;
                    }

                }, 60000));
                assertEquals(100, readCount.get());
                assertTrue(finishedCalled.get());
            }
        }
    }

    @Test
    public void testInterruptedScan() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, Collections.emptyList());
                Assert.assertEquals(1, resultCreateTable);

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, Arrays.asList("test_" + i, 1, 2)));
                }
                AtomicInteger readCount = new AtomicInteger();
                AtomicBoolean finishedCalled = new AtomicBoolean();
                assertTrue(connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable", Collections.emptyList(), new ScanRecordConsumer() {
                    @Override
                    public void finish() {
                        finishedCalled.set(true);
                    }

                    @Override
                    public boolean accept(Map<String, Object> record) throws Exception {
                        return readCount.incrementAndGet() < 37;
                    }

                }, 60000));
                assertTrue(readCount.get() < 100); // the 'interruption' is asynch and so there is no real control on the number of record returned
                assertTrue(finishedCalled.get());
            }
        }
    }

    @Test
    public void testInterruptedScanWithClientException() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, Collections.emptyList());
                Assert.assertEquals(1, resultCreateTable);

                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, Arrays.asList("test_" + i, 1, 2)));
                }
                AtomicInteger readCount = new AtomicInteger();
                AtomicBoolean finishedCalled = new AtomicBoolean();
                assertTrue(connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable", Collections.emptyList(), new ScanRecordConsumer() {
                    @Override
                    public void finish() {
                        finishedCalled.set(true);
                    }

                    @Override
                    public boolean accept(Map<String, Object> record) throws Exception {
                        boolean ok = readCount.incrementAndGet() < 37;
                        if (!ok) {
                            throw new Exception("client error, please close scanner on server");
                        }
                        return true;
                    }

                }, 60000));
                assertTrue(readCount.get() < 100); // the 'interruption' is asynch and so there is no real control on the number of record returned
                assertTrue(finishedCalled.get());
            }
        }
    }
}
