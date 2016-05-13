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

import herddb.codec.RecordSerializer;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Booting two servers, one table space
 *
 * @author enrico.olivelli
 */
public class MultiServerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookie();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void test_leader_online_log_available() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_2.set(ServerConfiguration.PROPERTY_NODEID, "server2");
        serverconfig_2.set(ServerConfiguration.PROPERTY_PORT, 7868);
        serverconfig_2.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)));

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1"));

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null));
                    if (found.found()) {
                        break;
                    }
                }
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null)).found());
            }

        }
    }

    @Test
    public void test_leader_offline_log_available() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_2.set(ServerConfiguration.PROPERTY_NODEID, "server2");
        serverconfig_2.set(ServerConfiguration.PROPERTY_PORT, 7868);
        serverconfig_2.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_2.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)));

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1"));

        }

        try (Server server_2 = new Server(serverconfig_2)) {
            server_2.start();

            assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

            // wait for data to arrive on server_2
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null));
                if (found.found()) {
                    break;
                }
            }
            assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null)).found());
        }
    }

}
