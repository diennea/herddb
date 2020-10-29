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

package herddb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class AdvancedInsertSyntaxTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testInsertMultiValues() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement create = con.createStatement();
                     PreparedStatement statement = con.prepareStatement("INSERT INTO mytable (name) values (?),(?)");
                     PreparedStatement statement_return_keys = con.prepareStatement("INSERT INTO mytable (name) values (?),(?)", Statement.RETURN_GENERATED_KEYS)) {
                    create.execute("CREATE TABLE mytable (n1 int primary key auto_increment, name string)");

                    statement.setString(1, "v1");
                    statement.setString(2, "v2");
                    statement.executeUpdate();

                    try {
                        statement_return_keys.setString(1, "v1");
                        statement_return_keys.setString(2, "v2");
                        statement_return_keys.executeUpdate();
                        fail();
                    } catch (SQLException err) {
                        assertTrue(err.getMessage().contains("cannot 'return values' on multi-values insert"));
                    }

                }
            }
        }
    }

    @Test
    public void testInsertFromSelect() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement create = con.createStatement();
                     PreparedStatement statement = con.prepareStatement("INSERT INTO mytable (name) values(?)")) {
                    create.execute("CREATE TABLE mytable (n1 int primary key auto_increment, name string)");
                    create.execute("CREATE TABLE mytable2 (n1 int primary key auto_increment, name string)");

                    {
                        for (int i = 0; i < 100; i++) {
                            statement.setString(1, "v" + i);
                            statement.addBatch();
                        }
                        int[] results = statement.executeBatch();
                        for (int i = 0; i < 100; i++) {
                            assertEquals(1, results[i]);
                        }

                        try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable ORDER BY n1")) {
                            int count = 0;
                            while (rs.next()) {
                                assertEquals("v" + count, rs.getString("name"));
                                assertEquals(count + 1, rs.getInt("n1"));
                                count++;
                            }
                            assertEquals(100, count);
                        }
                    }

                    statement.executeUpdate("INSERT INTO mytable2(n1,name) SELECT n1, name from mytable order by name desc");

                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable2")) {
                        assertTrue(rs.next());
                        assertEquals(100, rs.getInt(1));
                    }

                    // leverage auto_increment
                    statement.executeUpdate("INSERT INTO mytable2(name) SELECT name from mytable order by name desc");

                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable2")) {
                        assertTrue(rs.next());
                        assertEquals(200, rs.getInt(1));
                    }

                    try (ResultSet rs = statement.executeQuery("SELECT n1 FROM mytable2 order by n1")) {
                        Set<Integer> ids = new HashSet<>();
                        while (rs.next()) {
                            int id = rs.getInt(1);
                            assertTrue(ids.add(id));
                        }
                        assertEquals(200, ids.size());
                    }
                }
            }
        }

    }

}
