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

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.LoopbackClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class SimpleDataSourceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));
                try (HerdDBDataSource dataSource = new HerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement statement = con.createStatement();) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                        boolean found = false;
                        while (rs.next()) {
                            String key = rs.getString("key");
                            String name = rs.getString("name");
                            assertEquals("k1", key);
                            assertEquals("name1", name);
                            found = true;
                        }
                        assertTrue(found);
                    }
                    assertEquals(1, statement.executeUpdate("UPDATE mytable set name='name2' where key='k1'"));
                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                        boolean found = false;
                        while (rs.next()) {
                            String key = rs.getString("key");
                            String name = rs.getString("name");
                            System.out.println("QUI " + key + " " + name);
                            assertEquals("k1", key);
                            assertEquals("name2", name);
                            found = true;
                        }
                        assertTrue(found);
                    }
                    assertEquals(1, statement.executeUpdate("DELETE FROM mytable where name='name2' and key='k1'"));
                    assertEquals(0, statement.executeUpdate("UPDATE mytable set name='name2' where key='k1'"));

                    try (PreparedStatement ps = con.prepareStatement("INSERT INTO mytable (key,name) values(?,?)")) {
                        for (int i = 0; i < 10; i++) {
                            ps.setString(1, "kp_" + i);
                            ps.setString(2, "n2_" + i);
                            assertEquals(1, ps.executeUpdate());
                        }
                    }

                    try (PreparedStatement ps = con.prepareStatement("SELECT * FROM mytable");
                            ResultSet rs = ps.executeQuery()) {
                        Set<String> foundKeys = new HashSet<>();
                        while (rs.next()) {
                            foundKeys.add(rs.getString("key"));
                        }
                        for (int i = 0; i < 10; i++) {
                            assertTrue(foundKeys.contains("kp_" + i));
                        }

                    }
                    try (PreparedStatement ps = con.prepareStatement("SELECT COUNT(*) as total FROM mytable");
                            ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10, rs.getLong("total"));
                    }
                    try (PreparedStatement ps = con.prepareStatement("SELECT COUNT(*) FROM mytable");
                            ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10, rs.getLong("COUNT(*)"));
                        assertEquals(10, rs.getLong(1));
                        assertEquals(Long.valueOf(10), rs.getObject(1));
                        assertEquals(Long.valueOf(10), rs.getObject("COUNT(*)"));
                    }
                }
            }
        }
    }
}
