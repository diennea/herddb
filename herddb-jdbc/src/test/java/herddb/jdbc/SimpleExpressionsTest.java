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
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class SimpleExpressionsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client)) {

                    try (Connection con = dataSource.getConnection();
                         Statement statement = con.createStatement()) {
                        statement.execute("CREATE TABLE mytable (k1 string primary key, n1 int, l1 long, t1 timestamp, nu string, b1 bool, d1 double)");

                    }

                    try (Connection con = dataSource.getConnection();
                         PreparedStatement statement = con.prepareStatement("INSERT INTO mytable(k1,n1,l1,t1,nu,b1,d1) values(?,?,?,?,?,?,?)")) {
                        int i = 1;
                        statement.setString(i++, "mykey");
                        statement.setInt(i++, 1);
                        statement.setLong(i++, 2);
                        statement.setTimestamp(i++, new java.sql.Timestamp(System.currentTimeMillis()));
                        statement.setString(i++, null);
                        statement.setBoolean(i++, true);
                        statement.setDouble(i++, 1.5);
                        int rows = statement.executeUpdate();

                        Assert.assertEquals(1, rows);
                    }

                    // Types
                    try (Connection con = dataSource.getConnection()) {
                        // String
                        try (PreparedStatement statement = con.prepareStatement("SELECT k1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals("mykey", rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT upper(k1) FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals("MYKEY", rs.getObject(1));
                            }
                        }

                        // Number
                        try (PreparedStatement statement = con.prepareStatement("SELECT n1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(1, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT n1*5 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(5L, rs.getObject(1));
                                assertEquals(5, rs.getLong(1));
                                assertEquals(5, rs.getInt(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT n1*5.0 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(5d, rs.getObject(1));
                                assertEquals(5, rs.getLong(1));
                                assertEquals(5, rs.getInt(1));
                            }
                        }

                        // Long
                        try (PreparedStatement statement = con.prepareStatement("SELECT l1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(2L, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT l1*5 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(10L, rs.getObject(1));
                                assertEquals(10, rs.getLong(1));
                                assertEquals(10, rs.getInt(1));
                                assertEquals(10f, rs.getFloat(1), 0);
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT l1*5.1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(10.2, rs.getObject(1));
                                assertEquals(10, rs.getLong(1));
                                assertEquals(10, rs.getInt(1));
                                assertEquals(10.2f, rs.getFloat(1), 0);
                            }
                        }

                        // Null
                        try (PreparedStatement statement = con.prepareStatement("SELECT nu FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(null, rs.getObject(1));
                            }
                        }

                        // Double
                        try (PreparedStatement statement = con.prepareStatement("SELECT 3/2 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(1.5, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT d1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(1.5, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT d1*5/2 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(3.75, rs.getObject(1));
                                assertEquals(3.75, rs.getDouble(1), 0);
                                assertEquals(3, rs.getLong(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT -n1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(-1, rs.getObject(1));
                                assertEquals(-1, rs.getDouble(1), 0);
                                assertEquals(-1, rs.getInt(1), 0);
                                assertEquals(-1, rs.getLong(1), 0);
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT -n1-n1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(-2L, rs.getObject(1));
                                assertEquals(-2, rs.getDouble(1), 0);
                                assertEquals(-2, rs.getInt(1), 0);
                                assertEquals(-2, rs.getLong(1), 0);
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT +n1+n1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(2L, rs.getObject(1));
                                assertEquals(2, rs.getDouble(1), 0);
                                assertEquals(2, rs.getInt(1), 0);
                                assertEquals(2, rs.getLong(1), 0);
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT n1*n1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(1L, rs.getObject(1));
                                assertEquals(1, rs.getDouble(1), 0);
                                assertEquals(1, rs.getInt(1), 0);
                                assertEquals(1, rs.getLong(1), 0);
                            }
                        }

                        try (PreparedStatement statement = con.prepareStatement("SELECT -d1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(-1.5, rs.getObject(1));
                                assertEquals(-1.5, rs.getDouble(1), 0);
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT +d1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(1.5, rs.getObject(1));
                                assertEquals(1.5, rs.getDouble(1), 0);
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT +d1+d1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(3, rs.getDouble(1), 0);
                                assertEquals(3.0, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT +d1*d1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(2.25, rs.getDouble(1), 0);
                                assertEquals(2.25, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT -d1+d1 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(0, rs.getDouble(1), 0);
                                assertEquals(0.0, rs.getObject(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT +d1+4 FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(5.5, rs.getDouble(1), 0);
                                assertEquals(5.5, rs.getObject(1));
                            }
                        }

                        // Boolean
                        try (PreparedStatement statement = con.prepareStatement("SELECT true FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(true, rs.getObject(1));
                                assertEquals(true, rs.getBoolean(1));
                            }
                        }
                        try (PreparedStatement statement = con.prepareStatement("SELECT false FROM mytable")) {
                            try (ResultSet rs = statement.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(false, rs.getObject(1));
                                assertEquals(false, rs.getBoolean(1));
                            }
                        }
                    }

                }
            }
        }
    }
}
