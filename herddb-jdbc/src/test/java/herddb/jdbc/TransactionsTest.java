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
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Transactions, implicit transaction creation tests
 *
 * @author enrico.olivelli
 */
public class TransactionsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (AbstractHerdDBDataSource dataSource = new AbstractHerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Connection con2 = dataSource.getConnection();
                        Statement statement = con.createStatement();
                        Statement statement2 = con2.createStatement();) {

                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                    con.setAutoCommit(false);

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k2','name2')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k3','name3')"));

                    try (ResultSet rs = statement2.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(0, rs.getLong(1));
                    }

                    con.commit();

                    try (ResultSet rs = statement2.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getLong(1));
                    }

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k4','name4')"));
                    con.commit();

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k5','name5')"));

                    try (ResultSet rs = statement2.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(4, rs.getLong(1));
                    }
                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(5, rs.getLong(1));
                    }

                    con.rollback();

                    try (ResultSet rs = statement2.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(4, rs.getLong(1));
                    }

                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(4, rs.getLong(1));
                    }
                    con.commit();

                    try (PreparedStatement ps1 = con.prepareStatement("SELECT COUNT(*) FROM mytable");
                            ResultSet rs = ps1.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(4, rs.getLong(1));
                    }
                    con.commit();

                    // complex multi record update
                    try (PreparedStatement ps1 = con.prepareStatement("UPDATE mytable set name='aa' WHERE 1=-1");) {
                        assertEquals(0, ps1.executeUpdate());
                    }
                    con.commit();
                    
                    
                    // complex multi record update
                    try (PreparedStatement ps1 = con.prepareStatement("UPDATE mytable set name='aa' WHERE 1=1");) {
                        assertEquals(4, ps1.executeUpdate());
                    }
                    con.commit();

                    // direct record update
                    try (PreparedStatement ps1 = con.prepareStatement("UPDATE mytable set name='aa' WHERE key='k5'");) {
                        assertEquals(0, ps1.executeUpdate());
                    }
                    con.commit();

                }
            }
        }
    }
}
