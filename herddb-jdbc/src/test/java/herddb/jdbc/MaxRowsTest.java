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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class MaxRowsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (AbstractHerdDBDataSource dataSource = new AbstractHerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement statement = con.createStatement();) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k2','name2')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k3','name3')"));

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(3, count);
                    }
                    statement.setMaxRows(2);
                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(2, count);
                    }
                    statement.setMaxRows(0);
                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(3, count);
                    }
                    statement.setMaxRows(Integer.MAX_VALUE);
                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(3, count);
                    }

                    try (PreparedStatement ps = con.prepareStatement("SELECT * FROM mytable");) {
                        try (ResultSet rs = ps.executeQuery()) {
                            int count = 0;
                            while (rs.next()) {
                                count++;
                            }
                            assertEquals(3, count);
                        }
                        ps.setMaxRows(2);
                        try (ResultSet rs = ps.executeQuery()) {
                            int count = 0;
                            while (rs.next()) {
                                count++;
                            }
                            assertEquals(2, count);
                        }
                        ps.setMaxRows(0);
                        try (ResultSet rs = ps.executeQuery()) {
                            int count = 0;
                            while (rs.next()) {
                                count++;
                            }
                            assertEquals(3, count);
                        }
                        ps.setMaxRows(Integer.MAX_VALUE);
                        try (ResultSet rs = ps.executeQuery()) {
                            int count = 0;
                            while (rs.next()) {
                                count++;
                            }
                            assertEquals(3, count);
                        }

                    }

                }
            }
        }
    }
}
