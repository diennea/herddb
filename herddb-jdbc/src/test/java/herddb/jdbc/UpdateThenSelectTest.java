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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;

/**
 * Check update then select (#320, not every row update really was run).
 *
 * @author diego.salvi
 */
public class UpdateThenSelectTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        int runs = 25;

        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client)) {

                    for (int run = 0; run < runs; ++run) {

                        try (Connection con = dataSource.getConnection();
                                Statement statement = con.createStatement();) {
                            statement.execute(
                                    "CREATE TABLE mytable (k1 int primary key, n1 int, l1 long, t1 timestamp, nu string, b1 bool, d1 double)");
                        }

                        try (Connection con = dataSource.getConnection();
                                PreparedStatement statement = con.prepareStatement(
                                        "INSERT INTO mytable(k1,n1,l1,t1,nu,b1,d1) values(?,?,?,?,?,?,?)");) {

                            for (int id = 0; id < 10; ++id) {
                                int i = 1;
                                statement.setInt(i++, id);
                                statement.setInt(i++, 1);
                                statement.setLong(i++, 2);
                                statement.setTimestamp(i++, new java.sql.Timestamp(System.currentTimeMillis()));
                                statement.setString(i++, null);
                                statement.setBoolean(i++, true);
                                statement.setDouble(i++, 1.5);
                                statement.addBatch();
                            }

                            int[] rows = statement.executeBatch();
                            for (int row : rows) {
                                Assert.assertEquals(1, row);
                            }
                        }

                        try (Connection con = dataSource.getConnection()) {

                            con.setAutoCommit(false);

                            try (PreparedStatement statement = con.prepareStatement("UPDATE mytable set n1 = ?");) {
                                statement.setInt(1, 100);
                                int updated = statement.executeUpdate();
                                Assert.assertEquals(10, updated);
                            }

                            try (PreparedStatement statement = con
                                    .prepareStatement("SELECT n1 FROM mytable WHERE k1 = ?")) {
                                for (int id = 0; id < 10; ++id) {
                                    statement.setInt(1, id);

                                    try (ResultSet rs = statement.executeQuery()) {
                                        Assert.assertTrue(rs.next());
                                        Assert.assertEquals(100, rs.getInt(1));
                                    }
                                }
                            }

                            con.commit();

                        }

                        try (Connection con = dataSource.getConnection()) {
                            try (PreparedStatement statement = con
                                    .prepareStatement("SELECT n1 FROM mytable WHERE k1 = ?")) {
                                for (int id = 0; id < 10; ++id) {
                                    statement.setInt(1, id);

                                    try (ResultSet rs = statement.executeQuery()) {
                                        Assert.assertTrue(rs.next());
                                        Assert.assertEquals(100, rs.getInt(1));
                                    }
                                }
                            }
                        }

                        try (Connection con = dataSource.getConnection();
                                Statement statement = con.createStatement();) {
                            statement.execute("DROP TABLE mytable");
                        }
                    }
                }
            }
        }
    }
}
