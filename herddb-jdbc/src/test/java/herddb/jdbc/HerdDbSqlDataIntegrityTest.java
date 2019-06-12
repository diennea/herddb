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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for ensuring proper handling of SQLIntegrityViolation implementation on the server
 * @author amitvc
 *
 */
public class HerdDbSqlDataIntegrityTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test()
    public void basic_test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                Connection con = dataSource.getConnection();
                Statement create = con.createStatement();
                create.execute("CREATE TABLE test_t1 (n1 int primary key, name string)");
                PreparedStatement statement = con.prepareStatement("INSERT INTO test_t1(n1,name) values(?,?)");
                statement.setInt(1, 100);
                statement.setString(2, "test1");
                statement.executeUpdate();
                try {
                    con.prepareStatement("INSERT INTO test_t1 (n1,name) values(?,?)");
                    statement.setInt(1, 100);
                    statement.setString(2, "test1");
                    statement.executeUpdate();
                } catch (SQLIntegrityConstraintViolationException ex) {
                    Assert.assertTrue(ex instanceof  SQLIntegrityConstraintViolationException);
                }
            }
        }
    }

    @Test()
    public void basic_test_with_transactions() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                Connection con = dataSource.getConnection();
                con.setAutoCommit(false);

                Statement create = con.createStatement();
                create.execute("CREATE TABLE test_t1 (n1 int primary key, name string)");

                Statement statement = con.createStatement();
                statement.executeUpdate("INSERT INTO test_t1(n1,name) values(100,'test100')");

                try {
                    statement.executeUpdate("INSERT INTO test_t1(n1,name) values(100,'test10220')");
                } catch (SQLIntegrityConstraintViolationException ex) {
                    Assert.assertTrue(ex instanceof  SQLIntegrityConstraintViolationException);
                }
                statement.executeUpdate("INSERT INTO test_t1(n1,name) values(101,'test101')");
                con.commit();

                try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM test_t1")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getLong(1));
                }

                try (ResultSet rs = statement.executeQuery("SELECT * FROM test_t1")) {
                    int i=0;
                    while(rs.next()) {
                        if (i==0) {
                            i++;
                            Assert.assertEquals(100,rs.getLong(1));
                            Assert.assertEquals("test100",rs.getString(2));
                        } else {
                            Assert.assertEquals(101,rs.getLong(1));
                            Assert.assertEquals("test101",rs.getString(2));
                        }
                    }
                }
            }
        }
    }
}
