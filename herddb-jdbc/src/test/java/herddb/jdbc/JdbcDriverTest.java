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

import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for connections using jdbc driver
 *
 * @author enrico.olivelli
 */
public class JdbcDriverTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (Connection connection = DriverManager.getConnection("jdbc:herddb:server:localhost:7000?");
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
                int count = 0;
                while (rs.next()) {
                    System.out.println("table: " + rs.getString(1));
                    count++;
                }
                assertTrue(count > 0);
            }

            try (Connection connection = DriverManager.getConnection("jdbc:herddb:server:localhost");
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
                int count = 0;
                while (rs.next()) {
                    System.out.println("table: " + rs.getString(1));
                    count++;
                }
                assertTrue(count > 0);
            }
        }
    }

    @Test
    public void testResuseConnections() throws Exception {

        ServerConfiguration conf = new ServerConfiguration(folder.newFolder().toPath());
        conf.set(ServerConfiguration.PROPERTY_PORT, "9123");
        try (Server server = new Server(conf)) {
            server.start();
            server.waitForStandaloneBoot();
            try (Connection connection = DriverManager.getConnection("jdbc:herddb:server:localhost:9123?");
                    Connection connection2 = DriverManager.getConnection("jdbc:herddb:server:localhost:9123?");
                    Statement statement = connection.createStatement();
                    Statement statement2 = connection2.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES");
                    ResultSet rs2 = statement2.executeQuery("SELECT * FROM SYSTABLES")) {
                int count = 0;
                while (rs.next()) {
                    System.out.println("table: " + rs.getString(1));
                    count++;
                }
                assertTrue(count > 0);
                assertEquals(1, server.getActualConnections().connections.size());
            }

            try (Connection connection = DriverManager.getConnection("jdbc:herddb:server:localhost:9123?");
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
                int count = 0;
                while (rs.next()) {
                    System.out.println("table: " + rs.getString(1));
                    count++;
                }
                assertTrue(count > 0);
                assertEquals(1, server.getActualConnections().connections.size());
            }
        }
    }

    @AfterClass
    public static void destroy() throws Exception {
        for (Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers(); drivers.hasMoreElements();) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
    }
}
