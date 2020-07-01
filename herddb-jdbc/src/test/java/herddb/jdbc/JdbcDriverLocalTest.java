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
import herddb.server.ServerConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for connections using jdbc driver
 *
 * @author enrico.olivelli
 */
public class JdbcDriverLocalTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testNoAutoClose() throws Exception {

        DriverManager.registerDriver(new Driver());

        Properties props = new Properties();
        props.put(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toString());
        try (Connection connection = DriverManager.getConnection("jdbc:herddb:local?autoClose=false", props);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
            int count = 0;
            while (rs.next()) {
                System.out.println("table: " + rs.getString(1));
                count++;
            }
            assertTrue(count > 0);
            statement.execute("CREATE TABLE tt(k1 string primary key)");
            statement.execute("INSERT INTO tt values('aa')");
        }

        try (Connection connection = DriverManager.getConnection("jdbc:herddb:local?autoClose=false", props);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM tt")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(1, count);
        }

    }

    @Test
    public void testAutoClose() throws Exception {
        DriverManager.registerDriver(new Driver());

        Properties props = new Properties();
        props.put(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toString());
        try (Connection connection = DriverManager.getConnection("jdbc:herddb:local?autoClose=false", props);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
            int count = 0;
            while (rs.next()) {
                System.out.println("table: " + rs.getString(1));
                count++;
            }
            assertTrue(count > 0);
            statement.execute("CREATE TABLE tt(k1 string primary key)");
            statement.execute("INSERT INTO tt values('aa')");
        }

        try (Connection connection = DriverManager.getConnection("jdbc:herddb:local?autoClose=false", props);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM tt")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(1, count);
        }

    }

    @After
    public void destroy() throws Exception {
        for (Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers(); drivers.hasMoreElements(); ) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
    }
}
