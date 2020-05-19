/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.jdbc;

import static org.junit.Assert.assertTrue;
import herddb.utils.ZKTestEnv;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for connections using jdbc driver
 *
 * @author enrico.olivelli
 */
public class JdbcJdbcDriverStartDiskLessTest {

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

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testStartServer() throws Exception {

        File dataDirFirstTime = folder.newFolder();
        String jdbcUrl = "jdbc:herddb:zookeeper:" + testEnv.getAddress() + "" + testEnv.getPath() + "?"
                + "server.start=true"
                + "&server.base.dir=" + dataDirFirstTime.getAbsolutePath()
                + "&server.node.id=nodeid"
                + "&server.mode=diskless-cluster";
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
            int count = 0;
            while (rs.next()) {
                System.out.println("table: " + rs.getString(1));
                count++;
            }
            assertTrue(count > 0);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
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

    @AfterClass
    public static void destroy() throws Exception {
        for (Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers(); drivers.hasMoreElements();) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
    }
}
