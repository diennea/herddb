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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests on auth
 *
 * @author enrico.olivelli
 */
public class PrettySQLExceptionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        ServerConfiguration serverConfiguration = new ServerConfiguration(folder.newFolder().toPath());

        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            try (Connection connection = DriverManager.getConnection("jdbc:herddb:server:localhost:7000?");
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT * FROM bad_table")) {
                fail();
            } catch (SQLException errorExpected) {
                System.out.println("EXCEPTION MESSAGE: " + errorExpected.getMessage());
                assertTrue("herddb.model.TableDoesNotExistException: no such table bad_table in tablespace default"
                        .equals(errorExpected.getMessage()) || errorExpected.getMessage().contains("Object 'BAD_TABLE' not found"));
            }

        }
    }

    @After
    public void destroy() throws Exception {
        for (Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers(); drivers.hasMoreElements(); ) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
    }
}
