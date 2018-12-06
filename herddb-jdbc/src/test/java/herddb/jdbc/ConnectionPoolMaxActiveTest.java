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
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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
public class ConnectionPoolMaxActiveTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        try (HerdDBEmbeddedDataSource dataSource = new HerdDBEmbeddedDataSource();) {
            dataSource.setMaxActive(20);

            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ClientConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            try (Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement();) {
                statement.execute("CREATE TABLE mytable (key string primary key, name string)");
            }

            Server server = dataSource.getServer();
            List<Connection> connections = new ArrayList<>();
            try {
                for (int i = 0; i < 10; i++) {
                    Connection con = dataSource.getConnection();
                    connections.add(con);
                    try (Statement statement = con.createStatement();) {
                        assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1" + i + "','name1')"));
                    }
                }
                // this is the number of sockets
                assertTrue(server.getConnectionCount() >= 1);
            } finally {
                for (Connection c : connections) {
                    c.close();
                }
            }

        }
    }

}
