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
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class MultipleDataSourcesSameJVMJMXTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     BasicHerdDBDataSource dataSource_2 = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Connection con_2 = dataSource_2.getConnection();
                     Statement statement = con.createStatement();
                     Statement statement_2 = con.createStatement()) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));
                    assertEquals(1, statement_2.executeUpdate("INSERT INTO mytable (key,name) values('k2','name2')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k3','name3')"));

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable a"
                            + " INNER JOIN mytable b ON 1=1")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(9, count);
                    }
                    MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();

                    Object attributeValue = jmxServer.getAttribute(new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=HerdDBClient"), "BorrowedCount");
                    assertEquals(1L, attributeValue);

                    Object attributeValue2 = jmxServer.getAttribute(new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=HerdDBClient2"), "BorrowedCount");
                    assertEquals(1L, attributeValue2);

                }
            }
        }
    }
}
