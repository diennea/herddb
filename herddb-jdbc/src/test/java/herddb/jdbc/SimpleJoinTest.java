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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class SimpleJoinTest {

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
                     Connection con = dataSource.getConnection();
                     Statement statement = con.createStatement()) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");

                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k2','name2')"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k3','name3')"));

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable a"
                            + " INNER JOIN mytable b ON 1=1")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(9, count);
                    }

                }
            }
        }
    }

    @Test
    public void testSubQuery() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement statement = con.createStatement()) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");

                    statement.execute("CREATE TABLE table1 (k1 string primary key,n1 int,s1 string)");
                    statement.execute("CREATE TABLE table2 (k2 string primary key,n2 int,s2 string)");

                    statement.execute("INSERT INTO table1 (k1,n1,s1) values('a',1,'A')");
                    statement.execute("INSERT INTO table1 (k1,n1,s1) values('b',2,'B')");

                    statement.execute("INSERT INTO table2 (k2,n2,s2) values('c',3,'A')");
                    statement.execute("INSERT INTO table2 (k2,n2,s2) values('d',4,'A')");

                    try (ResultSet rs = statement.executeQuery("SELECT t1.k1, max(n1) as maxn1, max(select n2 from table2 t2 WHERE t1.s1=t2.s2) as maxn2 FROM "
                            + " table1 t1 "
                            + " group by k1")) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(2, count);
                    }

                }
            }
        }
    }

}
