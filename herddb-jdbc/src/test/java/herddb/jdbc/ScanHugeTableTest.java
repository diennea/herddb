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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class ScanHugeTableTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBatch() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.getManager().setMaxDataUsedMemory(750 * 1024 * 1024);
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                    Connection con = dataSource.getConnection();
                    Statement s = con.createStatement();
                    PreparedStatement ps = con.prepareStatement("INSERT INTO mytable (n1, name) values(?,?)");) {
                    s.execute("CREATE TABLE mytable (n1 int primary key, name string)");

                    String bigPrefix = StringUtils.repeat("Test", 300);
                    int size = 1_000_000;
                    {
                        long _start = System.currentTimeMillis();
                        con.setAutoCommit(false);
                        for (int i = 0; i < size; i++) {
                            ps.setInt(1, i);
                            ps.setString(2, bigPrefix + i);
                            ps.addBatch();
                            if (i % 60000 == 0) {
                                ps.executeBatch();
                                con.commit();
                                long _stop = System.currentTimeMillis();
                                System.out.println("written " + i + " records_ "+(_stop-_start)+" ms");
                            }
                        }
                        ps.executeBatch();
                        con.commit();
                        long _stop = System.currentTimeMillis();
                        System.out.println("insert: " + (_stop - _start) + " ms");
                    }

                    server.getManager().checkpoint();

                    con.setAutoCommit(true);
                    {
                        long _start = System.currentTimeMillis();
                        try (ResultSet rs = s.executeQuery("SELECT COUNT(*) from mytable")) {
                            assertTrue(rs.next());
                            long res = rs.getLong(1);
                            assertEquals(size, res);
                        }
                        long _stop = System.currentTimeMillis();
                        System.out.println("count: " + (_stop - _start) + " ms");
                    }
                    {
                        long _start = System.currentTimeMillis();
                        try (ResultSet rs = s.executeQuery("SELECT * from mytable")) {
                            int i = 0;
                            while (rs.next()) {
                                if (i % 100000 == 0) {
                                    System.out.println("read " + i + " records");
                                }
                                i++;
                            }
                            assertEquals(size, i);
                        }
                        long _stop = System.currentTimeMillis();
                        System.out.println("read: " + (_stop - _start) + " ms");
                    }

                }
            }
        }

    }

}
