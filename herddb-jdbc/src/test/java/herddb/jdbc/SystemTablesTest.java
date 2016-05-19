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
import herddb.server.LoopbackClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
public class SystemTablesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new LoopbackClientSideMetadataProvider(server));
                try (HerdDBDataSource dataSource = new HerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement statement = con.createStatement();) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                    statement.execute("CREATE TABLE mytable2 (n2 int primary key auto_increment, name string, ts timestamp)");

                    try (ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {

                        Set<String> tables = new HashSet<>();
                        while (rs.next()) {
                            String name = rs.getString("table_name");
                            tables.add(name);
                        }
                        assertTrue(tables.contains("mytable"));
                        assertTrue(tables.contains("mytable2"));

                    }

                    DatabaseMetaData metaData = con.getMetaData();
                    try (ResultSet rs = metaData.getTables(null, null, null, null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains("mytable")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("mytable2")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("systables")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("syscolumns")).findAny().isPresent());

                    }

                    try (ResultSet rs = metaData.getTables(null, null, "m%table", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains("mytable")).findAny().isPresent());
                        assertEquals(1, records.size());
                    }
                    try (ResultSet rs = metaData.getTables(null, null, "m%table_", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains("mytable")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("mytable2")).findAny().isPresent());
                        assertEquals(2, records.size());
                    }

                    try (ResultSet rs = metaData.getColumns(null, null, "mytable2", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            System.out.println("record:" + record);
                            records.add(record);
                        }
                        assertEquals(3, records.size());
                        assertTrue(records.stream().filter(s -> s.contains("n2")
                                && s.contains("integer")
                        ).findAny().isPresent());
                        assertTrue(records.stream().filter(s
                                -> s.contains("name") && s.contains("string")
                        ).findAny().isPresent());
                        assertTrue(records.stream().filter(s
                                -> s.contains("ts") && s.contains("timestamp")
                        ).findAny().isPresent());
                    }
                }
            }
        }
    }
}
