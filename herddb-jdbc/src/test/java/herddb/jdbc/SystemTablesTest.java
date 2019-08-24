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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     Statement statement = con.createStatement()) {
                    statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                    statement.execute("CREATE INDEX mytableindex ON mytable(name)");
                    statement.execute("CREATE TABLE mytable2 (n2 int primary key auto_increment, name string not null, ts timestamp)");
                    statement.execute("CREATE TABLE mytable3 (n2 int primary key auto_increment, age int not null, phone long not null, salary double, married bool)");


                    try (ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {

                        Set<String> tables = new HashSet<>();
                        while (rs.next()) {
                            String name = rs.getString("table_name");
                            tables.add(name);
                        }
                        assertTrue(tables.contains("mytable"));
                        assertTrue(tables.contains("mytable2"));
                        assertTrue(tables.contains("mytable3"));

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
                        assertTrue(records.stream().filter(s -> s.contains("mytable3")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("systables")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("syscolumns")).findAny().isPresent());
                        assertTrue(records.stream().filter(s -> s.contains("sysstatements")).findAny().isPresent());

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
                    try (ResultSet rs = metaData.getTables(null, null, "m_table%", null)) {
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
                        assertTrue(records.stream().filter(s -> s.contains("mytable3")).findAny().isPresent());
                        assertEquals(3, records.size());
                    }

                    try (ResultSet rs = metaData.getColumns(null, null, "mytable2", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                                // Assert that name column has the appropriate data type and type names
                                if (value != null && value.equalsIgnoreCase("name")) {
                                    assertTrue(rs.getString("DATA_TYPE").equalsIgnoreCase("string"));
                                    assertTrue(rs.getString("TYPE_NAME").equalsIgnoreCase("string not null"));
                                }
                            }
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
                                -> s.contains("name") && s.contains("string")
                                && s.contains("string not null")
                        ).findAny().isPresent());

                        assertTrue(records.stream().filter(s
                                -> s.contains("ts") && s.contains("timestamp")
                        ).findAny().isPresent());
                    }

                    try (ResultSet rs = metaData.getColumns(null, null, "mytable3", null)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertEquals(5, records.size());
                        assertTrue(records.stream().filter(s -> s.contains("n2")
                                && s.contains("integer")
                        ).findAny().isPresent());

                        assertTrue(records.stream().filter(s
                                -> s.contains("age") && s.contains("integer")
                                && s.contains("integer not null")
                        ).findAny().isPresent());

                        assertTrue(records.stream().filter(s
                                -> s.contains("phone") && s.contains("long")
                                && s.contains("long not null")
                        ).findAny().isPresent());

                        assertTrue(records.stream().filter(s
                                -> s.contains("salary") && s.contains("double")
                                && s.contains("double")
                        ).findAny().isPresent());

                        assertTrue(records.stream().filter(s
                                -> s.contains("married") && s.contains("boolean")
                                && s.contains("boolean")
                        ).findAny().isPresent());
                    }

                    try (ResultSet rs = metaData.getIndexInfo(null, null, "mytable", false, false)) {

                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertEquals(2, records.size()); // pk + secondary index
                    }

                    try (ResultSet rs = metaData.getIndexInfo(null, null, "mytable", true, false)) {

                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertEquals(1, records.size()); // only pk
                    }

                    try (ResultSet rs = metaData.getIndexInfo(null, null, null, true, false)) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        // this is to be incremented at every new systable
                        assertEquals(22, records.size());
                    }
                    try (ResultSet rs = metaData.getSchemas()) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains(TableSpace.DEFAULT)).findAny().isPresent());
                        assertEquals(1, records.size());

                    }

                    try (ResultSet rs = metaData.getSchemas(null, "%" + TableSpace.DEFAULT.substring(2))) {
                        List<List<String>> records = new ArrayList<>();
                        while (rs.next()) {
                            List<String> record = new ArrayList<>();
                            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                                String value = rs.getString(i + 1);
                                record.add(value);
                            }
                            records.add(record);
                        }
                        assertTrue(records.stream().filter(s -> s.contains(TableSpace.DEFAULT)).findAny().isPresent());
                        assertEquals(1, records.size());

                    }
                    try (ResultSet rs = metaData.getSchemas(null, "no")) {
                        List<List<String>> records = new ArrayList<>();
                        assertFalse(rs.next());

                    }
                }
            }
        }
    }

    @Test
    public void ensureWeThrowExceptionForNonSupportedNotNullTypes_Double() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                    Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement();
                    statement.execute("CREATE TABLE mytable (key string primary key, name string,d1 double not null)");
                } catch (SQLException ex) {
                    assertTrue(ex.getMessage().contains("StatementExecutionException"));
                    assertTrue(ex.getMessage().contains("Not null constraints not supported for column type 6"));
                }
            }
        }
    }

    @Test
    public void ensureWeThrowAppropriateExceptionWhenNotNullConstraintViolated_AddBatch_ExecuteBatch() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try {
                    BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                    Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement();
                    statement.execute("CREATE TABLE mytable (n1 int primary key auto_increment, name string not null)");
                    PreparedStatement insertPs = con.prepareStatement("INSERT INTO mytable (name) values(?)");
                    for (int i = 0; i < 10; i++) {
                        insertPs.setString(1, null);
                        insertPs.addBatch();
                    }
                    insertPs.executeBatch();
                } catch (SQLException ex) {
                    assertTrue(ex.getMessage().contains("StatementExecutionException"));
                    assertTrue(ex.getMessage().contains("Cannot have null value in non null type string"));
                }
            }
        }
    }
}
