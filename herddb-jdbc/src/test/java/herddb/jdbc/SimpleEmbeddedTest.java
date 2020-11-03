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
import herddb.client.ClientConfiguration;
import herddb.server.ServerConfiguration;
import java.io.File;
import java.io.FileWriter;
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
public class SimpleEmbeddedTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        try (HerdDBEmbeddedDataSource dataSource = new HerdDBEmbeddedDataSource()) {
            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ClientConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_AUTODISCOVERY + "");
            try (Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement()) {
                statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));
                try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                    boolean found = false;
                    while (rs.next()) {
                        String key = rs.getString("key");
                        String name = rs.getString("name");
                        assertEquals("k1", key);
                        assertEquals("name1", name);
                        found = true;
                    }
                    assertTrue(found);
                }
            }
        }
    }

    @Test
    public void testAdditionalConfiguration() throws Exception {
        File baseDir = folder.newFolder();
        File file = new File(baseDir, "additionalConfiguration.txt");
        try (FileWriter w = new FileWriter(file);) {
            w.write("server.base.dir=" + baseDir.getAbsolutePath() + "\n");
            w.write("server.port=0\n");
            w.write("client.base.dir=" + baseDir.getAbsolutePath() + "\n");
            w.write("server.start=true");
        }

        try (HerdDBEmbeddedDataSource dataSource = new HerdDBEmbeddedDataSource()) {
            dataSource.setUrl("jdbc:herddb:server:localhost:7000?configFile=" + file.getAbsolutePath());
            try (Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement()) {
                assertEquals(baseDir.getAbsolutePath(), dataSource.getServer().getManager().getServerConfiguration().getString(ServerConfiguration.PROPERTY_BASEDIR, ""));
                assertEquals(baseDir.getAbsolutePath(), dataSource.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_BASEDIR, ""));
                statement.execute("CREATE TABLE mytable (key string primary key, name string)");
                assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));
                try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable")) {
                    boolean found = false;
                    while (rs.next()) {
                        String key = rs.getString("key");
                        String name = rs.getString("name");
                        assertEquals("k1", key);
                        assertEquals("name1", name);
                        found = true;
                    }
                    assertTrue(found);
                }
            }
        }
    }

}
