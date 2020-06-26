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

import static org.junit.Assert.assertEquals;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StreamBasedWritesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);  Connection con = dataSource.getConnection();  Statement statement = con.createStatement()) {
                    statement.execute("CREATE TABLE mytable ("
                            + "key int primary key,"
                            + "valstring string,"
                            + "valbinary blob"
                            + ")");

                    String myString = "mystring";
                    byte[] myBinary = "mybinary".getBytes(StandardCharsets.UTF_8);

                    String myShortString = "my";
                    byte[] myShortBinary = "my".getBytes(StandardCharsets.UTF_8);

                    Set<Integer> shorterValues = new HashSet<>();
                    try (PreparedStatement insert = con.prepareStatement("INSERT INTO mytable (key,valstring,valbinary ) values(?,?,?)")) {

                        int i = 0;

                        insert.setInt(1, i);
                        insert.setString(2, myString);
                        insert.setBytes(3, myBinary);
                        insert.executeUpdate();

                        i++;

                        insert.setInt(1, i);
                        insert.setCharacterStream(2, new StringReader(myString));
                        insert.setBinaryStream(3, new ByteArrayInputStream(myBinary));
                        insert.executeUpdate();

                        i++;

                        insert.setInt(1, i);
                        insert.setNCharacterStream(2, new StringReader(myString));
                        insert.setBinaryStream(3, new ByteArrayInputStream(myBinary));
                        insert.executeUpdate();

                        i++;

                        insert.setInt(1, i);
                        insert.setCharacterStream(2, new StringReader(myString), 2);
                        insert.setBinaryStream(3, new ByteArrayInputStream(myBinary), 2);
                        insert.executeUpdate();
                        shorterValues.add(i);

                        i++;

                        insert.setInt(1, i);
                        insert.setNCharacterStream(2, new StringReader(myString), 2);
                        insert.setBinaryStream(3, new ByteArrayInputStream(myBinary), 2L);
                        insert.executeUpdate();
                        shorterValues.add(i);

                        i++;

                        insert.setInt(1, i);
                        insert.setClob(2, new StringReader(myString));
                        insert.setBlob(3, new ByteArrayInputStream(myBinary));
                        insert.executeUpdate();

                        i++;

                        insert.setInt(1, i);
                        insert.setClob(2, new StringReader(myString), 2);
                        insert.setBlob(3, new ByteArrayInputStream(myBinary), 2);
                        insert.executeUpdate();
                        shorterValues.add(i);

                        i++;

                        insert.setInt(1, i);
                        insert.setAsciiStream(2, new ByteArrayInputStream(myString.getBytes(StandardCharsets.US_ASCII)), 2);
                        insert.setBlob(3, new ByteArrayInputStream(myBinary), 2);
                        insert.executeUpdate();
                        shorterValues.add(i);

                        i++;

                        insert.setInt(1, i);
                        insert.setAsciiStream(2, new ByteArrayInputStream(myString.getBytes(StandardCharsets.US_ASCII)));
                        insert.setBlob(3, new ByteArrayInputStream(myBinary));
                        insert.executeUpdate();

                        i++;

                        insert.setInt(1, i);
                        insert.setAsciiStream(2, new ByteArrayInputStream(myString.getBytes(StandardCharsets.US_ASCII)), 2L);
                        insert.setBlob(3, new ByteArrayInputStream(myBinary), 2);
                        insert.executeUpdate();
                        shorterValues.add(i);

                        i++;

                        insert.setInt(1, i);
                        insert.setUnicodeStream(2, new ByteArrayInputStream(myString.getBytes(StandardCharsets.US_ASCII)), 2);
                        insert.setBlob(3, new ByteArrayInputStream(myBinary), 2);
                        insert.executeUpdate();
                        shorterValues.add(i);

                    }

                    try (PreparedStatement ps = con.prepareStatement("SELECT key,valstring,valbinary"
                            + " FROM mytable ORDER BY key");  ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            int key = rs.getInt(1);
                            if (shorterValues.contains(key)) {
                                checkString(rs, 2, myShortString, key);
                                checkBlob(rs, 3, myShortBinary, key);
                            } else {
                                checkString(rs, 2, myString, key);
                                checkBlob(rs, 3, myBinary, key);
                            }
                        }
                    }

                }
            }
        }
    }

    private static void checkString(ResultSet rs, int index, Object expected, int key) {
        try {
            String actual = rs.getString(index);
            Assert.assertEquals("error at key " + key, expected, actual);

            Object object = rs.getObject(index);
            Assert.assertEquals("error at key " + key, expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkBlob(ResultSet rs, int index, byte[] expected, int key) {
        try {
            byte[] actual = rs.getBytes(index);
            Assert.assertArrayEquals(expected, actual);

            if (expected != null) {
                DataInputStream dataInputStream = new DataInputStream(rs.getBinaryStream(index));
                byte[] actualFromStream = new byte[actual.length];
                dataInputStream.readFully(actualFromStream);
                Assert.assertArrayEquals("error at key " + key, expected, actualFromStream);

                Blob blob = rs.getBlob(index);
                assertEquals("error at key " + key, blob.length(), actual.length);

                DataInputStream dataInputStream2 = new DataInputStream(blob.getBinaryStream());
                byte[] actualFromStream2 = new byte[actual.length];
                dataInputStream2.readFully(actualFromStream2);
                Assert.assertArrayEquals("error at key " + key, expected, actualFromStream2);
            }

            byte[] object = (byte[]) rs.getObject(index);
            Assert.assertArrayEquals((byte[]) expected, object);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
