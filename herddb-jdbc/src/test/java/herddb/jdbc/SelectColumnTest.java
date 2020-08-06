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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SelectColumnTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Parameters(name = "{index}: {0}")
    public static Collection<TestData> data() {
        final List<TestData> data = new ArrayList<>();

        data.add(new TestData("int", SelectColumnTest::checkInteger, new Object[][]{{1, 1}, {0, null},
                {Integer.MAX_VALUE, Integer.MAX_VALUE}, {Integer.MIN_VALUE, Integer.MIN_VALUE}}));

        data.add(new TestData("long", SelectColumnTest::checkLong, new Object[][]{{1L, 1L}, {0L, null},
                {Long.MAX_VALUE, Long.MAX_VALUE}, {Long.MIN_VALUE, Long.MIN_VALUE}}));

        data.add(new TestData("string", SelectColumnTest::checkString,
                new Object[][]{{"1", "1"}, {"null", null}, {"tre", "tre"}, {"", ""}}));

        data.add(new TestData("blob", SelectColumnTest::checkBlob,
                new Object[][]{{"1".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8)}, {"null".getBytes(StandardCharsets.UTF_8), null},
                    {"tre".getBytes(StandardCharsets.UTF_8), "tre".getBytes(StandardCharsets.UTF_8)}, {"".getBytes(StandardCharsets.UTF_8), "".getBytes(StandardCharsets.UTF_8)}}));

        data.add(new TestData("timestamp", SelectColumnTest::checkTimestamp,
                new Object[][]{{new Timestamp(1), null}, {new Timestamp(0), new Timestamp(0)},
                        {new Timestamp(Long.MAX_VALUE), new Timestamp(Long.MAX_VALUE)}}));

        data.add(new TestData("time", SelectColumnTest::checkTime,
                new Object[][]{{new Time(1), null}, {new Time(0), new Time(0)},
                        {new Time(Long.MAX_VALUE), new Time(Long.MAX_VALUE)}}));

        data.add(new TestData("date", SelectColumnTest::checkDate,
                new Object[][]{{new java.sql.Date(1), null}, {new java.sql.Date(0), new java.sql.Date(0)},
                        {new java.sql.Date(Long.MAX_VALUE), new java.sql.Date(Long.MAX_VALUE)}}));

        /* PK not suported for these types, using 'int' */

        data.add(new TestData("int", "double", null, SelectColumnTest::checkDouble,
                new Object[][]{{1, 1d}, {0, null}, {2, 2.2d}, {Integer.MAX_VALUE, Double.POSITIVE_INFINITY},
                        {Integer.MIN_VALUE, Double.NEGATIVE_INFINITY}, {-1, Double.NaN}}));

         data.add(new TestData("int", "double", null, SelectColumnTest::checkBigDecimal,
                new Object[][]{{1, BigDecimal.valueOf(1d)}, {0, null}, {2, BigDecimal.valueOf(2.2d)}, {Integer.MAX_VALUE, BigDecimal.TEN},
                        {Integer.MIN_VALUE, BigDecimal.TEN}, {-1, BigDecimal.ONE}}));

        data.add(new TestData("int", "boolean", null, SelectColumnTest::checkBoolean,
                new Object[][]{{0, true}, {1, false}, {2, null}}));

         // use getBoolean on "int" column
        data.add(new TestData("int", "int", null, SelectColumnTest::checkBooleanOnIntColumn,
                new Object[][]{{0, true}, {1, false}, {2, null}}));

        data.add(new TestData("int", "long", null, SelectColumnTest::checkBooleanOnLongColumn,
                new Object[][]{{0L, true}, {1L, false}, {2L, null}}));

        return data;

    }

    private final TestData data;

    public SelectColumnTest(TestData data) {
        super();
        this.data = data;
    }

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
                    statement.execute("CREATE TABLE mytable (key " + data.keyType + " primary key, val " + data.valueType + ")");

                    try (PreparedStatement insert = con.prepareStatement("INSERT INTO mytable (key,val) values(?,?)")) {
                        for (Object[] odata : data.data) {
                            insert.setObject(1, odata[0]);
                            setParameterAccordingToJavaType(insert, 2, odata[1]);

                            assertEquals(1, insert.executeUpdate());
                        }
                    }

                    if (data.checkKey()) {
                        try (PreparedStatement selectKey = con.prepareStatement("SELECT key FROM mytable WHERE key = ?")) {
                            for (Object[] odata : data.data) {

                                selectKey.setObject(1, odata[0]);

                                try (ResultSet rs = selectKey.executeQuery()) {
                                    int count = 0;
                                    while (rs.next()) {
                                        data.keyChecker.accept(rs, odata[0]);
                                        count++;
                                    }
                                    assertEquals(1, count);
                                }
                            }
                        }
                    }

                    try (PreparedStatement selectKey = con.prepareStatement("SELECT val FROM mytable WHERE key = ?")) {
                        for (Object[] odata : data.data) {

                            selectKey.setObject(1, odata[0]);

                            try (ResultSet rs = selectKey.executeQuery()) {
                                int count = 0;
                                while (rs.next()) {
                                    data.valueChecker.accept(rs, odata[1]);
                                    count++;
                                }
                                assertEquals(1, count);
                            }
                        }
                    }

                }
            }
        }
    }

    private void setParameterAccordingToJavaType(final PreparedStatement insert, int index, Object odata) throws SQLException {
        if (odata instanceof String) {
            insert.setString(index, (String) odata);
        } else if (odata instanceof java.sql.Time) {
            insert.setTime(index, (java.sql.Time) odata);
        } else if (odata instanceof java.sql.Date) {
            insert.setDate(index, (java.sql.Date) odata);
        } else if (odata instanceof java.sql.Timestamp) {
            insert.setTimestamp(index, (java.sql.Timestamp) odata);
        } else if (odata instanceof Long) {
            insert.setLong(index, (Long) odata);
        } else if (odata instanceof Double) {
            insert.setDouble(index, (Double) odata);
        } else if (odata instanceof Integer) {
            insert.setInt(index, (Integer) odata);
        } else if (odata instanceof Boolean) {
            insert.setBoolean(index, (Boolean) odata);
        } else if (odata instanceof byte[]) {
            insert.setBytes(index, (byte[]) odata);
        } else if (odata instanceof BigDecimal) {
            insert.setBigDecimal(index, (BigDecimal) odata);
        } else if (odata == null) {
            insert.setNull(index, Types.NULL);
        } else {
            fail(odata.getClass().toString());
        }
    }

    public static final class TestData {

        private final String keyType;
        private final String valueType;
        private final BiConsumer<ResultSet, Object> keyChecker;
        private final BiConsumer<ResultSet, Object> valueChecker;
        private final Object[][] data;

        public TestData(String type, BiConsumer<ResultSet, Object> checker, Object[][] data) {
            this(type, type, checker, checker, data);
        }

        public TestData(String keyType, String valueType, BiConsumer<ResultSet, Object> keyChecker, BiConsumer<ResultSet, Object> valueChecker, Object[][] data) {
            super();

            this.keyType = keyType;
            this.valueType = valueType;
            this.keyChecker = keyChecker;
            this.valueChecker = valueChecker;
            this.data = data;
        }

        public boolean checkKey() {
            return keyChecker != null;
        }

        @Override
        public String toString() {
            return (checkKey() ? keyType : "-") + "/" + valueType;
        }

    }

    private static void checkInteger(ResultSet rs, Object expected) {
        try {
            Integer actual = rs.getInt(1);
            if (rs.wasNull()) {
                actual = null;
            }
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkLong(ResultSet rs, Object expected) {
        try {
            Long actual = rs.getLong(1);
            if (rs.wasNull()) {
                actual = null;
            }
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkDouble(ResultSet rs, Object expected) {
        try {
            Double actual = rs.getDouble(1);
            if (rs.wasNull()) {
                actual = null;
            }
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkBigDecimal(ResultSet rs, Object expected) {
        try {
            BigDecimal actual = rs.getBigDecimal(1);
            Object object = rs.getObject(1);
            if (expected != null) {
                // BigDecimal.equals considers 'scale' and so 10.0 != 10
                assertTrue(actual.compareTo((BigDecimal) expected) == 0);
                // getObject returns double
                assertEquals(((BigDecimal) expected).doubleValue(), object);
            } else {
                assertNull(actual);
                assertNull(object);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

     private static void checkBooleanOnIntColumn(ResultSet rs, Object expected) {
        try {
            Boolean actual = rs.getBoolean(1);
            if (rs.wasNull()) {
                actual = null;
            }
            assertEquals(expected, actual);

            // getObject will return a java.lang.Integer
            Object object = rs.getObject(1);
            if (actual == null) {
                assertNull(object);
            } else {
                assertEquals(actual ? 1 : 0, object);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
     }
     private static void checkBooleanOnLongColumn(ResultSet rs, Object expected) {
        try {
            Boolean actual = rs.getBoolean(1);
            if (rs.wasNull()) {
                actual = null;
            }
            assertEquals(expected, actual);

            // getObject will return a java.lang.Integer
            Object object = rs.getObject(1);
            if (actual == null) {
                assertNull(object);
            } else {
                assertEquals(actual ? 1L : 0L, object);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkBoolean(ResultSet rs, Object expected) {
        try {
            Boolean actual = rs.getBoolean(1);
            if (rs.wasNull()) {
                actual = null;
            }
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkTimestamp(ResultSet rs, Object expected) {
        try {
            Timestamp actual = rs.getTimestamp(1);
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkTime(ResultSet rs, Object expected) {
        try {
            Time actual = rs.getTime(1);
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            // getObject will return java.sql.Timestamp
            if (expected != null) {
                assertEquals(new java.sql.Timestamp(((java.sql.Time) expected).getTime()), object);
            } else {
                assertNull(object);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkDate(ResultSet rs, Object expected) {
        try {
            java.sql.Date actual = rs.getDate(1);
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            // getObject will return java.sql.Timestamp
            if (expected != null) {
                assertEquals(new java.sql.Timestamp(((java.sql.Date) expected).getTime()), object);
            } else {
                assertNull(object);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkString(ResultSet rs, Object expected) {
        try {
            String actual = rs.getString(1);
            assertEquals(expected, actual);

            Object object = rs.getObject(1);
            assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkBlob(ResultSet rs, Object expected) {
        try {
            byte[] actual = rs.getBytes(1);
            assertArrayEquals((byte[]) expected, actual);

            if (expected != null) {
                DataInputStream dataInputStream = new DataInputStream(rs.getBinaryStream(1));
                byte[] actualFromStream = new byte[actual.length];
                dataInputStream.readFully(actualFromStream);
                assertArrayEquals((byte[]) expected, actualFromStream);

                Blob blob = rs.getBlob(1);
                assertEquals(blob.length(), actual.length);

                DataInputStream dataInputStream2 = new DataInputStream(blob.getBinaryStream());
                byte[] actualFromStream2 = new byte[actual.length];
                dataInputStream2.readFully(actualFromStream2);
                assertArrayEquals((byte[]) expected, actualFromStream2);
            }

            byte[] object = (byte[]) rs.getObject(1);
            assertArrayEquals((byte[]) expected, object);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
