package herddb.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;

@RunWith(Parameterized.class)
public class SelectColumnTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Parameters( name = "{index}: {0}" )
    public static Collection<TestData> data() {
        final List<TestData> data = new ArrayList<>();

        data.add(new TestData("int", SelectColumnTest::checkInteger, new Object[][] { { 1, 1 }, { 0, null },
                { Integer.MAX_VALUE, Integer.MAX_VALUE }, { Integer.MIN_VALUE, Integer.MIN_VALUE } }));

        data.add(new TestData("long", SelectColumnTest::checkLong, new Object[][] { { 1L, 1L }, { 0L, null },
                { Long.MAX_VALUE, Long.MAX_VALUE }, { Long.MIN_VALUE, Long.MIN_VALUE } }));

        data.add(new TestData("string", SelectColumnTest::checkString,
                new Object[][] { { "1", "1" }, { "null", null }, { "tre", "tre" }, { "", "" } }));

        data.add(new TestData("timestamp", SelectColumnTest::checkTimestamp,
                new Object[][] { { new Timestamp(1), null }, { new Timestamp(0), new Timestamp(0) },
                        { new Timestamp(Long.MAX_VALUE), new Timestamp(Long.MAX_VALUE) } }));

        /* I tipi seguenti non supportano gli id */

        data.add(new TestData("int", "double", null, SelectColumnTest::checkDouble,
                new Object[][] { { 1, 1d }, { 0, null }, { 2, 2.2d }, { Integer.MAX_VALUE, Double.POSITIVE_INFINITY },
                        { Integer.MIN_VALUE, Double.NEGATIVE_INFINITY }, { -1, Double.NaN } }));

        data.add(new TestData("int", "boolean", null, SelectColumnTest::checkBoolean,
                new Object[][] { { 0, true }, { 1, false }, { 2, null } }));

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
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement statement = con.createStatement();) {
                    statement.execute("CREATE TABLE mytable (key " + data.keyType + " primary key, val " + data.valueType + ")");

                    try (PreparedStatement insert = con.prepareStatement("INSERT INTO mytable (key,val) values(?,?)")) {
                        for (Object[] odata : data.data) {
                            insert.setObject(1, odata[0]);
                            insert.setObject(2, odata[1]);

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
            return (checkKey() ? keyType : "-" ) + "/" + valueType;
        }

    }

    private static void checkInteger(ResultSet rs, Object expected) {
        try {
            Integer actual = rs.getInt(1);
            if (rs.wasNull()) {
                actual = null;
            }
            Assert.assertEquals(expected, actual);

            Object object = rs.getObject(1);
            Assert.assertEquals(expected, object);
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
            Assert.assertEquals(expected, actual);

            Object object = rs.getObject(1);
            Assert.assertEquals(expected, object);
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
            Assert.assertEquals(expected, actual);

            Object object = rs.getObject(1);
            Assert.assertEquals(expected, object);
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
            Assert.assertEquals(expected, actual);

            Object object = rs.getObject(1);
            Assert.assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkTimestamp(ResultSet rs, Object expected) {
        try {
            Timestamp actual = rs.getTimestamp(1);
            Assert.assertEquals(expected, actual);

            Object object = rs.getObject(1);
            Assert.assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkString(ResultSet rs, Object expected) {
        try {
            String actual = rs.getString(1);
            Assert.assertEquals(expected, actual);

            Object object = rs.getObject(1);
            Assert.assertEquals(expected, object);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
