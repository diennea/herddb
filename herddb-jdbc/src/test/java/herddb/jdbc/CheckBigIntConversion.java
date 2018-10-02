package herddb.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;

public class CheckBigIntConversion {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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
                    statement.execute("CREATE TABLE mytable (key BIGINT primary key)");

                    /* With high enough longs the value was converted to double loosing "precision" */
                    long value = 72057598332895235L;
                    try (PreparedStatement insert = con.prepareStatement(
                            "INSERT INTO mytable (key) values(" + Long.toString(value) + ")")) {
                        insert.executeUpdate();
                    }

                    try (PreparedStatement selectKey = con.prepareStatement("SELECT key FROM mytable")) {
                        try (ResultSet rs = selectKey.executeQuery()) {
                            while (rs.next()) {
                                Long result = (Long) rs.getObject(1);

                                if (result.longValue() != value) {
                                    throw new IllegalStateException(Long.toString(value) + " != " + result);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
