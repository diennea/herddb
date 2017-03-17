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
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Transactions, implicit transaction creation tests
 *
 * @author enrico.olivelli
 */
public class MysqlCompatilityTest {

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
                    Connection con2 = dataSource.getConnection();
                    Statement statement = con.createStatement();) {

                    statement.execute("DROP TABLE IF EXISTS `sm_machine`;");
                    statement.execute("CREATE TABLE `sm_machine` (\n"
                        + "  `ip` varchar(20) NOT NULL DEFAULT '',\n"
                        + "  `firefox_version` int(50) DEFAULT NULL,\n"
                        + "  `chrome_version` int(50) DEFAULT NULL,\n"
                        + "  `ie_version` int(50) DEFAULT NULL,\n"
                        + "  `log` varchar(2000) DEFAULT NULL,\n"
                        + "  `offset` int(50) DEFAULT NULL,\n"
                        + "  PRIMARY KEY (`ip`)\n"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");

                    int multiInsertCount = statement.executeUpdate("INSERT INTO `sm_machine` VALUES"
                        + "('10.168.10.106',26,36,9,NULL,1),"
                        + "('10.168.10.107',26,31,10,NULL,1),"
                        + "('10.168.10.108',26,36,11,NULL,2),"
                        + "('10.168.10.109',33,38,10,NULL,3),"
                        + "('10.168.10.110',33,38,10,NULL,4);");
                    assertEquals(5, multiInsertCount);

                    try (PreparedStatement ps = statement.getConnection().prepareStatement("INSERT INTO `sm_machine` VALUES"
                        + "(?,?,?,?,?,?),"
                        + "(?,?,?,?,?,?),"
                        + "(?,?,?,?,?,?),"
                        + "(?,?,?,?,?,?),"
                        + "(?,?,?,?,?,?)")) {
                        int i = 1;
                        ps.setString(i++, "11.168.10.106");
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, null);
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, "11.168.10.107");
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, null);
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, "11.168.10.108");
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, null);
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, "11.168.10.109");
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, null);
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, "11.168.10.110");
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.setString(i++, null);
                        ps.setInt(i++, 1);
                        ps.setInt(i++, 1);
                        ps.execute();
                        int multiInsertCount2 = ps.getUpdateCount();
                        assertEquals(5, multiInsertCount2);
                    }

                    statement.executeQuery("SELECT ip, `offset` FROM sm_machine WHERE `offset` = 1").close();

                    statement.execute("DROP TABLE sm_machine;");
                    try {
                        statement.executeQuery("SELECT COUNT(*) FROM sm_machine").close();
                        fail();
                    } catch (SQLException err) {
                        assertTrue(err.getMessage().contains(herddb.model.TableDoesNotExistException.class.getName()));
                    }

                    statement.execute("CREATE TABLE test_odd_names (\n"
                        + "  value varchar(20) NOT NULL DEFAULT '',\n"
                        + "  PRIMARY KEY (`value`)\n"
                        + ")");

                    statement.executeUpdate("INSERT INTO test_odd_names VALUES"
                        + "('10.168.10.106')");

                    try (ResultSet rs = statement.executeQuery("SELECT value FROM test_odd_names WHERE `value` = '10.168.10.106'")) {
                        assertTrue(rs.next());
                        assertEquals("10.168.10.106", rs.getString(1));
                        assertEquals("10.168.10.106", rs.getString("value"));
                    }

                }
            }
        }

    }
}
