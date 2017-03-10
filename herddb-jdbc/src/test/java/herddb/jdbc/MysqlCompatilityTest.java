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
import java.sql.SQLException;
import java.sql.Statement;
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
                    
                    statement.executeUpdate("INSERT INTO `sm_machine` VALUES"
                        + "('10.168.10.106',26,36,9,NULL,1),"
                        + "('10.168.10.107',26,31,10,NULL,1),"
                        + "('10.168.10.108',26,36,11,NULL,2),"
                        + "('10.168.10.109',33,38,10,NULL,3),"
                        + "('10.168.10.110',33,38,10,NULL,4);");

                    statement.executeQuery("SELECT ip, `offset` FROM sm_machine WHERE `offset` = 1").close();

                    statement.execute("DROP TABLE sm_machine;");
                    try {
                        statement.executeQuery("SELECT COUNT(*) FROM sm_machine").close();
                        fail();
                    } catch (SQLException err) {
                        assertTrue(err.getMessage().contains(herddb.model.TableDoesNotExistException.class.getName()));
                    }

                }
            }
        }

    }
}
