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
import herddb.server.StaticClientSideMetadataProvider;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author enrico.olivelli
 */
public class MixedCaseIdentifiersTest {

    final static String CREATE_TABLE = "CREATE TABLE q1_MESSAGE (\n"
            + "  MSG_ID bigint NOT NULL PRIMARY KEY,\n"
            + "  STATUS tinyint,  \n"
            + "  RECIPIENT string,  \n"
            + "  SID int,  \n"
            + "  LAST_MODIFY_TIME timestamp,  \n"
            + "  NEXT_SEND_TIME timestamp,  \n"
            + "  LASTBOUNCECATEGORY tinyint null"
            + ")";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testUpdate() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (HerdDBDataSource dataSource = new HerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement create = con.createStatement();
                        PreparedStatement statement_insert = con.prepareStatement("INSERT INTO q1_MESSAGE(msg_id,STATUS,recipient) values(?,?,?)");
                        PreparedStatement statement_update = con.prepareStatement("UPDATE q1_MESSAGE SET STATUS = 2,lastbouncecategory=null WHERE MSG_ID = ? and (status=1 or status=5)");) {
                    create.execute(CREATE_TABLE);

                    long msg_id = 213;
                    statement_insert.setLong(1, msg_id);
                    statement_insert.setInt(2, 1);
                    statement_insert.setString(3, "test@localhost");
                    assertEquals(1, statement_insert.executeUpdate());

                    statement_update.setLong(1, msg_id);
                    assertEquals(1, statement_update.executeUpdate());

                    try (ResultSet rs = create.executeQuery("SELECT M.MSG_ID FROM q1_MESSAGE M WHERE 1=1 AND (M.RECIPIENT LIKE '%@localhost%')")) {
                        long _msg_id = -1;
                        int record_count = 0;
                        while (rs.next()) {
                            _msg_id = rs.getLong(1);
                            record_count++;
                        }
                        assertEquals(1, record_count);
                        assertTrue(_msg_id > 0);
                    }

                    try (PreparedStatement ps = con.prepareStatement("UPDATE q1_MESSAGE SET STATUS= 6,SID=?,LAST_MODIFY_TIME=?,NEXT_SEND_TIME = null, LASTBOUNCECATEGORY=? WHERE MSG_ID = ?")) {
                        ps.setInt(1, 1);
                        ps.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
                        ps.setInt(3, 2);

                        ps.setLong(4, msg_id);
                        assertEquals(1, ps.executeUpdate());

                    }
                    
                    
                    
                    try (ResultSet rs = create.executeQuery("SELECT M.MSG_ID FROM q1_MESSAGE M WHERE 1=1 AND status=6 and (M.RECIPIENT LIKE '%@localhost%')")) {
                        long _msg_id = -1;
                        int record_count = 0;
                        while (rs.next()) {
                            _msg_id = rs.getLong(1);
                            record_count++;
                        }
                        assertEquals(1, record_count);
                        assertTrue(_msg_id > 0);
                    }

                }
            }
        }

    }
}
