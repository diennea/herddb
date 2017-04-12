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
package herddb.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.DMLResult;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;

/**
 * Basic server/client boot test
 *
 * @author enrico.olivelli
 */
public class SimpleClientServerAutoTransactionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();;
        String _baseDir = baseDir.toString();
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                    HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                DMLResult executeUpdateResult = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", TransactionContext.AUTOTRANSACTION_ID, false, Arrays.asList("test", 1, 2));

                long tx = executeUpdateResult.transactionId;
                long countInsert = executeUpdateResult.updateCount;
                Assert.assertEquals(1, countInsert);


                GetResult res = connection.executeGet(TableSpace.DEFAULT,
                        "SELECT * FROM mytable WHERE id='test'", tx, Collections.emptyList());;
                Map<String, Object> record = res.data;
                Assert.assertNotNull(record);
                assertEquals("test", record.get("id"));
                assertEquals(Long.valueOf(1), record.get("n1"));
                assertEquals(Integer.valueOf(2), record.get("n2"));

                connection.commitTransaction(TableSpace.DEFAULT, tx);

                try (ScanResultSet scan = connection.executeScan(server.getManager().getVirtualTableSpaceId(), "SELECT * FROM sysconfig", Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        String name = (String) aa.get("name");
                        assertEquals("server.base.dir", name);
                        String value = (String) aa.get("value");
                        assertEquals(_baseDir, value);
                    }
                }

                try (ScanResultSet scan = connection.executeScan(null, "SELECT * FROM " + server.getManager().getVirtualTableSpaceId() + ".sysclients", Collections.emptyList(), 0, 0, 10);) {
                    List<Map<String, Object>> all = scan.consume();
                    for (Map<String, Object> aa : all) {
                        assertEquals("jvm-local", aa.get("address"));
                        assertEquals("1", aa.get("id") + "");
                        assertEquals(ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT, aa.get("username"));
                        assertNotNull(aa.get("connectionts"));
                    }
                    assertEquals(1, all.size());
                }

            }
        }
    }
}
