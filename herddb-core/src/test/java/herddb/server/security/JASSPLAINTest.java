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

package herddb.server.security;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.model.TableSpace;
import herddb.security.sasl.SaslUtils;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Simple username/password authentication
 *
 * @author enrico.olivelli
 */
public class JASSPLAINTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();


            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath())
                    .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_PLAIN)
                    .set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, "sa")
                    .set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, "bad-password"));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList());
                fail();
            } catch (HDBException error) {
                assertTrue(error.getMessage().contains("Failed authentication for username"));
            }

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath())
                    .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_PLAIN)
                    .set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, "sa")
                    .set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, "hdb"));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, false, true, Arrays.asList("test_0", 1, 2)).updateCount);
                Map<RawString, Object> newValue = connection.executeUpdate(TableSpace.DEFAULT, "UPDATE mytable set n1= n1+1 where id=?", 0, true, true, Arrays.asList("test_0")).newvalue;
                assertEquals(Long.valueOf(2), newValue.get(RawString.of("n1")));

            }
        }
    }
}
