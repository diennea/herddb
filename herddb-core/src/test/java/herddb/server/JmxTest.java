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

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.model.TableSpace;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import javax.management.ObjectName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Demonstates the usage of the update "newvalue" facility to implement atomic-counters
 *
 * @author enrico.olivelli
 */
public class JmxTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                {
                    final ObjectName statusBeanName = new ObjectName("herddb.server:type=Table,Name=" + TableSpace.DEFAULT + ".mytable");
                    Object attribute = ManagementFactory.getPlatformMBeanServer().getAttribute(statusBeanName, "Tablesize");
                    assertEquals(0L, attribute);
                }
                {
                    final ObjectName statusBeanName = new ObjectName("herddb.server:type=TableSpace,Name=" + TableSpace.DEFAULT);
                    Object attribute = ManagementFactory.getPlatformMBeanServer().getAttribute(statusBeanName, "Tablesize");
                    assertEquals(0L, attribute);
                }

            }
        }
    }
}
