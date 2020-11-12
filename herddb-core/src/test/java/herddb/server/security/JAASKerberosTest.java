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
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.RawString;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Demonstates the usage of the update "newvalue" facility to implement
 * atomic-counters
 *
 * @author enrico.olivelli
 */
public class JAASKerberosTest {

    private MiniKdc kdc;
    private Properties conf;

    @Rule
    public TemporaryFolder kdcDir = new TemporaryFolder();

    @Rule
    public TemporaryFolder workDir = new TemporaryFolder();

    @Before
    public void startMiniKdc() throws Exception {

        conf = MiniKdc.createConf();
        kdc = new MiniKdc(conf, kdcDir.getRoot());
        kdc.start();

        String localhostName = "localhost";
        String principalServerNoRealm = "herddb/" + localhostName;
        String principalServer = "herddb/" + localhostName + "@" + kdc.getRealm();
        String principalClientNoRealm = "herddbclient/" + localhostName;
        String principalClient = principalClientNoRealm + "@" + kdc.getRealm();

        System.out.println("adding principal: " + principalServerNoRealm);
        System.out.println("adding principal: " + principalClientNoRealm);

        File keytabClient = new File(workDir.getRoot(), "herddbclient.keytab");
        kdc.createPrincipal(keytabClient, principalClientNoRealm);

        File keytabServer = new File(workDir.getRoot(), "herddbserver.keytab");
        kdc.createPrincipal(keytabServer, principalServerNoRealm);

        File jaas_file = new File(workDir.getRoot(), "jaas.conf");
        try (FileWriter writer = new FileWriter(jaas_file)) {
            writer.write("\n"
                    + "HerdDBServer {\n"
                    + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                    + "  useKeyTab=true\n"
                    + "  keyTab=\"" + keytabServer.getAbsolutePath() + "\n"
                    + "  storeKey=true\n"
                    + "  useTicketCache=false\n"
                    + "  principal=\"" + principalServer + "\";\n"
                    + "};\n"
                    + "\n"
                    + "\n"
                    + "\n"
                    + "HerdDBClient {\n"
                    + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                    + "  useKeyTab=true\n"
                    + "  keyTab=\"" + keytabClient.getAbsolutePath() + "\n"
                    + "  storeKey=true\n"
                    + "  useTicketCache=false\n"
                    + "  principal=\"" + principalClient + "\";\n"
                    + "};\n"
            );

        }

        File krb5file = new File(workDir.getRoot(), "krb5.conf");
        try (FileWriter writer = new FileWriter(krb5file)) {
            writer.write("[libdefaults]\n"
                    + " default_realm = " + kdc.getRealm() + "\n"
                    // disable UDP as Kerby will listen only on TCP by default
                    + " udp_preference_limit=1\n"
                    + "\n"
                    + "[realms]\n"
                    + " " + kdc.getRealm() + "  = {\n"
                    + "  kdc = " + kdc.getHost() + ":" + kdc.getPort() + "\n"
                    + " }"
            );

        }

        System.setProperty("java.security.auth.login.config", jaas_file.getAbsolutePath());
        System.setProperty("java.security.krb5.conf", krb5file.getAbsolutePath());

    }

    @After
    public void stopMiniKdc() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("java.security.krb5.conf");
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        ServerConfiguration serverConfig = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverConfig.set(ServerConfiguration.PROPERTY_HOST, "localhost");
        try (Server server = new Server(serverConfig)) {
            server.start();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                Assert.assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO mytable (id,n1,n2) values(?,?,?)", 0, false, true, Arrays.asList("test_0", 1, 2)).updateCount);
                Map<RawString, Object> newValue = connection.executeUpdate(TableSpace.DEFAULT, "UPDATE mytable set n1= n1+1 where id=?", 0, true, true, Arrays.asList("test_0")).newvalue;
                assertEquals(Long.valueOf(2), newValue.get(RawString.of("n1")));

            }
        } finally {
            System.clearProperty("java.security.auth.login.config");
        }
    }
}
