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
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * SimpleServer boot test
 *
 * @author enrico.olivelli
 */
public class UseSystemPropertiesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try {
            File tmpConfFile = folder.newFile("test.server.properties");

            try (InputStream in = UseSystemPropertiesTest.class.getResourceAsStream("/conf/test.server.properties")) {
                Properties props = new Properties();
                props.load(in);
                props.put(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
                try (FileOutputStream oo = new FileOutputStream(tmpConfFile)) {
                    props.store(oo, "");
                }
            }
            Thread runner = new Thread(() -> {
                System.setProperty(ServerConfiguration.PROPERTY_NODEID, "my-node-id");
                ServerMain.main(tmpConfFile.getAbsolutePath());
            });
            runner.start();
            while (ServerMain.getRunningInstance() == null
                    || !ServerMain.getRunningInstance().isStarted()) {
                Thread.sleep(1000);
                System.out.println("waiting for boot");
            }

            ServerMain.getRunningInstance().getServer().waitForStandaloneBoot();
            assertEquals("my-node-id", ServerMain.getRunningInstance().getServer().getNodeId());

            ServerMain.getRunningInstance().close();
        } finally {
            if (ServerMain.getRunningInstance() != null) {
                ServerMain.getRunningInstance().close();
            }
        }
    }
}
