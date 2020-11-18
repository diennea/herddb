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

import herddb.daemons.PidFileLocker;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper for standalone ZooKeeper server (for local demos/tests)
 *
 * @author enrico.olivelli
 */
public class ZooKeeperMainWrapper implements AutoCloseable {

    private final Properties configuration;
    private final PidFileLocker pidFileLocker;
    private ZooKeeperServerMain server;

    private static ZooKeeperMainWrapper runningInstance;

    public ZooKeeperMainWrapper(Properties configuration) {
        this.configuration = configuration;
        this.pidFileLocker = new PidFileLocker(Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath());
    }

    @Override
    public void close() {

    }

    public static void main(String... args) {
        try {
            String here = new File(System.getProperty("user.dir")).getAbsolutePath();
            LOG.error("Starting ZookKeeper version from HerdDB package version" + herddb.utils.Version.getVERSION());
            Properties configuration = new Properties();

            boolean configFileFromParameter = false;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("-")) {
                    File configFile = new File(args[i]).getAbsoluteFile();
                    LOG.error("Reading configuration from " + configFile);
                    try (InputStreamReader reader =
                                 new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
                        configuration.load(reader);
                    }
                    configFileFromParameter = true;
                } else if (arg.equals("--use-env")) {
                    System.getenv().forEach((key, value) -> {
                        System.out.println("Considering env as system property " + key + " -> " + value);
                        System.setProperty(key, value);
                    });
                } else if (arg.startsWith("-D")) {
                    int equals = arg.indexOf('=');
                    if (equals > 0) {
                        String key = arg.substring(2, equals);
                        String value = arg.substring(equals + 1);
                        System.setProperty(key, value);
                    }
                }
            }
            if (!configFileFromParameter) {
                File configFile = new File("conf/zoo.cfg").getAbsoluteFile();
                System.out.println("Reading configuration from " + configFile);
                if (configFile.isFile()) {
                    try (InputStreamReader reader = new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
                        configuration.load(reader);
                    }
                }
            }

            System.getProperties().forEach((k, v) -> {
                String key = k + "";
                if (!key.startsWith("java") && !key.startsWith("user")) {
                    configuration.put(k, v);
                }
            });

            for (Object key : configuration.keySet()) {
                String value = configuration.getProperty(key.toString());
                String newvalue = value.replace("${user.dir}", here);
                configuration.put(key, newvalue);
            }
            String datadir = configuration.getProperty("dataDir", null);
            if (datadir != null) {
                File file = new File(datadir);
                if (!file.isDirectory()) {
                    LOG.error("Creating directory " + file.getAbsolutePath());
                    boolean result = file.mkdirs();
                    if (!result) {
                        LOG.error("Failed to create directory " + file.getAbsolutePath());
                    }
                } else {
                    LOG.error("Using directory " + file.getAbsolutePath());
                }
            }

            Runtime.getRuntime().addShutdownHook(new Thread("ctrlc-hook") {

                @Override
                public void run() {
                    System.out.println("Ctrl-C trapped. Shutting down");
                    ZooKeeperMainWrapper _brokerMain = runningInstance;
                    if (_brokerMain != null) {
                        Runtime.getRuntime().halt(0);
                    }
                }

            });
            runningInstance = new ZooKeeperMainWrapper(configuration);
            runningInstance.run();

        } catch (Throwable t) {
            t.printStackTrace();
            Runtime.getRuntime().halt(0);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMainWrapper.class.getName());

    public void run() throws Exception {
        pidFileLocker.lock();

        server = new ZooKeeperServerMain();
        QuorumPeerConfig qp = new QuorumPeerConfig();
        qp.parseProperties(configuration);
        ServerConfig sc = new ServerConfig();
        sc.readFrom(qp);
        server.runFromConfig(sc);

    }
}
