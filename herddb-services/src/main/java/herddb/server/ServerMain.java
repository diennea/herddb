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

import static java.util.Locale.ROOT;
import static java.util.stream.Collectors.toMap;
import herddb.daemons.PidFileLocker;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusServlet;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by enrico.olivelli on 23/03/2015.
 */
public class ServerMain implements AutoCloseable {

    static {
        // see https://github.com/netty/netty/pull/7650
        if (System.getProperty("io.netty.tryReflectionSetAccessible") == null) {
            System.setProperty("io.netty.tryReflectionSetAccessible", "true");
        }
    }

    private final Properties configuration;
    private final PidFileLocker pidFileLocker;
    private Server server;
    private org.eclipse.jetty.server.Server httpserver;
    private boolean started;
    private String uiurl;
    private String metricsUrl;

    private static ServerMain runningInstance;

    public ServerMain(Properties configuration) {
        this.configuration = configuration;
        this.pidFileLocker = new PidFileLocker(Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath());
    }

    @Override
    public void close() {

        if (server != null) {
            try {
                server.close();
            } catch (Exception ex) {
                LoggerFactory.getLogger(ServerMain.class.getName()).error(null, ex);
            } finally {
                server = null;
            }
        }
        if (httpserver != null) {
            try {
                httpserver.stop();
            } catch (Exception ex) {
                LoggerFactory.getLogger(ServerMain.class.getName()).error(null, ex);
            } finally {
                httpserver = null;
            }
        }
        pidFileLocker.close();
        running.countDown();
    }

    public static void main(String... args) {
        if (Boolean.parseBoolean(System.getenv("HERDDB_USE_ENV"))) {
            useEnv();
        }
        try {
            LOG.info("Starting HerdDB version {}", herddb.utils.Version.getVERSION());
            Properties configuration = new Properties();

            boolean configFileFromParameter = false;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("-")) {
                    File configFile = new File(args[i]).getAbsoluteFile();
                    LOG.info("Reading configuration from {}", configFile);
                    try (InputStreamReader reader = new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8)) {
                        configuration.load(reader);
                    }
                    configFileFromParameter = true;
                } else if (arg.equals("--use-env")) {
                    useEnv();
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
                File configFile = new File("conf/server.properties").getAbsoluteFile();
                LOG.info("Reading configuration from {}", configFile);
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
            Runtime.getRuntime().addShutdownHook(new Thread("ctrlc-hook") {

                @Override
                public void run() {
                    System.out.println("Ctrl-C trapped. Shutting down");
                    ServerMain _brokerMain = runningInstance;
                    if (_brokerMain != null) {
                        _brokerMain.close();
                    }
                }

            });
            runningInstance = new ServerMain(configuration);
            runningInstance.start();

            runningInstance.join();

        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void useEnv() {
        // herddb_env_foo_bar -> foo.bar=xxx which enables to fully configure herddb this way
        System.getProperties().putAll(System.getenv().entrySet().stream()
                .filter(e -> e.getKey().toUpperCase(ROOT).startsWith("HERDDB_ENV_"))
                .collect(toMap(e -> e.getKey().substring("HERDDB_ENV_".length()).replace('_', '.'), Map.Entry::getValue)));
    }

    private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class.getName());

    public boolean isStarted() {
        return started;
    }

    private static final CountDownLatch running = new CountDownLatch(1);

    public static ServerMain getRunningInstance() {
        return runningInstance;
    }

    public Server getServer() {
        return server;
    }

    public void join() {
        try {
            running.await();
        } catch (InterruptedException discard) {
        }
        started = false;
    }

    public void start() throws Exception {
        pidFileLocker.lock();
        PrometheusMetricsProvider statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsProviderConfig = new PropertiesConfiguration();
        statsProviderConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        configuration.forEach((key, value) -> {
            statsProviderConfig.setProperty(key + "", value);
        });
        statsProvider.start(statsProviderConfig);

        ServerConfiguration config = new ServerConfiguration(this.configuration);

        StatsLogger statsLogger = statsProvider.getStatsLogger("");
        server = new Server(config, statsLogger);
        server.start();

        boolean httpEnabled = config.getBoolean("http.enable", true);
        if (httpEnabled) {
            String httphost = config.getString("http.host", server.getNetworkServer().getHost());
            String httpadvertisedhost = config.getString("http.advertised.host", server.getServerHostData().getHost());
            int httpport = config.getInt("http.port", 9845);
            int httpadvertisedport = config.getInt("http.advertised.port", httpport);

            httpserver = new org.eclipse.jetty.server.Server(new InetSocketAddress(httphost, httpport));
            ContextHandlerCollection contexts = new ContextHandlerCollection();
            httpserver.setHandler(contexts);

            ServletContextHandler contextRoot = new ServletContextHandler(ServletContextHandler.GZIP);
            contextRoot.setContextPath("/");
            contextRoot.addServlet(new ServletHolder(new PrometheusServlet(statsProvider)), "/metrics");
            contexts.addHandler(contextRoot);

            File webUi = new File("web/ui");
            if (webUi.isDirectory()) {
                WebAppContext webApp = new WebAppContext(new File("web/ui").getAbsolutePath(), "/ui");
                contexts.addHandler(webApp);
            } else {
                System.out.println("Cannot find " + webUi.getAbsolutePath() + " directory. Web UI will not be deployed");
            }

            uiurl = "http://" + httpadvertisedhost + ":" + httpadvertisedport + "/ui/#/login?url=" + server.getJdbcUrl();
            metricsUrl = "http://" + httpadvertisedhost + ":" + httpadvertisedport + "/metrics";
            System.out.println("Listening for client (http) connections on " + httphost + ":" + httpport);
            httpserver.start();
        }

        System.out.println("HerdDB server starter. Node id " + server.getNodeId());
        System.out.println("JDBC URL: " + server.getJdbcUrl());
        System.out.println("Web Interface: " + uiurl);
        System.out.println("Metrics: " + metricsUrl);
        started = true;
    }

    public String getUiurl() {
        return uiurl;
    }

    public String getMetricsUrl() {
        return metricsUrl;
    }


}
