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

import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.utils.Version;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * JDBC Driver
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
public class Driver implements java.sql.Driver, AutoCloseable {

    private static final Logger LOG = Logger.getLogger(Driver.class.getName());

    private static final Driver INSTANCE = new Driver();
    private static final DataSourceManager DATASOURCE_MANAGER = new DataSourceManager();

    static {
        final Runnable awaiter = PreloadClasses.run();
        try {
            DriverManager.registerDriver(INSTANCE, () -> {
                INSTANCE.close();
                awaiter.run();
            });
        } catch (SQLException error) {
            LOG.log(Level.SEVERE, "error while registring JDBC driver:" + error, error);
        }
    }

    public Driver() {
    }

    private static class DataSourceManager {

        private final HashMap<String, HerdDBEmbeddedDataSource> datasources = new HashMap<>();

        private synchronized HerdDBEmbeddedDataSource ensureDatasource(String url, Properties info) {
            String key = computeKey(url, info);
            HerdDBEmbeddedDataSource ds = datasources.get(key);
            if (ds != null) {
                return ds;
            }
            /**
             * DriverManager puts username/password in 'user' and 'password'
             * properties
             */
            ds = new HerdDBEmbeddedDataSource(info);
            ds.setUrl(url);
            if (!url.contains("poolConnections") && !info.containsKey("poolConnections")) {
                // default to not pooling connections if not configured explicitly
                // usually JDBC Driver users use their own pool
                ds.setPoolConnections(false);
            }
            final DataSourceManager dr = this;
            ds.setOnAutoClose(d -> {
                synchronized (dr) {
                    if (d.isAutoClose()) {
                        LOG.log(Level.INFO, "AutoClosing JDBC Driver created DS {0}", d);
                        d.close();
                        datasources.remove(key);
                    }
                }
            });
            LOG.log(Level.INFO, "JDBC Driver created DS {0}", ds);


            datasources.put(key, ds);
            return ds;
        }

        private synchronized void closeAll() {
            LOG.log(Level.INFO, "Unregistering HerdDB JDBC Driver {0}", this);
            datasources.forEach((key, ds) -> {
                LOG.log(Level.INFO, "Unregistering HerdDB JDBC Driver {0} Datasource ID {1}", new Object[]{this, key});
                ds.close();
            });
            datasources.clear();
        }

        /**
         * Closes drive embedded datasources by their connection url
         */
        public synchronized void closeDatasources(String url) {

            List<Entry<String, HerdDBEmbeddedDataSource>> entries = datasources.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(url + '#')).collect(Collectors.toList());

            for (Entry<String, HerdDBEmbeddedDataSource> entry : entries) {
                datasources.remove(entry.getKey());
                entry.getValue().close();
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        HerdDBEmbeddedDataSource datasource = DATASOURCE_MANAGER.ensureDatasource(url, info);
        return datasource.getConnection();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith("jdbc:herddb:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return Version.getJDBC_DRIVER_MAJOR_VERSION();
    }

    @Override
    public int getMinorVersion() {
        return Version.getJDBC_DRIVER_MINOR_VERSION();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOG;
    }

    private static String computeKey(String url, Properties info) {
        StringBuilder res = new StringBuilder(url);
        res.append('#');
        if (info != null) {
            List<String> keys = new ArrayList<>(info.stringPropertyNames());
            keys.sort(Comparator.naturalOrder());
            for (String key : keys) {
                String value = info.getProperty(key, "");
                res.append(key);
                res.append('=');
                res.append(value);
                res.append('.');
            }
        }
        return res.toString();
    }


    @Override
    public synchronized void close() {
        DATASOURCE_MANAGER.closeAll();
    }


    private static final class PreloadClasses {
        private PreloadClasses() {
            // no-op
        }

        public static Runnable run() {
            // todo: check if the loader "isRegisteredAsParallelCapable"? should be the case anyway these days
            if (Boolean.getBoolean("herddb.driver.preloadClasses.skip")) { // useless using CDS or native mode
                return () -> {};
            }

            final ClassLoader loader = ofNullable(Thread.currentThread().getContextClassLoader()).orElseGet(ClassLoader::getSystemClassLoader);
            final List<String> classes = asList(// classes with slow <cinit>
                    "org.apache.calcite.sql.fun.SqlStdOperatorTable",
                    "herddb.sql.CalcitePlanner",
                    "herddb.core.TableSpaceManager",
                    "herddb.client.ClientConfiguration",
                    "herddb.server.ServerConfiguration",
                    "org.apache.calcite.rel.metadata.JaninoRelMetadataProvider",
                    "org.apache.calcite.sql.validate.SqlValidator$Config",
                    "org.apache.calcite.tools.Programs",
                    "org.apache.calcite.plan.RelOptRules",
                    "org.apache.calcite.sql2rel.StandardConvertletTable"
            );
            final int threads = Math.min(classes.size(), Math.max(1, Runtime.getRuntime().availableProcessors()));
            final ExecutorService es = Executors.newFixedThreadPool(
                    threads,
                    new ThreadFactory() {
                        private final AtomicInteger counter = new AtomicInteger();

                        @Override
                        public Thread newThread(final Runnable r) {
                            return new Thread(r, "herddb-classes-preaload-" + counter.incrementAndGet());
                        }
                    });
            final CountDownLatch latch = new CountDownLatch(classes.size());
            for (final String name : classes) {
                es.execute(() -> {
                    try {
                        Class.forName(name, true, loader);
                    } catch (final Error | ClassNotFoundException cnfe) {
                        // skip
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return () -> {
                try {
                    latch.await();
                    es.shutdown();
                    es.awaitTermination(200, MILLISECONDS); // should be immediate
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                System.setProperty("herddb.driver.preloadClasses.skip", "true"); // in case, protected by classloading (Driver)
            };
        }
    }
}
