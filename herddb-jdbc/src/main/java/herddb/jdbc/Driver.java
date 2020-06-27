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
        try {
            DriverManager.registerDriver(INSTANCE, () -> {
                INSTANCE.close();
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
        StringBuilder res = new StringBuilder();
        res.append(url);
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


}
