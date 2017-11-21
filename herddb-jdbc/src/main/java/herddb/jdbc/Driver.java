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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JDBC Driver
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
public class Driver implements java.sql.Driver, AutoCloseable {

    private static final Logger LOG = Logger.getLogger(Driver.class.getName());

    private static final Driver INSTANCE = new Driver();

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

    private final HashMap<String, HerdDBEmbeddedDataSource> datasources = new HashMap<>();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        HerdDBEmbeddedDataSource datasource = ensureDatasource(url, info);
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
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOG;
    }

    private synchronized HerdDBEmbeddedDataSource ensureDatasource(String url, Properties info) {
        String key = url + "_" + info;
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

    @Override
    public synchronized void close() {
        LOG.log(Level.SEVERE, "Unregistering HerdDB JDBC Driver");
        datasources.values().forEach(BasicHerdDBDataSource::close);
        datasources.clear();
    }

}
