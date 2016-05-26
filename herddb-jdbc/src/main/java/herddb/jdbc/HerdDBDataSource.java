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

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Generic DataSource for connecting to a remote HerdDB cluster
 *
 * @author enrico.olivelli
 */
public class HerdDBDataSource extends AbstractHerdDBDataSource {

    private static final Logger LOGGER = Logger.getLogger(HerdDBDataSource.class.getName());

    private ClientConfiguration clientConfiguration;
    private final Properties properties = new Properties();
    private String url;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public HerdDBDataSource() {
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    protected synchronized void ensureConnection() throws SQLException {
        if (clientConfiguration == null) {
            clientConfiguration = new ClientConfiguration(properties);
            clientConfiguration.readJdbcUrl(url);
        }
        if (client == null) {
            client = new HDBClient(clientConfiguration);
        }
        super.ensureConnection();
    }

    @Override
    public synchronized void close() {
        super.close();
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
