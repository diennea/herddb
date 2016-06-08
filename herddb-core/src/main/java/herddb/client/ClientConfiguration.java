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
package herddb.client;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Client configuration
 *
 * @author enrico.olivelli
 */
public class ClientConfiguration {

    private final Properties properties;

    public static final String PROPERTY_BASEDIR = "client.baseDir";
    public static final String PROPERTY_TIMEOUT = "client.timeout";
    public static final String PROPERTY_CLIENTID = "client.clientId";
    public static final long PROPERTY_TIMEOUT_DEFAULT = 1000L * 60 * 5;
    public static final String PROPERTY_CLIENTID_DEFAULT = "localhost";

    public static final String PROPERTY_MODE = "client.mode";

    public static final String PROPERTY_MODE_LOCAL = "local";
    public static final String PROPERTY_MODE_STANDALONE = "standalone";
    public static final String PROPERTY_MODE_CLUSTER = "cluster";

    public static final String PROPERTY_SERVER_ADDRESS = "client.server.address";
    public static final String PROPERTY_SERVER_PORT = "client.server.port";
    public static final String PROPERTY_SERVER_SSL = "client.server.ssl";

    public static final String PROPERTY_ZOOKEEPER_ADDRESS = "client.zookeeper.address";
    public static final String PROPERTY_ZOOKEEPER_SESSIONTIMEOUT = "client.zookeeper.sessiontimeout";
    public static final String PROPERTY_ZOOKEEPER_PATH = "client.zookeeper.path";

    public static final String PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT = "localhost:1281";
    public static final String PROPERTY_ZOOKEEPER_PATH_DEFAULT = "/herd";
    public static final int PROPERTY_SERVER_PORT_DEFAULT = 7000;
    public static final String PROPERTY_SERVER_ADDRESS_DEFAULT = "localhost";
    public static final boolean PROPERTY_SERVER_SSL_DEFAULT = false;
    public static final int PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT = 40000;

    public ClientConfiguration(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    public ClientConfiguration(Path baseDir) {
        this();
        set(PROPERTY_BASEDIR, baseDir.toAbsolutePath());
    }

    public ClientConfiguration() {
        this.properties = new Properties();
        set(PROPERTY_BASEDIR, new File(System.getProperty("java.io.tmpdir")).toPath().toAbsolutePath());
    }

    public int getInt(String key, int defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Integer.parseInt(value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Boolean.parseBoolean(value);
    }

    public long getLong(String key, long defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Long.parseLong(value);
    }

    public String getString(String key, String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

    public ClientConfiguration set(String key, Object value) {
        if (value == null) {
            this.properties.remove(key);
        } else {
            this.properties.setProperty(key, value + "");
        }
        return this;
    }

    public void readJdbcUrl(String url) {
        if (url == null || url.isEmpty()) {
            return;
        }
        int questionMark = url.indexOf('?');
        if (questionMark <= 0) {
            questionMark = url.length();
        }
        String before = url.substring(0, questionMark);
        if (!before.startsWith("jdbc:herddb:")) {
            throw new IllegalArgumentException("invalid url " + url);
        }
        if (before.startsWith("jdbc:herddb:zookeeper:")) {
            set(PROPERTY_MODE, PROPERTY_MODE_CLUSTER);
            String zkaddress = before.substring("jdbc:herddb:zookeeper:".length());
            int slash = zkaddress.indexOf('/');
            if (slash <= 0) {
                set(PROPERTY_ZOOKEEPER_ADDRESS, zkaddress);
            } else {
                String path = zkaddress.substring(slash);
                zkaddress = zkaddress.substring(0, slash);
                set(PROPERTY_ZOOKEEPER_ADDRESS, zkaddress);
                set(PROPERTY_ZOOKEEPER_PATH, path);
            }
        } else if (before.startsWith("jdbc:herddb:server:")) {
            set(PROPERTY_MODE, PROPERTY_MODE_STANDALONE);
            before = before.substring("jdbc:herddb:server:".length());
            int port_pos = before.indexOf(':');
            String host = before;
            int port = PROPERTY_SERVER_PORT_DEFAULT;
            if (port_pos > 0) {
                host = before.substring(0, port_pos);
                port = Integer.parseInt(before.substring(port_pos + 1));
            }
            set(PROPERTY_SERVER_ADDRESS, host);
            set(PROPERTY_SERVER_PORT, port);
        } else if (before.startsWith("jdbc:herddb:local:")) {
            set(PROPERTY_MODE, PROPERTY_MODE_LOCAL);
        }
        String qs = url.substring(questionMark);
        String[] params = qs.split("&");
        for (String param : params) {
            // TODO: URLDecoder??
            int pos = param.indexOf('=');
            if (pos > 0) {
                String key = param.substring(0, pos);
                String value = param.substring(pos + 1);
                set(key, value);
            } else {
                set(param, "");
            }
        }
    }

}
