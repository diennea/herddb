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

import java.nio.file.Path;
import java.util.Properties;

/**
 * Server configuration
 *
 * @author enrico.olivelli
 */
public class ServerConfiguration {

    private final Properties properties;

    public static final String PROPERTY_NODEID = "nodeId";
    public static final String PROPERTY_BASEDIR = "baseDir";
    public static final String PROPERTY_HOST = "host";
    public static final String PROPERTY_PORT = "port";
    public static final String PROPERTY_SSL = "ssl";

    public ServerConfiguration(Properties properties) {
        this.properties = properties;
    }

    public ServerConfiguration(Path baseDir) {
        this();
        set(PROPERTY_BASEDIR, baseDir.toAbsolutePath());
    }

    public ServerConfiguration() {
        this.properties = new Properties();
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(this.properties.getProperty(key, defaultValue + ""));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.getBoolean(this.properties.getProperty(key, defaultValue + ""));
    }

    public long getLong(String key, long defaultValue) {
        return Long.parseLong(this.properties.getProperty(key, defaultValue + ""));
    }

    public String getString(String key, String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

    public ServerConfiguration set(String key, Object value) {
        if (value == null) {
            this.properties.remove(key);
        } else {
            this.properties.setProperty(key, value + "");
        }
        return this;
    }

}
