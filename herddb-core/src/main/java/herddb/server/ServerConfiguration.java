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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Server configuration
 *
 * @author enrico.olivelli
 */
public final class ServerConfiguration {

    private final Properties properties;

    public static final String PROPERTY_NODEID = "server.node.id";
    public static final String PROPERTY_MODE = "server.mode";

    /**
     * Accept requests only for TableSpaces for which the local server is leader
     */
    public static final String PROPERTY_ENFORCE_LEADERSHIP = "server.enforce.leadership";
    public static final boolean PROPERTY_ENFORCE_LEADERSHIP_DEFAULT = true;

    public static final String PROPERTY_MODE_LOCAL = "local";
    public static final String PROPERTY_MODE_STANDALONE = "standalone";
    public static final String PROPERTY_MODE_CLUSTER = "cluster";

    public static final String PROPERTY_BASEDIR = "server.base.dir";
    public static final String PROPERTY_BASEDIR_DEFAULT = "dbdata";
    public static final String PROPERTY_DATADIR = "server.data.dir";
    public static final String PROPERTY_DATADIR_DEFAULT = "data";
    public static final String PROPERTY_LOGDIR = "server.log.dir";
    public static final String PROPERTY_LOGDIR_DEFAULT = "txlog";
    public static final String PROPERTY_TMPDIR = "server.tmp.dir";
    public static final String PROPERTY_TMPDIR_DEFAULT = "tmp";
    public static final String PROPERTY_METADATADIR = "server.metadata.dir";
    public static final String PROPERTY_METADATADIR_DEFAULT = "metadata";
    public static final String PROPERTY_ADVERTISED_HOST = "server.advertised.host";
    public static final String PROPERTY_ADVERTISED_PORT = "server.advertised.port";
    public static final String PROPERTY_HOST = "server.host";
    public static final String PROPERTY_HOST_AUTODISCOVERY = "";
    public static final String PROPERTY_HOST_DEFAULT = "localhost";
    public static final String PROPERTY_PORT = "server.port";
    public static final String PROPERTY_SSL = "server.ssl";

    public static final String PROPERTY_ZOOKEEPER_ADDRESS = "server.zookeeper.address";
    public static final String PROPERTY_ZOOKEEPER_SESSIONTIMEOUT = "server.zookeeper.session.timeout";
    public static final String PROPERTY_ZOOKEEPER_PATH = "server.zookeeper.path";

    public static final String PROPERTY_BOOKKEEPER_START = "server.bookkeeper.start";
    public static final boolean PROPERTY_BOOKKEEPER_START_DEFAULT = false;

    public static final String PROPERTY_BOOKKEEPER_BOOKIE_PORT = "server.bookkeeper.port";
    public static final int PROPERTY_BOOKKEEPER_BOOKIE_PORT_DEFAULT = 3181;

    public static final String PROPERTY_BOOKKEEPER_ENSEMBLE = "server.bookkeeper.ensemble.size";
    public static final int PROPERTY_BOOKKEEPER_ENSEMBLE_DEFAULT = 1;
    public static final String PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE = "server.bookkeeper.write.quorum.size";
    public static final int PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE_DEFAULT = 1;
    public static final String PROPERTY_BOOKKEEPER_ACKQUORUMSIZE = "server.bookkeeper.ack.quorum.size";
    public static final int PROPERTY_BOOKKEEPER_ACKQUORUMSIZE_DEFAULT = 1;
    public static final String PROPERTY_BOOKKEEPER_LOGRETENTION_PERIOD = "server.bookkeeper.log.retention.period";

    public static final String PROPERTY_LOG_RETENTION_PERIOD = "server.log.retention.period";
    public static final long PROPERTY_LOG_RETENTION_PERIOD_DEFAULT = 1000L * 60 * 60 * 24 * 2;

    public static final String PROPERTY_CHECKPOINT_PERIOD = "server.checkpoint.period";
    public static final long PROPERTY_CHECKPOINT_PERIOD_DEFAULT = 1000L * 60 * 15;

    /**
     * Maximum dirty bytes percentage at which a pages will be considered for rebuild during a checkpoint.
     * This value must be between 0 and 1.0. By default, the value is 0.25.
     */
    public static final String PROPERTY_DIRTY_PAGE_THRESHOLD = "server.checkpoint.page.dirty.max.threshold";
    public static final double PROPERTY_DIRTY_PAGE_THRESHOLD_DEFAULT = 0.25D;

    /**
     * Minimum byte fill percentage at which a pages will be considered for rebuild during a checkpoint.
     * This value must be between 0 and 1.0. By default, the value is 0.75D.
     */
    public static final String PROPERTY_FILL_PAGE_THRESHOLD = "server.checkpoint.page.fill.min.threshold";
    public static final double PROPERTY_FILL_PAGE_THRESHOLD_DEFAULT = 0.75D;

    /**
     * Maximum target time in milliseconds to spend during standard checkpoint operations. Checkpoint duration
     * could be longer than this to complete pages flush. If set to -1 checkpoints won't have a time limit. Be
     * aware that configuring this parameter to small values could impact performances on the long run
     * increasing pages pollution with dirty not reclaimed records: in many cases is safer to configure a
     * wider dirty page threshold. By default, the value is -1.
     */
    public static final String PROPERTY_CHECKPOINT_DURATION = "server.checkpoint.duration";
    public static final long PROPERTY_CHECKPOINT_DURATION_DEFAULT = -1;

    /**
     * Maximum target time in milliseconds to spend during standard checkpoint operations on compacting
     * smaller pages. Is should be less than the maximum checkpoint duration configured by
     * {@link #PROPERTY_CHECKPOINT_DURATION}. If set to -1 checkpoints won't have a time limit. Regardless his
     * value at least one page will be compacted for each checkpoint. By default, the value is 1000 ms.
     */
    public static final String PROPERTY_COMPACTION_DURATION = "server.checkpoint.compaction";
    public static final long PROPERTY_COMPACTION_DURATION_DEFAULT = 1000L;

    public static final String PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT = "localhost:1281";
    public static final String PROPERTY_ZOOKEEPER_PATH_DEFAULT = "/herd";
    public static final int PROPERTY_PORT_DEFAULT = 7000;
    public static final int PROPERTY_PORT_AUTODISCOVERY = 0;
    public static final int PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT = 40000;

    public static final String PROPERTY_USERS_FILE = "server.users.file";
    public static final String PROPERTY_USERS_FILE_DEFAULT = "";

    public static final String PROPERTY_CLEAR_AT_BOOT = "server.clear.at.boot";
    public static final boolean PROPERTY_CLEAR_AT_BOOT_DEFAULT = false;

    public static final String PROPERTY_SERVER_TO_SERVER_USERNAME = "server.username";
    public static final String PROPERTY_SERVER_TO_SERVER_PASSWORD = "server.password";

    public static final String PROPERTY_DISK_SWAP_MAX_RECORDS = "server.disk.swap.max.records";
    public static final int PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT = 10000;

    public static final String PROPERTY_MAX_LOGICAL_PAGE_SIZE = "server.memory.page.size";
    public static final long PROPERTY_MAX_LOGICAL_PAGE_SIZE_DEFAULT = 1 * 1024 * 1024;

    public static final String PROPERTY_HALT_ON_TABLESPACEBOOT_ERROR = "server.haltontablespacebooterror";
    public static final boolean PROPERTY_HALT_ON_TABLESPACEBOOT_ERROR_DEAULT = false;

    /**
     * Maximum memory usable by HerdDB. If 0 the system will try to use most of the RAM of the JVM. When you
     * are embedding the server in another process use this property in order to limit the usage of resources
     * by the database.
     */
    public static final String PROPERTY_MEMORY_LIMIT_REFERENCE = "server.memory.max.limit";
    public static final long PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT = 0L;

    public static final String PROPERTY_PLANSCACHE_MAXMEMORY = "server.memory.planscache.limit";
    public static final long PROPERTY_PLANSCACHE_MAXMEMORY_DEFAULT = 50 * 1024 * 1024L;

    /**
     * Maximum amount of memory used for data pages
     */
    public static final String PROPERTY_MAX_DATA_MEMORY = "server.memory.data.limit";
    public static final long PROPERTY_MAX_DATA_MEMORY_DEFAULT = 0L;

    /**
     * Maximum amount of memory used for primary index pages
     */
    public static final String PROPERTY_MAX_PK_MEMORY = "server.memory.pk.limit";
    public static final long PROPERTY_MAX_PK_MEMORY_DEFAULT = 0L;


    public static final String PROPERTY_JMX_ENABLE = "server.jmx.enable";
    public static final boolean PROPERTY_JMX_ENABLE_DEFAULT = true;

    public ServerConfiguration(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    public ServerConfiguration(Path baseDir) {
        this();
        set(PROPERTY_BASEDIR, baseDir.toAbsolutePath());
    }

    /**
     * Copy configuration
     *
     * @return
     */
    public ServerConfiguration copy() {
        Properties copy = new Properties();
        copy.putAll(this.properties);
        return new ServerConfiguration(copy);
    }

    public ServerConfiguration() {
        this.properties = new Properties();
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Boolean.parseBoolean(value);
    }

    public int getInt(String key, int defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Integer.parseInt(value);
    }

    public long getLong(String key, long defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Long.parseLong(value);
    }

    public double getDouble(String key, double defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Double.parseDouble(value);
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

    @Override
    public String toString() {
        return "ServerConfiguration{" + "properties=" + properties + '}';
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
            int port = PROPERTY_PORT_DEFAULT;
            if (port_pos > 0) {
                host = before.substring(0, port_pos);
                port = Integer.parseInt(before.substring(port_pos + 1));
            }
            set(PROPERTY_HOST, host);
            set(PROPERTY_PORT, port);
        } else if (before.startsWith("jdbc:herddb:local")) {
            set(PROPERTY_MODE, PROPERTY_MODE_LOCAL);
        }
        if (questionMark < url.length()) {
            String qs = url.substring(questionMark + 1);
            String[] params = qs.split("&");
            LOG.log(Level.SEVERE, "url " + url + " qs " + qs + " params " + Arrays.toString(params));
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
    private static final Logger LOG = Logger.getLogger(ServerConfiguration.class.getName());

    public List<String> keys() {
        return properties.keySet().stream().map(Object::toString).sorted().collect(Collectors.toList());
    }

}
