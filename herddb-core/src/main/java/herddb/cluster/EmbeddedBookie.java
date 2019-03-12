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
package herddb.cluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

import herddb.network.netty.NetworkUtils;
import herddb.server.ServerConfiguration;

/**
 * Utility for starting embedded Apache BookKeeper Server (Bookie)
 *
 * @author enrico.olivelli
 */
public class EmbeddedBookie implements AutoCloseable {

    private final static Logger LOG = Logger.getLogger(EmbeddedBookie.class.getName());

    private final Path baseDirectory;
    private final ServerConfiguration configuration;
    private final ZookeeperMetadataStorageManager metadataManager;
    private final StatsLogger statsLogger;

    private BookieServer bookieServer;


    public EmbeddedBookie(Path baseDirectory, ServerConfiguration configuration, ZookeeperMetadataStorageManager metadataManager) {
        this(baseDirectory, configuration, metadataManager, null);
    }

    public EmbeddedBookie(Path baseDirectory, ServerConfiguration configuration, ZookeeperMetadataStorageManager metadataManager, StatsLogger statsLogger) {
        this.baseDirectory = baseDirectory;
        this.configuration = configuration;
        this.metadataManager = metadataManager;
        this.statsLogger = statsLogger != null ? statsLogger : new NullStatsLogger();
    }

    public void start() throws Exception {
        org.apache.bookkeeper.conf.ServerConfiguration conf = new org.apache.bookkeeper.conf.ServerConfiguration();
        conf.setZkTimeout(metadataManager.getZkSessionTimeout());
        conf.setZkServers(metadataManager.getZkAddress());
        conf.setZkLedgersRootPath(metadataManager.getLedgersPath());
        conf.setStatisticsEnabled(true);
        int port = configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_BOOKIE_PORT, ServerConfiguration.PROPERTY_BOOKKEEPER_BOOKIE_PORT_DEFAULT);

        conf.setUseHostNameAsBookieID(true);
        Path bookie_dir = baseDirectory.resolve("bookie");
        if (port <= 0) {
            Integer _port = readLocalBookiePort(bookie_dir);
            if (_port == null) {
                _port = NetworkUtils.assignFirstFreePort();
                LOG.log(Level.SEVERE, "As configuration parameter "
                        + ServerConfiguration.PROPERTY_BOOKKEEPER_BOOKIE_PORT + " is {0},I have choosen to listen on port {1}."
                        + " Set to a positive number in order to use a fixed port", new Object[]{Integer.toString(port), Integer.toString(_port)});
                persistLocalBookiePort(bookie_dir, _port);
            }
            port = _port;
        }
        conf.setBookiePort(port);
        Files.createDirectories(bookie_dir);
        Path bookie_data_dir = bookie_dir.resolve("bookie_data").toAbsolutePath();
        Path bookie_journal_dir = bookie_dir.resolve("bookie_journal").toAbsolutePath();
        Files.createDirectories(bookie_data_dir);
        Files.createDirectories(bookie_journal_dir);
        conf.setLedgerDirNames(new String[]{bookie_data_dir.toString()});
        conf.setJournalDirName(bookie_journal_dir.toString());
        conf.setFlushInterval(1000);
        conf.setMaxBackupJournals(5);
        conf.setMaxJournalSizeMB(1048);
        conf.setNumAddWorkerThreads(8);
        conf.setMaxPendingReadRequestPerThread(200000); // new in 4.6
        conf.setMaxPendingAddRequestPerThread(200000); // new in 4.6
        conf.setEnableLocalTransport(true);
        conf.setProperty("journalMaxGroupWaitMSec", 10L); // default 200ms
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setAutoRecoveryDaemonEnabled(false);
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);

        for (String key : configuration.keys()) {
            if (key.startsWith("bookie.")) {
                String bookieConf = key.substring("bookie.".length());
                String value = configuration.getString(key, null);
                conf.addProperty(bookieConf, value);
                LOG.log(Level.CONFIG, "config {0} remapped to {1}={2}", new Object[]{key, bookieConf, value});
            }
        }
        long _start = System.currentTimeMillis();
        LOG.severe("Booting Apache Bookkeeper on port " + port);

        Files.createDirectories(bookie_dir);
        dumpBookieConfiguration(bookie_dir, conf);

        boolean forcemetaformat = configuration.getBoolean("bookie.forcemetaformat", false);
        LOG.log(Level.CONFIG, "bookie.forcemetaformat={0}", forcemetaformat);

        boolean result = BookKeeperAdmin.format(conf, false, forcemetaformat);
        if (result) {
            LOG.info("BookKeeperAdmin.format: created a new workspace on ZK");
        } else {
            LOG.info("BookKeeperAdmin.format: ZK space does not need an format operation");
        }

        boolean forceformat = configuration.getBoolean("bookie.forceformat", false);
        LOG.log(Level.CONFIG, "bookie.forceformat={0}", forceformat);
        if (forceformat) {
            result = Bookie.format(conf, false, forceformat);
            if (result) {
                LOG.info("Bookie.format: formatter applied to local bookie");
            } else {
                LOG.info("Bookie.format: local boookie did not need formatting");
            }
        }

        bookieServer = new BookieServer(conf, statsLogger);
        bookieServer.start();
        for (int i = 0; i < 100; i++) {
            if (bookieServer.getBookie().isRunning()) {
                LOG.info("Apache Bookkeeper started");
                break;
            }
            Thread.sleep(500);
        }
        long _stop = System.currentTimeMillis();
        LOG.severe("Booting Apache Bookkeeper finished. Time " + (_stop - _start) + " ms");

    }

    private void dumpBookieConfiguration(Path bookie_dir, org.apache.bookkeeper.conf.ServerConfiguration conf) throws IOException {
        // dump actual BookKeeper configuration in order to use bookkeeper shell
        Path actual_bookkeeper_configuration = bookie_dir.resolve("embedded.bookie.properties");
        StringBuilder builder = new StringBuilder();
        for (Iterator<String> key_it = conf.getKeys(); key_it.hasNext();) {
            String key = key_it.next() + "";
            Object value = conf.getProperty(key);
            if (value instanceof Collection) {
                value = ((Collection) value).stream().map(String::valueOf).collect(Collectors.joining(","));
            }
            builder.append(key + "=" + value + "\n");
        }
        Files.write(actual_bookkeeper_configuration, builder.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        LOG.severe(
                "Dumped actual Bookie configuration to " + actual_bookkeeper_configuration.toAbsolutePath());
    }

    @Override
    public void close() {
        if (bookieServer != null) {
            LOG.info("Apache Bookkeeper stopping");
            try {
                bookieServer.shutdown();

                bookieServer.join();
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            } finally {
                bookieServer = null;
            }
        }
    }

    public Integer readLocalBookiePort(Path dataPath) throws IOException {
        Path file = dataPath.resolve("bookie_port");
        try {
            LOG.log(Level.SEVERE, "Looking for local port into file {0}", file);
            if (!Files.isRegularFile(file)) {
                LOG.log(Level.SEVERE, "Cannot find file {0}", file);
                return null;
            }
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            for (String line : lines) {
                line = line.trim().toLowerCase();
                // skip comments and empty lines
                if (line.startsWith("#") || line.isEmpty()) {
                    continue;
                }
                int res = Integer.parseInt(line);
                LOG.log(Level.SEVERE, "Found local port {0} into file {1}", new Object[]{Integer.toString(res), file});
                return res;
            }
            throw new IOException("Cannot find any valid line inside file " + file.toAbsolutePath());
        } catch (IOException error) {
            LOG.log(Level.SEVERE, "Error while reading file " + file.toAbsolutePath(), error);
            throw error;
        }
    }

    public void persistLocalBookiePort(Path dataPath, int port) throws IOException {
        Files.createDirectories(dataPath);
        Path file = dataPath.resolve("bookie_port");
        StringBuilder message = new StringBuilder();
        message.append("# This file contains the port of the bookie used by this node\n");
        message.append("# Do not change the contents of this file, otherwise the beheaviour of the system will\n");
        message.append("# lead eventually to data loss\n");
        message.append("# \n");
        message.append("# Any line which starts with '#' and and blank line will be ignored\n");
        message.append("# The system will consider the first non-blank line as port\n");
        message.append("\n\n");
        message.append(port);
        Files.write(file, message.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
    }
}
