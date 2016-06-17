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

import herddb.server.ServerConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.proto.BookieServer;

/**
 * Utility for starting embedded Apache BookKeeper Server (Bookie)
 *
 * @author enrico.olivelli
 */
public class EmbeddedBookie implements AutoCloseable {

    private final static Logger LOGGER = Logger.getLogger(EmbeddedBookie.class.getName());
    private final ServerConfiguration configuration;
    private final Path baseDirectory;
    private BookieServer bookieServer;

    public EmbeddedBookie(Path baseDirectory, ServerConfiguration configuration) {
        this.configuration = configuration;
        this.baseDirectory = baseDirectory;
    }

    public void start() throws Exception {

        org.apache.bookkeeper.conf.ServerConfiguration conf = new org.apache.bookkeeper.conf.ServerConfiguration();
        conf.setZkTimeout(configuration.getInt(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT));
        conf.setZkServers(configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT));
        conf.setBookiePort(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_BOOKIE_PORT,ServerConfiguration.PROPERTY_BOOKKEEPER_BOOKIE_PORT_DEFAULT));
        conf.setUseHostNameAsBookieID(true);
        Path bookie_dir = baseDirectory.resolve("bookie");
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
        conf.setEnableLocalTransport(true);
        conf.setProperty("journalMaxGroupWaitMSec", (long) 10L); // default 200ms
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setAutoRecoveryDaemonEnabled(true);
        conf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);

        for (String key : configuration.keys()) {
            if (key.startsWith("bookie.")) {
                String bookieConf = key.substring("bookie.".length());
                String value = configuration.getString(key, null);
                conf.addProperty(bookieConf, value);
                LOGGER.log(Level.CONFIG, "config {0} remapped to {1}={2}", new Object[]{key, bookieConf, value});
            }
        }
        System.out.println("Booting Apache Bookkeeper");

        boolean forcemetaformat = configuration.getBoolean("bookie.forcemetaformat", false);
        LOGGER.log(Level.CONFIG, "bookie.forcemetaformat={0}", forcemetaformat);

        org.apache.bookkeeper.conf.ClientConfiguration adminConf = new org.apache.bookkeeper.conf.ClientConfiguration(conf);
        boolean result = BookKeeperAdmin.format(adminConf, false, forcemetaformat);
        if (result) {
            LOGGER.info("BookKeeperAdmin.format: created a new workspace on ZK");
        } else {
            LOGGER.info("BookKeeperAdmin.format: ZK space does not need an format operation");
        }

        boolean forceformat = configuration.getBoolean("bookie.forceformat", false);
        LOGGER.log(Level.CONFIG, "bookie.forceformat={0}", forceformat);
        if (forceformat) {
            result = Bookie.format(conf, false, forceformat);
            if (result) {
                LOGGER.info("Bookie.format: formatter applied to local bookie");
            } else {
                LOGGER.info("Bookie.format: local boookie did not need formatting");
            }
        }

        bookieServer = new BookieServer(conf);
        bookieServer.start();
        for (int i = 0; i < 100; i++) {
            if (bookieServer.getBookie().isRunning()) {
                LOGGER.info("Apache Bookkeeper started");
                break;
            }
            Thread.sleep(500);
        }
    }

    @Override
    public void close() {
        if (bookieServer != null) {
            LOGGER.info("Apache Bookkeeper stopping");
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
}
