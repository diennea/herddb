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

import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.LogNotAvailableException;
import herddb.server.ServerConfiguration;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * CommitLog on Apache BookKeeper
 *
 * @author enrico.olivelli
 */
public class BookkeeperCommitLogManager extends CommitLogManager {

    private final ZookeeperMetadataStorageManager metadataStorageManager;
    private int ensemble = 1;
    private int writeQuorumSize = 1;
    private int ackQuorumSize = 1;
    private BookKeeper bookKeeper;
    private final ClientConfiguration config;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;

    public BookkeeperCommitLogManager(ZookeeperMetadataStorageManager metadataStorageManager, ServerConfiguration serverConfiguration) {
        config = new ClientConfiguration();
        config.setThrottleValue(0);
        config.setZkServers(metadataStorageManager.getZkAddress());
        config.setZkTimeout(metadataStorageManager.getZkSessionTimeout());
        config.setEnableParallelRecoveryRead(true);
        config.setEnableDigestTypeAutodetection(true);
        LOG.log(Level.CONFIG, "Processing server config {0}", serverConfiguration);
        if (serverConfiguration.getBoolean("bookie.preferlocalbookie", false)) {
            config.setEnsemblePlacementPolicy(PreferLocalBookiePlacementPolicy.class);
        }
        for (String key : serverConfiguration.keys()) {
            if (key.startsWith("bookkeeper.")) {
                String _key = key.substring("bookkeeper.".length());
                String value = serverConfiguration.getString(key, null);
                LOG.log(Level.CONFIG, "Setting BookKeeper client configuration: {0}={1}", new Object[]{_key, value});
                config.setProperty(_key, value);
            }

        }
        LOG.config("BookKeeper client configuration:");
        for (Iterator e = config.getKeys(); e.hasNext();) {
            Object key = e.next();
            LOG.log(Level.CONFIG, "{0}={1}", new Object[]{key, config.getProperty(key + "")});
        }
        this.metadataStorageManager = metadataStorageManager;
    }

    @Override
    public void start() throws LogNotAvailableException {
        try {
            this.bookKeeper
                    = BookKeeper
                            .forConfig(config)
                            .build();
        } catch (IOException | InterruptedException | BKException t) {
            close();
            throw new LogNotAvailableException(t);
        }
    }

    @Override
    public void close() {
        if (bookKeeper != null) {
            try {
                bookKeeper.close();
            } catch (InterruptedException | BKException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }
    private static final Logger LOG = Logger.getLogger(BookkeeperCommitLogManager.class.getName());

    public int getEnsemble() {
        return ensemble;
    }

    public void setEnsemble(int ensemble) {
        this.ensemble = ensemble;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public void setWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    public void setAckQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
    }

    public long getLedgersRetentionPeriod() {
        return ledgersRetentionPeriod;
    }

    public void setLedgersRetentionPeriod(long ledgersRetentionPeriod) {
        this.ledgersRetentionPeriod = ledgersRetentionPeriod;
    }

    @Override
    public CommitLog createCommitLog(String tableSpace) throws LogNotAvailableException {
        BookkeeperCommitLog res = new BookkeeperCommitLog(tableSpace, metadataStorageManager, bookKeeper);
        res.setAckQuorumSize(ackQuorumSize);
        res.setEnsemble(ensemble);
        res.setLedgersRetentionPeriod(ledgersRetentionPeriod);
        res.setWriteQuorumSize(writeQuorumSize);
        return res;
    }

}
