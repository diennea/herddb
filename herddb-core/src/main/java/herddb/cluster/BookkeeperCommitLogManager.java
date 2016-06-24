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
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;

    public BookkeeperCommitLogManager(ZookeeperMetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

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
        BookkeeperCommitLog res = new BookkeeperCommitLog(tableSpace, metadataStorageManager);
        res.setAckQuorumSize(ackQuorumSize);
        res.setEnsemble(ensemble);
        res.setLedgersRetentionPeriod(ledgersRetentionPeriod);
        res.setWriteQuorumSize(writeQuorumSize);
        return res;
    }

}
