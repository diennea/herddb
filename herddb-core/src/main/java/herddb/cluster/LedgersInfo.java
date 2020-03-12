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

import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Information on ledgers user by the broker
 *
 * @author enrico.olivelli
 */
public class LedgersInfo {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private List<Long> activeLedgers = new ArrayList<>();
    private List<Long> ledgersTimestamps = new ArrayList<>();
    private long firstLedger = -1;
    private int zkVersion = -1;

    @Override
    public synchronized String toString() {
        return "LedgersInfo{" + "activeLedgers=" + activeLedgers + ", firstLedger=" + firstLedger + ", zkVersion=" + zkVersion + '}';
    }

    public synchronized List<Long> getLedgersTimestamps() {
        return ledgersTimestamps;
    }

    public synchronized void setLedgersTimestamps(List<Long> ledgersTimestamps) {
        this.ledgersTimestamps = ledgersTimestamps;
    }

    public synchronized int getZkVersion() {
        return zkVersion;
    }

    public synchronized void setZkVersion(int zkVersion) {
        this.zkVersion = zkVersion;
    }

    public synchronized byte[] serialize() {
        try {

            VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream();
            MAPPER.writeValue(oo, this);
            return oo.toByteArray();
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    public static LedgersInfo deserialize(byte[] data, int zkVersion) {
        if (data == null || data.length == 0) {
            LedgersInfo info = new LedgersInfo();
            info.setZkVersion(zkVersion);
            return info;
        }
        try {
            LedgersInfo info = MAPPER.readValue(new SimpleByteArrayInputStream(data), LedgersInfo.class);
            info.setZkVersion(zkVersion);
            return info;
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }

    }

    public synchronized void addLedger(long id) {
        activeLedgers.add(id);
        ledgersTimestamps.add(System.currentTimeMillis());
        if (firstLedger < 0) {
            firstLedger = id;
        }
    }

    public synchronized void removeLedger(long id) throws IllegalArgumentException {
        int index = -1;
        for (int i = 0; i < activeLedgers.size(); i++) {
            if (activeLedgers.get(i) == id) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            throw new IllegalArgumentException("ledger " + id + " not in list " + activeLedgers);
        }
        activeLedgers.remove(index);
        ledgersTimestamps.remove(index);
    }

    public synchronized List<Long> getOldLedgers(long timestamp) throws IllegalArgumentException {
        List<Long> res = new ArrayList<>();
        for (int i = 0; i < activeLedgers.size(); i++) {
            long id = activeLedgers.get(i);
            long ledgerTimestamp = ledgersTimestamps.get(i);
            if (ledgerTimestamp < timestamp) {
                LOG.log(Level.INFO, "ledeger {0} is to be dropped, time is {1} < {2}", new Object[]{id, new java.sql.Timestamp(ledgerTimestamp), new java.sql.Timestamp(timestamp)});
                res.add(id);
            } else {
                LOG.log(Level.INFO, "ledeger {0} is to keep, time is {1} >= {2}", new Object[]{id, new java.sql.Timestamp(ledgerTimestamp), new java.sql.Timestamp(timestamp)});
            }
        }
        return res;
    }

    private static final Logger LOG = Logger.getLogger(LedgersInfo.class.getName());

    public synchronized List<Long> getActiveLedgers() {
        return new ArrayList<>(activeLedgers);
    }

    public synchronized void setActiveLedgers(List<Long> activeLedgers) {
        this.activeLedgers = activeLedgers;
    }

    public synchronized long getFirstLedger() {
        return firstLedger;
    }

    public synchronized void setFirstLedger(long firstLedger) {
        this.firstLedger = firstLedger;
    }

}
