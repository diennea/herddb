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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Information on ledgers user by the broker
 *
 * @author enrico.olivelli
 */
public class LedgersInfo {

    private List<Long> activeLedgers = new ArrayList<>();
    private List<Long> ledgersTimestamps = new ArrayList<>();
    private long firstLedger = -1;
    private int zkVersion = -1;

    @Override
    public String toString() {
        return "LedgersInfo{" + "activeLedgers=" + activeLedgers + ", firstLedger=" + firstLedger + ", zkVersion=" + zkVersion + '}';
    }

    public List<Long> getLedgersTimestamps() {
        return ledgersTimestamps;
    }

    public void setLedgersTimestamps(List<Long> ledgersTimestamps) {
        this.ledgersTimestamps = ledgersTimestamps;
    }

    public int getZkVersion() {
        return zkVersion;
    }

    public void setZkVersion(int zkVersion) {
        this.zkVersion = zkVersion;
    }

    public byte[] serialize() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            mapper.writeValue(oo, this);
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
            ObjectMapper mapper = new ObjectMapper();
            LedgersInfo info = mapper.readValue(new ByteArrayInputStream(data), LedgersInfo.class);
            info.setZkVersion(zkVersion);
            return info;
        } catch (Exception impossible) {
            throw new RuntimeException(impossible);
        }

    }

    public void addLedger(long id) {
        activeLedgers.add(id);
        ledgersTimestamps.add(System.currentTimeMillis());
        if (firstLedger < 0) {
            firstLedger = id;
        }
    }

    public void removeLedger(long id) throws IllegalArgumentException {
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

    public List<Long> getOldLedgers(long timestamp) throws IllegalArgumentException {
        List<Long> res = new ArrayList<>();
        for (int i = 0; i < activeLedgers.size(); i++) {
            long id = activeLedgers.get(i);
            long ledgerTimestamp = ledgersTimestamps.get(i);
            if (ledgerTimestamp < timestamp) {
                res.add(id);
            }
        }
        return res;
    }

    public List<Long> getActiveLedgers() {
        return activeLedgers;
    }

    public void setActiveLedgers(List<Long> activeLedgers) {
        this.activeLedgers = activeLedgers;
    }

    public long getFirstLedger() {
        return firstLedger;
    }

    public void setFirstLedger(long firstLedger) {
        this.firstLedger = firstLedger;
    }

}
