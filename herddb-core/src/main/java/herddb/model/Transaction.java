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
package herddb.model;

import herddb.utils.Bytes;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A Transaction, that is a series of Statement which must be executed with ACID
 * semantics on a set of tables of the same TableSet
 *
 * @author enrico.olivelli
 */
public class Transaction {

    public final long transactionId;
    public final String tableSpace;
    public final Map<String, List<LockHandle>> locks;
    public final Map<String, List<Record>> changedRecords;
    public final Map<String, List<Bytes>> newRecords;

    public Transaction(long transactionId, String tableSpace) {
        this.transactionId = transactionId;
        this.tableSpace = tableSpace;
        this.locks = new HashMap<>();
        this.changedRecords = new HashMap<>();
        this.newRecords = new HashMap<>();
    }

    public List<Record> getChangedRecordsForTable(String tableName) {
        return changedRecords.get(tableName);
    }

    public List<Bytes> getNewRecordsForTable(String tableName) {
        return newRecords.get(tableName);
    }

    public LockHandle lookupLock(String tableName, Bytes key) {
        List<LockHandle> ll = locks.get(tableName);
        if (ll == null) {
            return null;
        }
        for (LockHandle l : ll) {
            if (l.key.equals(key)) {
                return l;
            }
        }
        return null;
    }

    public void registerLockOnTable(String tableName, LockHandle handle) {
        List<LockHandle> ll = locks.get(tableName);
        if (ll == null) {
            ll = new ArrayList<>();
            locks.put(tableName, ll);
        }
        ll.add(handle);
    }

    public void registerChangedOnTable(String tableName, Record record) {
        List<Record> ll = changedRecords.get(tableName);
        if (ll == null) {
            ll = new ArrayList<>();
            changedRecords.put(tableName, ll);
        }
        ll.add(record);
    }

    public void registerInsertOnTable(String tableName, Bytes key) {
        List<Bytes> ll = newRecords.get(tableName);
        if (ll == null) {
            ll = new ArrayList<>();
            newRecords.put(tableName, ll);
        }
        ll.add(key);
    }

    public void releaseLocksOnTable(String tableName, LocalLockManager lockManager) {
        List<LockHandle> ll = locks.get(tableName);
        if (ll != null) {
            for (LockHandle l : ll) {
                lockManager.releaseLock(l);
            }
        }
    }

    public void unregisterUpgradedLocksOnTable(String tableName, LockHandle lock) {
        List<LockHandle> ll = locks.get(tableName);
        if (ll != null) {
            for (Iterator<LockHandle> it = ll.iterator(); it.hasNext();) {
                LockHandle next = it.next();
                if (next == lock) {
                    it.remove();
                    break;
                }
            }
        }
    }

}
