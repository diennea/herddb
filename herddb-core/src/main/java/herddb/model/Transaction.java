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
import java.util.Collections;
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
    public final Map<String, List<Record>> newRecords;
    public final Map<String, List<Bytes>> deletedRecords;
    public final Map<String, Table> newTables;

    public Transaction(long transactionId, String tableSpace) {
        this.transactionId = transactionId;
        this.tableSpace = tableSpace;
        this.locks = new HashMap<>();
        this.changedRecords = new HashMap<>();
        this.newRecords = new HashMap<>();
        this.deletedRecords = new HashMap<>();
        this.newTables = new HashMap<>();
    }

    public Map<String, Table> getNewTables() {
        return newTables;
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

    public void registerNewTable(Table table) {
        newTables.put(table.name, table);
    }

    public void registerInsertOnTable(String tableName, Bytes key, Bytes value) {
        List<Record> ll = newRecords.get(tableName);
        if (ll == null) {
            ll = new ArrayList<>();
            newRecords.put(tableName, ll);
        }
        ll.add(new Record(key, value));

        List<Bytes> deleted = deletedRecords.get(tableName);
        if (deleted != null) {
            deleted.remove(key);
        }

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

    /**
     *
     * @param tableName
     * @param key
     * @param value if null this is a DELETE
     */
    public void registerRecoredUpdate(String tableName, Bytes key, Bytes value) {
        List<Record> ll = changedRecords.get(tableName);
        if (ll == null) {
            ll = new ArrayList<>();
            changedRecords.put(tableName, ll);
        }

        for (int i = 0; i < ll.size(); i++) {
            Record r = ll.get(i);
            if (r.key.equals(key)) {
                if (value == null) {
                    // DELETE
                    ll.remove(i);
                    return;
                } else {
                    // MULTIPLE UPDATE ON SAME KEY
                    ll.set(i, new Record(key, value));
                    return;
                }
            }
        }
        if (value != null) {
            // FIRST UPDATE OF A KEY IN CURRENT TRANSACTION
            // record was not found in current transaction buffer
            ll.add(new Record(key, value));
        }
    }

    public void registerDeleteOnTable(String tableName, Bytes key) {
        Transaction.this.registerRecoredUpdate(tableName, key, null);

        List<Bytes> ll = deletedRecords.get(tableName);
        if (ll == null) {
            ll = new ArrayList<>();
            deletedRecords.put(tableName, ll);
        }
        for (int i = 0; i < ll.size(); i++) {
            Bytes r = ll.get(i);
            if (r.equals(key)) {
                //already tracked the delete
                break;
            }
        }
        ll.add(key);

    }

    public boolean recordDeleted(String tableName, Bytes key) {
        List<Bytes> deleted = deletedRecords.get(tableName);
        return deleted != null && deleted.contains(key);

    }

    public Record recordInserted(String tableName, Bytes key) {
        List<Record> inserted = newRecords.get(tableName);
        if (inserted == null) {
            return null;
        }
        return inserted.stream().filter(r -> r.key.equals(key)).findAny().orElse(null);

    }

    public Record recordUpdated(String tableName, Bytes key) {
        List<Record> updated = changedRecords.get(tableName);
        if (updated == null) {
            return null;
        }
        return updated.stream().filter(r -> r.key.equals(key)).findAny().orElse(null);

    }

    public Iterable<Record> getNewRecordsForTable(String tableName) {
        List<Record> inserted = newRecords.get(tableName);
        if (inserted == null) {
            return Collections.emptyList();
        } else {
            return inserted;
        }
    }

}
