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

import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import java.io.IOException;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Transaction, that is a series of Statement which must be executed with ACID
 * semantics on a set of tables of the same TableSet
 *
 * @author enrico.olivelli
 */
public class Transaction {

    public final long transactionId;
    public final String tableSpace;
    public final Map<String, Map<Bytes, LockHandle>> locks;
    public final Map<String, List<Record>> changedRecords;
    public final Map<String, List<Record>> newRecords;
    public final Map<String, List<Bytes>> deletedRecords;
    public Map<String, Table> newTables;
    public Map<String, Index> newIndexes;
    public Set<String> droppedTables;
    public Set<String> droppedIndexes;
    public LogSequenceNumber lastSequenceNumber;

    public Transaction(long transactionId, String tableSpace, LogSequenceNumber lastSequenceNumber) {
        this.transactionId = transactionId;
        this.tableSpace = tableSpace;
        this.locks = new HashMap<>();
        this.changedRecords = new HashMap<>();
        this.newRecords = new HashMap<>();
        this.deletedRecords = new HashMap<>();
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public LockHandle lookupLock(String tableName, Bytes key) {
        Map<Bytes, LockHandle> ll = locks.get(tableName);
        if (ll == null) {
            return null;
        }
        return ll.get(key);
    }

    public void registerLockOnTable(String tableName, LockHandle handle) {
        Map<Bytes, LockHandle> ll = locks.get(tableName);
        if (ll == null) {
            ll = new HashMap<>();
            locks.put(tableName, ll);
        }
        ll.put(handle.key, handle);
    }

    public synchronized void registerNewTable(Table table, LogSequenceNumber sequenceNumber) {
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        this.lastSequenceNumber = sequenceNumber;
        if (newTables == null) {
            newTables = new HashMap<>();
        }
        newTables.put(table.name, table);
        if (droppedTables == null) {
            droppedTables = new HashSet<>();
        }
        droppedTables.remove(table.name);
    }

    public synchronized void registerNewIndex(Index index, LogSequenceNumber sequenceNumber) {
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        this.lastSequenceNumber = sequenceNumber;
        if (newIndexes == null) {
            newIndexes = new HashMap<>();
        }
        newIndexes.put(index.name, index);
        if (droppedIndexes == null) {
            droppedIndexes = new HashSet<>();
        }
        droppedIndexes.remove(index.name);
    }

    public synchronized void registerInsertOnTable(String tableName, Bytes key, Bytes value, LogSequenceNumber sequenceNumber) {
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        this.lastSequenceNumber = sequenceNumber;
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
        Map<Bytes, LockHandle> ll = locks.get(tableName);
        if (ll != null) {
            for (LockHandle l : ll.values()) {
                lockManager.releaseLock(l);
            }
        }
    }

    public void unregisterUpgradedLocksOnTable(String tableName, LockHandle lock) {
        Map<Bytes, LockHandle> ll = locks.get(tableName);
        if (ll != null) {
            for (Iterator<Map.Entry<Bytes, LockHandle>> it = ll.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Bytes, LockHandle> next = it.next();
                if (next.getValue() == lock) {
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
    public synchronized void registerRecordUpdate(String tableName, Bytes key, Bytes value, LogSequenceNumber sequenceNumber) {
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        this.lastSequenceNumber = sequenceNumber;
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

    public synchronized void registerDeleteOnTable(String tableName, Bytes key, LogSequenceNumber sequenceNumber) {        
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        registerRecordUpdate(tableName, key, null, sequenceNumber);

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

    @Override
    public String toString() {
        return "Transaction{" + "transactionId=" + transactionId + ", tableSpace=" + tableSpace + ", locks=" + locks + ", changedRecords=" + changedRecords + ", newRecords=" + newRecords + ", deletedRecords=" + deletedRecords + ", newTables=" + newTables + ", newIndexes=" + newIndexes + '}';
    }

    public synchronized void registerDropTable(String tableName, LogSequenceNumber sequenceNumber) {
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        this.lastSequenceNumber = sequenceNumber;
        if (newTables == null) {
            newTables = new HashMap<>();
        }
        newTables.remove(tableName);
        if (droppedTables == null) {
            droppedTables = new HashSet<>();
        }
        droppedTables.add(tableName);
    }

    public synchronized void registerDropIndex(String indexName, LogSequenceNumber sequenceNumber) {
        if (lastSequenceNumber.after(sequenceNumber)) {
            return;
        }
        this.lastSequenceNumber = sequenceNumber;
        if (newIndexes == null) {
            newIndexes = new HashMap<>();
        }
        newIndexes.remove(indexName);
        if (droppedIndexes == null) {
            droppedIndexes = new HashSet<>();
        }
        droppedIndexes.add(indexName);
    }

    public boolean isTableDropped(String tableName) {
        return droppedTables != null && droppedTables.contains(tableName) && (newTables == null || !newTables.containsKey(tableName));
    }

    public boolean isIndexDropped(String indexName) {
        return droppedIndexes != null && droppedIndexes.contains(indexName) && (newIndexes == null || !newIndexes.containsKey(indexName));
    }

    public synchronized void serialize(ExtendedDataOutputStream out) throws IOException {
        out.writeInt(0); // flags        
        out.writeLong(transactionId);
        out.writeLong(lastSequenceNumber.ledgerId);
        out.writeLong(lastSequenceNumber.offset);
        out.writeVInt(changedRecords.size());
        for (Map.Entry<String, List<Record>> table : changedRecords.entrySet()) {
            out.writeUTF(table.getKey());
            out.writeVInt(table.getValue().size());
            for (Record r : table.getValue()) {
                out.writeArray(r.key.data);
                out.writeArray(r.value.data);
            }
        }
        out.writeVInt(newRecords.size());
        for (Map.Entry<String, List<Record>> table : newRecords.entrySet()) {
            out.writeUTF(table.getKey());
            out.writeVInt(table.getValue().size());
            for (Record r : table.getValue()) {
                out.writeArray(r.key.data);
                out.writeArray(r.value.data);
            }
        }
        out.writeVInt(deletedRecords.size());
        for (Map.Entry<String, List<Bytes>> table : deletedRecords.entrySet()) {
            out.writeUTF(table.getKey());
            out.writeVInt(table.getValue().size());
            for (Bytes key : table.getValue()) {
                out.writeArray(key.data);
            }
        }
        if (newTables == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(newTables.size());
            for (Table table : newTables.values()) {
                out.writeArray(table.serialize());
            }
        }
        if (droppedTables == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(droppedTables.size());
            for (String table : droppedTables) {
                out.writeUTF(table);
            }
        }
        if (newIndexes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(newIndexes.size());
            for (Index index : newIndexes.values()) {
                out.writeArray(index.serialize());
            }
        }
        if (droppedIndexes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(droppedIndexes.size());
            for (String index : droppedIndexes) {
                out.writeUTF(index);
            }
        }

    }

    public static Transaction deserialize(String tableSpace, ExtendedDataInputStream in) throws IOException {
        in.readInt(); // flags
        long id = in.readLong();
        long ledgerId = in.readLong();
        long offset = in.readLong();
        LogSequenceNumber lastSequenceNumber = new LogSequenceNumber(ledgerId, offset);
        Transaction t = new Transaction(id, tableSpace, lastSequenceNumber);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String table = in.readUTF();
            int numRecords = in.readVInt();
            List<Record> records = new ArrayList<>(numRecords);
            for (int k = 0; k < numRecords; k++) {
                byte[] key = in.readArray();
                byte[] value = in.readArray();
                records.add(new Record(Bytes.from_array(key), Bytes.from_array(value)));
            }
            t.changedRecords.put(table, records);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String table = in.readUTF();
            int numRecords = in.readVInt();
            List<Record> records = new ArrayList<>(numRecords);
            for (int k = 0; k < numRecords; k++) {
                byte[] key = in.readArray();
                byte[] value = in.readArray();
                records.add(new Record(Bytes.from_array(key), Bytes.from_array(value)));
            }
            t.newRecords.put(table, records);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String table = in.readUTF();
            int numRecords = in.readVInt();
            List<Bytes> records = new ArrayList<>(numRecords);
            for (int k = 0; k < numRecords; k++) {
                byte[] key = in.readArray();
                records.add(Bytes.from_array(key));
            }
            t.deletedRecords.put(table, records);
        }

        size = in.readVInt();
        if (size > 0) {
            t.newTables = new HashMap<>();
            for (int i = 0; i < size; i++) {
                byte[] data = in.readArray();
                Table table = Table.deserialize(data);
                t.newTables.put(table.name, table);
            }
        }

        size = in.readVInt();
        if (size > 0) {
            t.droppedTables = new HashSet<>();
            for (int i = 0; i < size; i++) {
                t.droppedTables.add(in.readUTF());
            }
        }
        size = in.readVInt();
        if (size > 0) {
            t.newIndexes = new HashMap<>();
            for (int i = 0; i < size; i++) {
                byte[] data = in.readArray();
                Index index = Index.deserialize(data);
                t.newIndexes.put(index.name, index);
            }
        }

        size = in.readVInt();
        if (size > 0) {
            t.droppedIndexes = new HashSet<>();
            for (int i = 0; i < size; i++) {
                t.droppedIndexes.add(in.readUTF());
            }
        }
        return t;

    }

    public void releaseLockOnKey(String tableName, Bytes key, LocalLockManager locksManager) {
        Map<Bytes, LockHandle> ll = locks.get(tableName);
        if (ll != null) {
            LockHandle lock = ll.remove(key);
            if (lock != null) {
                locksManager.releaseLock(lock);
            }
        }
    }

    public boolean isNewTable(String name) {
        return newTables != null && newTables.containsKey(name);
    }

}
