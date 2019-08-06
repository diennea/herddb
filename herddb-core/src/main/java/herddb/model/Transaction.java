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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.collections.map.HashedMap;

import herddb.core.HerdDBInternalException;
import herddb.log.CommitLogResult;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.ILocalLockManager;
import herddb.utils.LockHandle;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;

/**
 * A Transaction, that is a series of Statement which must be executed with ACID
 * semantics on a set of tables of the same TableSet
 *
 * @author enrico.olivelli
 */
public class Transaction {

    public final long transactionId;
    public final String tableSpace;
    public final Map<String, HashedMap> locks;
    public final Map<String, Map<Bytes, Record>> changedRecords;
    public final Map<String, Map<Bytes, Record>> newRecords;
    public final Map<String, Set<Bytes>> deletedRecords;
    public Map<String, Table> newTables;
    public Map<String, Index> newIndexes;
    public Set<String> droppedTables;
    public Set<String> droppedIndexes;
    public LogSequenceNumber lastSequenceNumber;
    public final long localCreationTimestamp;
    private final List<CommitLogResult> deferredWrites = new ArrayList<>();
    public volatile long lastActivityTs = System.currentTimeMillis();

    public Transaction(long transactionId, String tableSpace, CommitLogResult lastSequenceNumber) {
        this.transactionId = transactionId;
        this.tableSpace = tableSpace;
        this.locks = new HashMap<>();
        this.changedRecords = new HashMap<>();
        this.newRecords = new HashMap<>();
        this.deletedRecords = new HashMap<>();
        this.lastSequenceNumber = lastSequenceNumber.deferred ? null : lastSequenceNumber.getLogSequenceNumber();
        this.localCreationTimestamp = System.currentTimeMillis();
    }
    
    public void touch() {
        lastActivityTs = System.currentTimeMillis();
    }

    public LockHandle lookupLock(String tableName, Bytes key) {
        HashedMap ll = locks.get(tableName);
        if (ll == null || ll.isEmpty()) {
            return null;
        }
        return (LockHandle) ll.get(key);
    }

    public void registerLockOnTable(String tableName, LockHandle handle) {
        HashedMap ll = locks.get(tableName);
        if (ll == null) {
            ll = new HashedMap();
            locks.put(tableName, ll);
        }
        ll.put(handle.key, handle);
    }

    public synchronized void registerNewTable(Table table, CommitLogResult writeResult) {
        if (updateLastSequenceNumber(writeResult)) {
            return;
        }
        if (newTables
                == null) {
            newTables = new HashMap<>();
        }

        newTables.put(table.name, table);
        if (droppedTables
                == null) {
            droppedTables = new HashSet<>();
        }

        droppedTables.remove(table.name);
    }

    private boolean updateLastSequenceNumber(CommitLogResult writeResult) throws LogNotAvailableException {
        touch();
        if (writeResult.deferred) {
            deferredWrites.add(writeResult);
            return false;
        }
        LogSequenceNumber sequenceNumber = writeResult.getLogSequenceNumber();
        if (lastSequenceNumber != null && lastSequenceNumber.after(sequenceNumber)) {
            return true;
        }
        this.lastSequenceNumber = sequenceNumber;
        return false;
    }

    public synchronized void registerNewIndex(Index index, CommitLogResult writeResult) {
        if (updateLastSequenceNumber(writeResult)) {
            return;
        }
        if (newIndexes == null) {
            newIndexes = new HashMap<>();
        }
        newIndexes.put(index.name, index);
        if (droppedIndexes == null) {
            droppedIndexes = new HashSet<>();
        }
        droppedIndexes.remove(index.name);
    }

    public synchronized void registerInsertOnTable(String tableName, Bytes key, Bytes value, CommitLogResult writeResult) {
        if (updateLastSequenceNumber(writeResult)) {
            return;
        }

        Set<Bytes> deleted = deletedRecords.get(tableName);
        if (deleted != null) {
            boolean removed = deleted.remove(key);
            if (removed) {
                // special pattern:
                // DELETE - INSERT
                // it can be converted to an UPDATE in memory
                registerRecordUpdate(tableName, key, value, writeResult);
                return;
            }
        }

        Map<Bytes, Record> ll = newRecords.get(tableName);
        if (ll == null) {
            ll = new HashMap<>();
            newRecords.put(tableName, ll);
        }
        ll.put(key, new Record(key, value));

    }

    public void releaseLocksOnTable(String tableName, ILocalLockManager lockManager) {
        Map<Bytes, LockHandle> ll = locks.get(tableName);
        if (ll != null) {
            for (LockHandle l : ll.values()) {
                lockManager.releaseLock(l);
            }
        }
    }
    private static final Logger LOG = Logger.getLogger(Transaction.class.getName());

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
    public synchronized void registerRecordUpdate(String tableName, Bytes key, Bytes value, CommitLogResult writeResult) {
        if (updateLastSequenceNumber(writeResult)) {
            return;
        }
        Map<Bytes, Record> ll = changedRecords.get(tableName);
        if (ll == null) {
            ll = new HashMap<>();
            changedRecords.put(tableName, ll);
        }
        if (value == null) {
            ll.remove(key);
        } else {
            ll.put(key, new Record(key, value));
        }
    }

    public synchronized void registerDeleteOnTable(String tableName, Bytes key, CommitLogResult writeResult) {
        if (!writeResult.deferred
                && lastSequenceNumber != null && lastSequenceNumber.after(writeResult.getLogSequenceNumber())) {
            return;
        }
        registerRecordUpdate(tableName, key, null, writeResult);

        Set<Bytes> ll = deletedRecords.get(tableName);
        if (ll == null) {
            ll = new HashSet<>();
            deletedRecords.put(tableName, ll);
        }
        ll.add(key);
    }

    public boolean recordDeleted(String tableName, Bytes key) {
        Set<Bytes> deleted = deletedRecords.get(tableName);
        return deleted != null && deleted.contains(key);

    }

    public Record recordInserted(String tableName, Bytes key) {
        Map<Bytes, Record> inserted = newRecords.get(tableName);
        if (inserted == null) {
            return null;
        }
        return inserted.get(key);

    }

    public Record recordUpdated(String tableName, Bytes key) {
        Map<Bytes, Record> updated = changedRecords.get(tableName);
        if (updated == null) {
            return null;
        }
        return updated.get(key);

    }

    public Collection<Record> getNewRecordsForTable(String tableName) {
        Map<Bytes, Record> inserted = newRecords.get(tableName);
        if (inserted == null || inserted.isEmpty()) {
            return null;
        } else {
            return inserted.values();
        }
    }

    @Override
    public String toString() {
        return "Transaction{" + "transactionId=" + transactionId + ", tableSpace=" + tableSpace
                + ", locks=" + locks.size() + ", changedRecords=" + changedRecords.size()
                + ", newRecords=" + newRecords.size()
                + ", deletedRecords=" + deletedRecords.size()
                + ", newTables=" + newTables
                + ", newIndexes=" + newIndexes + '}';
    }

    public synchronized void registerDropTable(String tableName, CommitLogResult writeResult) {
        if (updateLastSequenceNumber(writeResult)) {
            return;
        }
        if (newTables == null) {
            newTables = new HashMap<>();
        }
        newTables.remove(tableName);
        if (droppedTables == null) {
            droppedTables = new HashSet<>();
        }
        droppedTables.add(tableName);
    }

    public synchronized void registerDropIndex(String indexName, CommitLogResult writeResult) {
        if (updateLastSequenceNumber(writeResult)) {
            return;
        }
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

    public byte[] serialize() {
        VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1024);
        try (ExtendedDataOutputStream ooo = new ExtendedDataOutputStream(oo)) {
            serialize(ooo);
        } catch (IOException impossible) {
            throw new HerdDBInternalException(impossible);
        }
        return oo.toByteArray();
    }

    public synchronized void serialize(ExtendedDataOutputStream out) throws IOException {
        out.writeVLong(1); // version
        out.writeVLong(0); // flags for future implementations
        out.writeZLong(transactionId);
        if (lastSequenceNumber != null) {
            out.writeZLong(lastSequenceNumber.ledgerId);
            out.writeZLong(lastSequenceNumber.offset);
        } else {
            out.writeZLong(0);
            out.writeZLong(0);
        }
        out.writeVInt(changedRecords.size());
        for (Map.Entry<String, Map<Bytes, Record>> table : changedRecords.entrySet()) {
            out.writeUTF(table.getKey());
            out.writeVInt(table.getValue().size());
            for (Record r : table.getValue().values()) {
                out.writeArray(r.key);
                out.writeArray(r.value);
            }
        }
        out.writeVInt(newRecords.size());
        for (Map.Entry<String, Map<Bytes, Record>> table : newRecords.entrySet()) {
            out.writeUTF(table.getKey());
            out.writeVInt(table.getValue().size());
            for (Record r : table.getValue().values()) {
                out.writeArray(r.key);
                out.writeArray(r.value);
            }
        }
        out.writeVInt(deletedRecords.size());
        for (Map.Entry<String, Set<Bytes>> table : deletedRecords.entrySet()) {
            out.writeUTF(table.getKey());
            out.writeVInt(table.getValue().size());
            for (Bytes key : table.getValue()) {
                out.writeArray(key);
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

    public static Transaction deserialize(String tableSpace, byte[] buf) {
        try {
            return deserialize(tableSpace, new ExtendedDataInputStream(new SimpleByteArrayInputStream(buf)));
        } catch (IOException err) {
            throw new HerdDBInternalException(err);
        }
    }

    public static Transaction deserialize(String tableSpace, ExtendedDataInputStream in) throws IOException {
        long version = in.readVLong(); // version
        long flags = in.readVLong(); // flags for future implementations
        if (version != 1 || flags != 0) {
            throw new IOException("corrupted transaction file");
        }
        long id = in.readZLong();
        long ledgerId = in.readZLong();
        long offset = in.readZLong();
        LogSequenceNumber lastSequenceNumber = new LogSequenceNumber(ledgerId, offset);
        Transaction t = new Transaction(id, tableSpace, new CommitLogResult(lastSequenceNumber, false, true));
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String table = in.readUTF();
            int numRecords = in.readVInt();
            Map<Bytes, Record> records = new HashMap<>();
            for (int k = 0; k < numRecords; k++) {
                byte[] key = in.readArray();
                byte[] value = in.readArray();
                Bytes bKey = Bytes.from_array(key);
                Record record = new Record(bKey, Bytes.from_array(value));
                records.put(bKey, record);
            }
            t.changedRecords.put(table, records);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String table = in.readUTF();
            int numRecords = in.readVInt();
            Map<Bytes, Record> records = new HashMap<>();
            for (int k = 0; k < numRecords; k++) {
                byte[] key = in.readArray();
                byte[] value = in.readArray();
                Bytes bKey = Bytes.from_array(key);
                Record record = new Record(bKey, Bytes.from_array(value));
                records.put(bKey, record);
            }
            t.newRecords.put(table, records);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String table = in.readUTF();
            int numRecords = in.readVInt();
            Set<Bytes> records = new HashSet<>();
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

    public void releaseLockOnKey(String tableName, Bytes key, ILocalLockManager locksManager) {
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

    public boolean isOnTable(String name) {
        // best effort guess
        return locks.containsKey(name)
                || (newIndexes != null && newIndexes.containsKey(name))
                || newRecords.containsKey(name)
                || deletedRecords.containsKey(name)
                || (droppedTables != null && droppedTables.contains(name))
                || (newTables != null && newTables.containsKey(name));
    }

    /* Visible for testing */
    public void sync() throws LogNotAvailableException {
        touch();
        // wait for all writes to be synch to log
        for (CommitLogResult result : deferredWrites) {
            LogSequenceNumber number = result.getLogSequenceNumber();
            if (lastSequenceNumber == null || number.after(lastSequenceNumber)) {
                lastSequenceNumber = number;
            }
        }
    }

    public void sync(LogSequenceNumber sequenceNumber) throws LogNotAvailableException {
        sync();

        /* Check that given transaction position isn't smaller than last seen sequence number */
        if (lastSequenceNumber != null && !sequenceNumber.after(lastSequenceNumber)) {
            throw new IllegalStateException("Corrupted transaction, syncing on a position smaller than transaction last sequence number");
        }
        lastSequenceNumber = sequenceNumber;
    }
    
    public boolean isAbandoned(long timestamp) {
        return lastActivityTs < timestamp;
    }

}
