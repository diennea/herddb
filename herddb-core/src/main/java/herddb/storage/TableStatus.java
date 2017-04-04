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
package herddb.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.PageSet.DataPageMetaData;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;

/**
 * Status of a table on disk
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class TableStatus {

    public final String tableName;
    public final LogSequenceNumber sequenceNumber;
    public final byte[] nextPrimaryKeyValue;
    public final Map<Long,DataPageMetaData> activePages;
    public final long nextPageId;

    public TableStatus(String tableName, LogSequenceNumber sequenceNumber, byte[] nextPrimaryKeyValue, long nextPageId,
            Map<Long,DataPageMetaData> activePages) {
        this.tableName = tableName;
        this.sequenceNumber = sequenceNumber;
        this.nextPrimaryKeyValue = nextPrimaryKeyValue;
        this.activePages = activePages;
        this.nextPageId = nextPageId;
    }

    public void serialize(ExtendedDataOutputStream output) throws IOException {
        output.writeVLong(1); // version
        output.writeVLong(0); // flags for future implementations
        output.writeUTF(tableName);
        output.writeLong(sequenceNumber.ledgerId);
        output.writeLong(sequenceNumber.offset);
        output.writeLong(nextPageId);
        output.writeArray(nextPrimaryKeyValue);

        output.writeVInt(activePages.size());
        for (Entry<Long,DataPageMetaData> active : activePages.entrySet()) {
            /* id */
            output.writeVLong(active.getKey());
            /* metadata */
            active.getValue().serialize(output);
        }
    }

    public static TableStatus deserialize(ExtendedDataInputStream in) throws IOException {
        long version = in.readVLong(); // version
        long flags = in.readVLong(); // flags for future implementations
        if (version != 1 || flags != 0) {
            throw new DataStorageManagerException("corrupted table status");
        }
        String tableName = in.readUTF();
        long ledgerId = in.readLong();
        long offset = in.readLong();
        long nextPageId = in.readLong();
        byte[] nextPrimaryKeyValue = in.readArray();

        int numActivePages = in.readVInt();
        Map<Long,DataPageMetaData> activePages = new HashMap<>(numActivePages);
        for (int i = 0; i < numActivePages; i++) {
            activePages.put(in.readVLong(), DataPageMetaData.deserialize(in));
        }
        return new TableStatus(tableName, new LogSequenceNumber(ledgerId, offset), nextPrimaryKeyValue, nextPageId, activePages);
    }

    @Override
    public String toString() {
        return "TableStatus{" + "tableName=" + tableName +
                    ", sequenceNumber=" + sequenceNumber +
                    ", nextPageId=" + nextPageId +
                    ", activePages=" + activePages.keySet() +
                    '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 23 * hash + Objects.hashCode(this.tableName);
        hash = 23 * hash + Objects.hashCode(this.sequenceNumber);
        hash = 23 * hash + Arrays.hashCode(this.nextPrimaryKeyValue);
        hash = 23 * hash + Objects.hashCode(this.activePages);
        hash = 23 * hash + (int) (this.nextPageId ^ (this.nextPageId >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TableStatus other = (TableStatus) obj;
        if (this.nextPageId != other.nextPageId) {
            return false;
        }
        if (!Objects.equals(this.tableName, other.tableName)) {
            return false;
        }
        if (!Objects.equals(this.sequenceNumber, other.sequenceNumber)) {
            return false;
        }
        if (!Arrays.equals(this.nextPrimaryKeyValue, other.nextPrimaryKeyValue)) {
            return false;
        }
        if (!Objects.equals(this.activePages, other.activePages)) {
            return false;
        }
        return true;
    }



}
