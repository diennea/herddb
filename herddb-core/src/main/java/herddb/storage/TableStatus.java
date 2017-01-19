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

import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Status of a table on disk
 *
 * @author enrico.olivelli
 */
public class TableStatus {

    public final String tableName;
    public final LogSequenceNumber sequenceNumber;
    public final byte[] nextPrimaryKeyValue;
    public final Set<Long> activePages;
    public final long nextPageId;

    public TableStatus(String tableName, LogSequenceNumber sequenceNumber, byte[] nextPrimaryKeyValue, long nextPageId, Set<Long> activePages) {
        this.tableName = tableName;
        this.sequenceNumber = sequenceNumber;
        this.nextPrimaryKeyValue = nextPrimaryKeyValue;
        this.activePages = new HashSet<>(activePages);
        this.nextPageId = nextPageId;
    }

    public void serialize(ExtendedDataOutputStream output) throws IOException {
        output.writeUTF(tableName);
        output.writeLong(sequenceNumber.ledgerId);
        output.writeLong(sequenceNumber.offset);
        output.writeLong(nextPageId);
        output.writeArray(nextPrimaryKeyValue);
        output.writeVInt(activePages.size());
        for (long idpage : activePages) {
            output.writeVLong(idpage);
        }
    }

    public static TableStatus deserialize(ExtendedDataInputStream in) throws IOException {
        String tableName = in.readUTF();
        long ledgerId = in.readLong();
        long offset = in.readLong();
        long nextPageId = in.readLong();
        byte[] nextPrimaryKeyValue = in.readArray();
        int numPages = in.readVInt();
        Set<Long> activePages = new HashSet<>();
        for (int i = 0; i < numPages; i++) {
            activePages.add(in.readVLong());
        }
        return new TableStatus(tableName, new LogSequenceNumber(ledgerId, offset), nextPrimaryKeyValue, nextPageId, activePages);
    }

    @Override
    public String toString() {
        return "TableStatus{" + "tableName=" + tableName + ", sequenceNumber=" + sequenceNumber + ", nextPageId=" + nextPageId + ", activePages=" + activePages + '}';
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
