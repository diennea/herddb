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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
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

    public TableStatus(String tableName, LogSequenceNumber sequenceNumber, byte[] nextPrimaryKeyValue, Set<Long> activePages) {
        this.tableName = tableName;
        this.sequenceNumber = sequenceNumber;
        this.nextPrimaryKeyValue = nextPrimaryKeyValue;
        this.activePages = new HashSet<>(activePages);
    }

    public void serialize(DataOutputStream output) throws IOException {
        output.writeUTF(tableName);
        output.writeLong(sequenceNumber.ledgerId);
        output.writeLong(sequenceNumber.offset);
        output.writeInt(nextPrimaryKeyValue.length);
        output.write(nextPrimaryKeyValue);
        output.writeInt(activePages.size());
        for (long idpage : activePages) {
            output.writeLong(idpage);
        }
    }

    public static TableStatus deserialize(DataInputStream in) throws IOException {
        String tableName = in.readUTF();
        long ledgerId = in.readLong();
        long offset = in.readLong();
        int len = in.readInt();
        byte[] nextPrimaryKeyValue = new byte[len];
        in.readFully(nextPrimaryKeyValue);
        int numPages = in.readInt();
        Set<Long> activePages = new HashSet<>();
        for (int i = 0; i < numPages; i++) {
            activePages.add(in.readLong());
        }
        return new TableStatus(tableName, new LogSequenceNumber(ledgerId, offset), nextPrimaryKeyValue, activePages);
    }

    @Override
    public String toString() {
        return "TableStatus{" + "tableName=" + tableName + ", sequenceNumber=" + sequenceNumber + ", activePages=" + activePages + '}';
    }
    
    

}
