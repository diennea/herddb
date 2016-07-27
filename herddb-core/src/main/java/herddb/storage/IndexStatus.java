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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Status of an index on disk
 *
 * @author enrico.olivelli
 */
public class IndexStatus {

    public final String indexName;
    public final LogSequenceNumber sequenceNumber;
    public final byte[] indexData;
    public final Set<Long> activePages;

    public IndexStatus(String indexName, LogSequenceNumber sequenceNumber, final Set<Long> activePages, byte[] indexData) {
        this.indexName = indexName;
        this.sequenceNumber = sequenceNumber;
        this.indexData = indexData != null ? indexData : new byte[0];
        this.activePages = activePages != null ? activePages : Collections.emptySet();
    }

    public void serialize(ExtendedDataOutputStream output) throws IOException {
        output.writeUTF(indexName);
        output.writeLong(sequenceNumber.ledgerId);
        output.writeLong(sequenceNumber.offset);
        output.writeVInt(activePages.size());
        for (long idpage : activePages) {
            output.writeVLong(idpage);
        }
        output.writeArray(indexData);

    }

    public static IndexStatus deserialize(ExtendedDataInputStream in) throws IOException {
        String indexName = in.readUTF();
        long ledgerId = in.readLong();
        long offset = in.readLong();
        int numPages = in.readVInt();
        Set<Long> activePages = new HashSet<>();
        for (int i = 0; i < numPages; i++) {
            activePages.add(in.readVLong());
        }
        byte[] indexData = in.readArray();
        return new IndexStatus(indexName, new LogSequenceNumber(ledgerId, offset), activePages, indexData);
    }

    @Override
    public String toString() {
        return "IndexStatus{" + "indexName=" + indexName + ", sequenceNumber=" + sequenceNumber + '}';
    }

}
