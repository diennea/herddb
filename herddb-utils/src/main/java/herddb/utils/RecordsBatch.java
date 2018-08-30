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
package herddb.utils;

import io.netty.buffer.ByteBuf;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A list of tuples, backed by a Netty ByteBuf
 *
 * @author enrico.olivelli
 */
public class RecordsBatch {

    public final String[] columnNames;
    public final int numRecords;
    private int currentRecordIndex;
    public final ByteBuf buffer;
    private DataAccessor next;
    private boolean finished;

    public RecordsBatch(int numRecords, String[] columnNames, ByteBuf buffer) {
        this.numRecords = numRecords;
        this.columnNames = columnNames;
        this.buffer = buffer;
        this.currentRecordIndex = 0;
        if (numRecords > 0) {
            next = readRecordAtCurrentPosition();
        }
    }

    public boolean isEmpty() {
        return numRecords == 0;
    }

    private DataAccessor readRecordAtCurrentPosition() {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            Object value = MessageUtils.readEncodedSimpleValue(buffer);
            map.put(columnNames[i], value);
        }
        return new MapDataAccessor(map, columnNames);
    }

    public boolean hasNext() {
        if (finished) {
            return false;
        }
        return ensureNext();
    }

    private boolean ensureNext() {
        if (next != null) {
            return true;
        }
        if (currentRecordIndex == numRecords - 1) {
            finished = true;
            return false;
        }
        currentRecordIndex++;
        next = readRecordAtCurrentPosition();
        return true;
    }

    public DataAccessor next() {
        if (finished) {
            throw new IllegalStateException("Scanner is exhausted");
        }
        DataAccessor _res = next;
        next = null;
        return _res;
    }

    public void release() {
        buffer.release();
    }

}
