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

import herddb.network.MessageWrapper;
import herddb.proto.flatbuf.AnyValueWrapper;
import herddb.proto.flatbuf.ColumnDefinition;
import herddb.proto.flatbuf.Response;
import herddb.proto.flatbuf.Row;
import java.nio.Buffer;
import java.nio.ByteBuffer;
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
    public MessageWrapper message;
    public Response response;
    private DataAccessor next;
    private boolean finished;
    public Map<String, Integer> columnNameToPosition;

    public RecordsBatch(MessageWrapper replyWrapper) {
        this.response = replyWrapper.getResponse();
        this.numRecords = response.rowsLength();
        this.columnNames = new String[response.columnNamesLength()];
        this.message = replyWrapper;
        for (int i = 0; i < columnNames.length; i++) {
            ColumnDefinition columnNameDef = response.columnNames(i);
            ByteBuffer byteBuffer = columnNameDef.getByteBuffer();
            int position = byteBuffer.position();
            int limit = byteBuffer.limit();
            String columnName = MessageUtils
                    .readString(columnNameDef.nameInByteBuffer(columnNameDef.getByteBuffer()));
            columnNames[i] = columnName;
            ((Buffer) byteBuffer).position(position);
            ((Buffer) byteBuffer).limit(limit);
        }

        this.currentRecordIndex = -1;
        if (numRecords == 0) {
            finished = true;
        }
    }

    private void ensureColumnNameToPosition() {
        if (columnNameToPosition == null) {
            columnNameToPosition = new HashMap<>();
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                columnNameToPosition.put(columnName, i);
            }
        }
    }

    public boolean isEmpty() {
        return numRecords == 0;
    }

    private final class RowDataAccessor implements DataAccessor {

        private final Row row;

        public RowDataAccessor(Row row) {
            this.row = row;
        }

        @Override
        public Object get(int index) {
            AnyValueWrapper cell = row.cells(index);
            return MessageUtils.decodeObject(cell);
        }

        @Override
        public int getNumFields() {
            return columnNames.length;
        }

        @Override
        public Object get(String property) {
            ensureColumnNameToPosition();
            Integer i = columnNameToPosition.get(property);
            if (i == null) {
                return null;
            }
            return get(i);
        }

        @Override
        public String[] getFieldNames() {
            return columnNames;
        }

    }

    private DataAccessor readRecordAtCurrentPosition() {
        Row row = response.rows(currentRecordIndex);
        return new RowDataAccessor(row);
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
        if (next == null) {
            throw new IllegalStateException("You have to call hasNext");
        }
        DataAccessor _res = next;
        next = null;
        return _res;
    }

    public void release() {
        message.close();
        message = null;
        response = null;
        next = null;
    }

}
