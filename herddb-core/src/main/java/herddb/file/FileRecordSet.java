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
package herddb.file;

import herddb.core.MaterializedRecordSet;
import herddb.model.Column;
import herddb.model.Projection;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import herddb.utils.DataAccessor;
import herddb.utils.DiskArrayList;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import herddb.model.ScanLimits;

/**
 * RecordSet which eventually swaps to disk
 *
 * @author enrico.olivelli
 */
class FileRecordSet extends MaterializedRecordSet {

    private DiskArrayList<DataAccessor> buffer;
    private final Path tmpDirectory;

    public FileRecordSet(int expectedSize, int swapThreshold, Column[] columns, String[] fieldNames, FileRecordSetFactory factory) {
        super(expectedSize, fieldNames, columns, factory);
        this.tmpDirectory = factory.tmpDirectory;
        this.buffer = new DiskArrayList<>(swapThreshold, factory.tmpDirectory, new TupleSerializer(columns, fieldNames));
        this.buffer.enableCompression();

    }

    private static final class TupleSerializer implements DiskArrayList.Serializer<DataAccessor> {

        private final Column[] columns;
        private final String[] fieldNames;
        private final int nColumns;

        public TupleSerializer(Column[] columns, String[] fieldNames) {
            this.columns = columns;
            this.fieldNames = fieldNames;
            this.nColumns = columns.length;
        }

        @Override
        public DataAccessor read(ExtendedDataInputStream oo) throws IOException {
            byte[] serialized = oo.readArray();
            return Tuple.deserialize(serialized, fieldNames, nColumns);
        }

        @Override
        public void write(DataAccessor object, ExtendedDataOutputStream oo) throws IOException {
            VisibleByteArrayOutputStream buffer = Tuple.serialize(object, columns);
            oo.writeArray(buffer.getBuffer(), 0, buffer.size());
        }

    }

    @Override
    public Iterator<DataAccessor> iterator() {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        return buffer.iterator();
    }

    @Override
    public void add(DataAccessor record) {
        if (writeFinished) {
            throw new IllegalStateException("RecordSet is in read mode");
        }
        buffer.add(record);
    }

    @Override
    public void writeFinished() {
        super.writeFinished();
        buffer.finish();
    }

    @Override
    public void sort(TupleComparator comparator) {

        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        if (comparator != null) {
            if (!buffer.isSwapped()) {
                buffer.sortBuffer(comparator);
            } else {
                List<DataAccessor> copyInMemory = new ArrayList<>();
                for (DataAccessor tuple : buffer) {
                    copyInMemory.add(tuple);
                }
                copyInMemory.sort(comparator);
                buffer.close();
                DiskArrayList<DataAccessor> newBuffer = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, new TupleSerializer(columns, fieldNames));
                newBuffer.enableCompression();
                for (DataAccessor t : copyInMemory) {
                    newBuffer.add(t);
                }
                newBuffer.finish();
                buffer = newBuffer;
            }
        }

    }

    @Override
    public void applyProjection(Projection projection, StatementEvaluationContext context) throws StatementExecutionException {
        this.columns = projection.getColumns();
        this.fieldNames = projection.getFieldNames();
        DiskArrayList<DataAccessor> projected = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, new TupleSerializer(columns, fieldNames));
        projected.enableCompression();
        for (DataAccessor record : buffer) {
            projected.add(projection.map(record, context));
        }
        projected.finish();
        this.buffer.close();
        this.buffer = projected;
    }

    @Override
    public void applyLimits(ScanLimits limits, StatementEvaluationContext context) throws StatementExecutionException {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        if (limits == null) {
            return;
        }
        int offset = limits.computeOffset(context);
        if (offset > 0) {
            int maxlen = buffer.size();
            if (offset >= maxlen) {
                buffer.close();

                // new empty buffer
                buffer = new DiskArrayList<>(Integer.MAX_VALUE, tmpDirectory, new TupleSerializer(columns, fieldNames));
                buffer.enableCompression();
                buffer.finish();
                return;
            }

            int samplesize = maxlen - offset;
            DiskArrayList<DataAccessor> copy = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, new TupleSerializer(columns, fieldNames));
            copy.enableCompression();
            int firstIndex = offset;
            int lastIndex = offset + samplesize;
            int i = 0;
            for (DataAccessor t : buffer) {
                if (i >= firstIndex && i < lastIndex) {
                    copy.add(t);
                }
                i++;
                if (i >= lastIndex) {
                    break;
                }
            }
            buffer.close();
            copy.finish();
            buffer = copy;
        }

        int maxRows = limits.computeMaxRows(context);
        if (maxRows > 0) {
            int maxlen = buffer.size();
            if (maxlen < maxRows) {
                return;
            }
            buffer.truncate(maxRows);
        }

    }

    @Override
    public void close() {
        buffer.close();
    }

}
