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

import herddb.codec.RecordSerializer;
import herddb.core.MaterializedRecordSet;
import herddb.model.Column;
import herddb.model.Projection;
import herddb.model.ScanLimits;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import herddb.utils.DiskArrayList;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * RecordSet which eventually swaps to disk
 *
 * @author enrico.olivelli
 */
class FileRecordSet extends MaterializedRecordSet {

    private DiskArrayList<Tuple> buffer;
    private final Path tmpDirectory;

    public FileRecordSet(int expectedSize, int swapThreshold, Column[] columns, FileRecordSetFactory factory) {
        super(expectedSize, columns, factory);
        this.tmpDirectory = factory.tmpDirectory;
        this.buffer = new DiskArrayList<>(swapThreshold, factory.tmpDirectory, SERIALIZER);
    }

    private static final TupleSerializer SERIALIZER = new TupleSerializer();

    private static final class TupleSerializer implements DiskArrayList.Serializer<Tuple> {

        @Override
        public Tuple read(ExtendedDataInputStream oo) throws IOException {
            // TODO: use a better serialization
            byte[] serialized = oo.readArray();
            return Tuple.deserialize(serialized);
        }

        @Override
        public void write(Tuple object, ExtendedDataOutputStream oo) throws IOException {
            // TODO: use a better serialization
            oo.writeArray(object.serialize());
        }

    }

    @Override
    public Iterator<Tuple> iterator() {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        return buffer.iterator();
    }

    @Override
    public void add(Tuple record) {
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
                List<Tuple> copyInMemory = new ArrayList<>();
                for (Tuple tuple : buffer) {
                    copyInMemory.add(tuple);
                }
                copyInMemory.sort(comparator);
                buffer.close();
                DiskArrayList<Tuple> newBuffer = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, SERIALIZER);
                for (Tuple t : copyInMemory) {
                    newBuffer.add(t);
                }
                newBuffer.finish();
                buffer = newBuffer;
            }
        }

    }

    @Override
    public void applyProjection(Projection projection) throws StatementExecutionException {
        this.columns = projection.getColumns();
        DiskArrayList<Tuple> projected = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, SERIALIZER);
        for (Tuple record : buffer) {
            projected.add(projection.map(record));
        }
        projected.finish();
        this.buffer.close();
        this.buffer = projected;
    }

    @Override
    public void applyLimits(ScanLimits limits) {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        if (limits == null) {
            return;
        }
        if (limits.getOffset() > 0) {
            int maxlen = buffer.size();
            if (limits.getOffset() >= maxlen) {
                buffer.close();

                // new empty buffer
                buffer = new DiskArrayList<>(Integer.MAX_VALUE, tmpDirectory, SERIALIZER);
                buffer.finish();
                return;
            }

            int samplesize = maxlen - limits.getOffset();
            DiskArrayList<Tuple> copy = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, SERIALIZER);
            int firstIndex = limits.getOffset();
            int lastIndex = limits.getOffset() + samplesize;
            int i = 0;
            for (Tuple t : buffer) {
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

        if (limits.getMaxRows() > 0) {
            int maxlen = buffer.size();
            if (maxlen < limits.getMaxRows()) {
                return;
            }
            DiskArrayList<Tuple> copy = new DiskArrayList<>(buffer.isSwapped() ? -1 : Integer.MAX_VALUE, tmpDirectory, SERIALIZER);
            int last = limits.getMaxRows();
            int i = 0;
            for (Tuple t : buffer) {
                if (i < last) {
                    copy.add(t);
                } else {
                    break;
                }
                i++;
            }
            buffer.close();
            copy.finish();
            buffer = copy;
        }

    }

    @Override
    public void close() {
        buffer.close();
    }

}
