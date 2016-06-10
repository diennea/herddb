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
package herddb.mem;

import herddb.core.*;
import herddb.model.Column;
import herddb.model.Projection;
import herddb.model.ScanLimits;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * a Simple MaterializedRecordSet held in memory
 *
 * @author enrico.olivelli
 */
public class MemoryRecordSet extends MaterializedRecordSet {

    private List<Tuple> buffer = new ArrayList<>();

    MemoryRecordSet(Column[] columns, RecordSetFactory factory) {
        super(-1, columns, factory);
    }

    MemoryRecordSet(int size, Column[] columns, RecordSetFactory factory) {
        super(size, columns, factory);
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
            throw new IllegalStateException("RecordSet in read mode");
        }
        buffer.add(record);
        size++;
    }

    @Override
    public void sort(TupleComparator comparator) {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        if (comparator != null) {
            buffer.sort(comparator);
        }
    }

    @Override
    public void applyProjection(Projection projection) throws StatementExecutionException {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        this.columns = projection.getColumns();
        List<Tuple> projected = new ArrayList<>(size);
        for (Tuple record : buffer) {
            projected.add(projection.map(record));
        }
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
                buffer.clear();
                return;
            }
            int samplesize = maxlen - limits.getOffset();
            buffer = buffer.subList(limits.getOffset(), limits.getOffset() + samplesize);
        }
        if (limits.getMaxRows() > 0) {
            int maxlen = buffer.size();
            if (maxlen < limits.getMaxRows()) {
                return;
            }
            buffer = buffer.subList(0, limits.getMaxRows());
        }
    }

}
