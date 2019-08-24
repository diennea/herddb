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

import herddb.core.MaterializedRecordSet;
import herddb.core.RecordSetFactory;
import herddb.model.Column;
import herddb.model.Projection;
import herddb.model.ScanLimits;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TupleComparator;
import herddb.utils.DataAccessor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * a Simple MaterializedRecordSet held in memory
 *
 * @author enrico.olivelli
 */
public class MemoryRecordSet extends MaterializedRecordSet {

    private List<DataAccessor> buffer = new ArrayList<>();

    MemoryRecordSet(String[] fieldNames, Column[] columns, RecordSetFactory factory) {
        super(-1, fieldNames, columns, factory);
    }

    MemoryRecordSet(int size, String[] fieldNames, Column[] columns, RecordSetFactory factory) {
        super(size, fieldNames, columns, factory);
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
    public void applyProjection(Projection projection, StatementEvaluationContext context) throws StatementExecutionException {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        this.columns = projection.getColumns();
        this.fieldNames = projection.getFieldNames();

        List<DataAccessor> projected = new ArrayList<>(size);
        for (DataAccessor record : buffer) {
            projected.add(projection.map(record, context));
        }
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
                buffer.clear();
                return;
            }
            int samplesize = maxlen - offset;
            buffer = buffer.subList(offset, offset + samplesize);
        }
        int maxRows = limits.computeMaxRows(context);
        if (maxRows > 0) {
            int maxlen = buffer.size();
            if (maxlen < maxRows) {
                return;
            }
            buffer = buffer.subList(0, maxRows);
        }
    }

}
