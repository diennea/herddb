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
package herddb.core;

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
public class MaterializedRecordSet {

    private List<Tuple> records;
    Column[] columns;
    private boolean writeFinished;
    private int size;
    private RecordSetFactory factory;

    MaterializedRecordSet(Column[] columns, RecordSetFactory factory) {
        this.records = new ArrayList<>();
        this.columns = columns;
        this.factory = factory;
    }

    MaterializedRecordSet(int size, Column[] columns, RecordSetFactory factory) {
        this.records = new ArrayList<>(size);
        this.columns = columns;
        this.factory = factory;
    }

    public Iterator<Tuple> iterator() {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        return records.iterator();
    }

    public void add(Tuple record) {
        if (writeFinished) {
            throw new IllegalStateException("RecordSet in read mode");
        }
        records.add(record);
        size++;
    }

    public void writeFinished() {
        writeFinished = true;
    }

    public int size() {
        return size;
    }

    public void sort(TupleComparator comparator) {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        if (comparator != null) {
            records.sort(comparator);
        }
    }

    public void applyProjection(Projection projection) throws StatementExecutionException {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        this.columns = projection.getColumns();
        List<Tuple> projected = new ArrayList<>(size);
        for (Tuple record : records) {
            projected.add(projection.map(record));
        }
        this.records = projected;
    }

    public void applyLimits(ScanLimits limits) {
        if (!writeFinished) {
            throw new IllegalStateException("RecordSet is still in write mode");
        }
        if (limits == null) {
            return;
        }
        if (limits.getOffset() > 0) {
            int maxlen = records.size();
            if (limits.getOffset() >= maxlen) {
                records.clear();
                return;
            }
            int samplesize = maxlen - limits.getOffset();
            records = records.subList(limits.getOffset(), limits.getOffset() + samplesize);
        }
        if (limits.getMaxRows() > 0) {
            int maxlen = records.size();
            if (maxlen < limits.getMaxRows()) {
                return;
            }
            records = records.subList(0, limits.getMaxRows());
        }
    }

}
