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
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import java.util.Iterator;

/**
 * a Simple MaterializedRecordSet held in memory
 *
 * @author enrico.olivelli
 */
public abstract class MaterializedRecordSet implements AutoCloseable, Iterable<Tuple> {

    protected Column[] columns;
    protected boolean writeFinished;
    protected int size;
    protected int expectedSize;
    protected RecordSetFactory factory;

    protected MaterializedRecordSet(int expectedSize, Column[] columns, RecordSetFactory factory) {
        this.expectedSize = expectedSize;
        this.columns = columns;
        this.factory = factory;
    }

    @Override
    public abstract Iterator<Tuple> iterator();

    public abstract void add(Tuple record);

    public void writeFinished() {
        writeFinished = true;
    }

    public Column[] getColumns() {
        return columns;
    }

    public int size() {
        return size;
    }

    public abstract void sort(TupleComparator comparator);

    public abstract void applyProjection(Projection projection, StatementEvaluationContext context) throws StatementExecutionException;

    public abstract void applyLimits(ScanLimits limits);

    @Override
    public void close() {
    }

}
