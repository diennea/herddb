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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.Column;
import herddb.model.Projection;
import herddb.model.ScanLimits;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TupleComparator;
import herddb.utils.DataAccessor;
import java.util.Iterator;

/**
 * a Simple MaterializedRecordSet held in memory
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public abstract class MaterializedRecordSet implements AutoCloseable, Iterable<DataAccessor> {

    protected Column[] columns;
    protected String[] fieldNames;
    protected boolean writeFinished;
    protected int size;
    protected final int expectedSize;
    protected final RecordSetFactory factory;

    protected MaterializedRecordSet(int expectedSize, String[] fieldNames, Column[] columns, RecordSetFactory factory) {
        this.fieldNames = fieldNames;
        this.expectedSize = expectedSize;
        this.columns = columns;
        this.factory = factory;
        if (fieldNames == null) {
            throw new NullPointerException();
        }
    }

    @Override
    public abstract Iterator<DataAccessor> iterator();

    public abstract void add(DataAccessor record);

    public void writeFinished() {
        writeFinished = true;
    }

    public Column[] getColumns() {
        return columns;
    }

    public int size() {
        return size;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public RecordSetFactory getFactory() {
        return factory;
    }

    public abstract void sort(TupleComparator comparator);

    public abstract void applyProjection(Projection projection, StatementEvaluationContext context) throws StatementExecutionException;

    public abstract void applyLimits(ScanLimits limits, StatementEvaluationContext context) throws StatementExecutionException;

    @Override
    public void close() {
    }

}
