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

package herddb.model.planner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.MaterializedRecordSet;
import herddb.core.RecordSetFactory;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.sql.AggregatedColumnCalculator;
import herddb.sql.expressions.AccessCurrentRowExpression;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic aggregation
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class AggregateOp implements PlannerOp {

    private final PlannerOp input;
    private final String[] fieldnames;
    private final Column[] columns;
    private final String[] aggtypes;
    private final List<Integer> groupedFiledsIndexes;
    private final List<List<Integer>> argLists;

    public AggregateOp(
            PlannerOp input,
            String[] fieldnames,
            Column[] columns,
            String[] aggtypes,
            List<List<Integer>> argLists,
            List<Integer> groupedFieldsIndexes
    ) {
        this.input = input;
        this.fieldnames = fieldnames;
        this.columns = columns;
        this.aggtypes = aggtypes;
        this.groupedFiledsIndexes = groupedFieldsIndexes;
        this.argLists = argLists;
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context,
            boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {

        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        ScanResult downstreamScanResult = (ScanResult) input;
        final DataScanner inputScanner = downstreamScanResult.dataScanner;
        AggregatedDataScanner filtered = new AggregatedDataScanner(inputScanner, context,
                tableSpaceManager.getDbmanager().getRecordSetFactory());
        return new ScanResult(downstreamScanResult.transactionId, filtered);

    }

    private static class Group {

        AggregatedColumnCalculator[] columns;

        public Group(AggregatedColumnCalculator[] columns) {
            this.columns = columns;
        }

    }

    private class AggregatedDataScanner extends DataScanner {

        private final DataScanner wrapped;
        private DataScanner aggregatedScanner;
        private final StatementEvaluationContext context;
        private final RecordSetFactory recordSetFactory;

        public AggregatedDataScanner(
                DataScanner wrapped,
                StatementEvaluationContext context,
                RecordSetFactory recordSetFactory
        ) throws StatementExecutionException {
            super(wrapped.getTransaction(), fieldnames, columns);
            this.wrapped = wrapped;
            this.context = context;
            this.recordSetFactory = recordSetFactory;
        }

        private class Key {

            final Object[] values;

            public Key(Object[] values) {
                this.values = values;
            }

            @Override
            public int hashCode() {
                int hash = 7;
                hash = 71 * hash + Arrays.deepHashCode(this.values);
                return hash;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                final Key other = (Key) obj;
                return Arrays.deepEquals(this.values, other.values);
            }

        }

        private Key key(DataAccessor tuple) throws DataScannerException {
            Object[] values = new Object[groupedFiledsIndexes.size()];
            int i = 0;
            for (int posInUpstreamRow : groupedFiledsIndexes) {
                Object value = tuple.get(posInUpstreamRow);
                values[i++] = value;
            }
            return new Key(values);
        }

        private void compute() throws DataScannerException {
            try {
                if (!groupedFiledsIndexes.isEmpty()) {
                    Map<Key, Group> groups = new HashMap<>();
                    while (wrapped.hasNext()) {
                        DataAccessor tuple = wrapped.next();
                        Key key = key(tuple);
                        Group group = groups.get(key);
                        if (group == null) {
                            group = createGroup();
                            groups.put(key, group);
                        }
                        for (AggregatedColumnCalculator cc : group.columns) {
                            cc.consume(tuple);
                        }
                    }
                    MaterializedRecordSet results = recordSetFactory
                            .createFixedSizeRecordSet(groups.values().size(),
                                    getFieldNames(), getSchema());
                    for (Map.Entry<Key, Group> cell : groups.entrySet()) {
                        Key key = cell.getKey();
                        Group group = cell.getValue();
                        AggregatedColumnCalculator[] columns = group.columns;
                        Object[] values = new Object[fieldnames.length];
                        int k = 0;
                        for (Object field : key.values) {
                            values[k++] = field;
                        }
                        for (AggregatedColumnCalculator cc : columns) {
                            values[k++] = cc.getValue();
                        }
                        Tuple tuple = new Tuple(fieldnames, values);
                        results.add(tuple);
                    }
                    results.writeFinished();
                    aggregatedScanner = new SimpleDataScanner(wrapped.getTransaction(), results);
                } else {
                    Group group = createGroup();
                    AggregatedColumnCalculator[] columns = group.columns;
                    while (wrapped.hasNext()) {
                        DataAccessor tuple = wrapped.next();
                        for (AggregatedColumnCalculator cc : columns) {
                            cc.consume(tuple);
                        }
                    }
                    Object[] values = new Object[fieldnames.length];
                    int k = 0;
                    for (AggregatedColumnCalculator cc : columns) {
                        values[k++] = cc.getValue();
                    }
                    Tuple tuple = new Tuple(fieldnames, values);
                    MaterializedRecordSet results = recordSetFactory
                            .createFixedSizeRecordSet(1, getFieldNames(), getSchema());
                    results.add(tuple);
                    results.writeFinished();
                    aggregatedScanner = new SimpleDataScanner(wrapped.getTransaction(), results);
                }
            } catch (StatementExecutionException err) {
                throw new DataScannerException(err);
            }
        }

        private Group createGroup() throws DataScannerException, StatementExecutionException {
            AggregatedColumnCalculator[] columns = new AggregatedColumnCalculator[aggtypes.length];
            for (int i = 0; i < aggtypes.length; i++) {
                String aggtype = aggtypes[i];

                String fieldName = fieldnames[i];
                List<Integer> argList = argLists.get(i);
                CompiledSQLExpression param = argList.isEmpty() ? null : new AccessCurrentRowExpression(argList.get(0)); // TODO, multi params ?
                AggregatedColumnCalculator calculator = BuiltinFunctions.getColumnCalculator(aggtype.toLowerCase(), fieldName, param, context);
                if (calculator == null) {
                    throw new StatementExecutionException("not implemented aggregation type " + aggtype);
                }
                columns[i] = calculator;
            }
            return new Group(columns);
        }

        @Override
        public boolean hasNext() throws DataScannerException {
            if (aggregatedScanner == null) {
                compute();
            }
            return aggregatedScanner.hasNext();
        }

        @Override
        public DataAccessor next() throws DataScannerException {
            if (aggregatedScanner == null) {
                compute();
            }
            return aggregatedScanner.next();
        }

        @Override
        public void close() throws DataScannerException {
            wrapped.close();
            if (aggregatedScanner != null) {
                aggregatedScanner.close();
            }
            super.close();
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        T unwrapped = input.unwrap(clazz);
        if (unwrapped != null) {
            return unwrapped;
        }
        return Wrapper.unwrap(this, clazz);
    }

    @Override
    public String toString() {
        return "AggregateOp{" + "input=" + input + ", fieldnames=" + Arrays.toString(fieldnames)
                + ", columns=" + Arrays.toString(columns) + ", aggtypes=" + Arrays.toString(aggtypes)
                + ", groupedFiledsIndexes=" + groupedFiledsIndexes + ", argLists=" + argLists + '}';
    }

    @Override
    public Column[] getSchema() {
        return columns;
    }

}
