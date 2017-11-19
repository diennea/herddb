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

import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.Projection;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.AbstractDataAccessor;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Executes a projection
 */
public class ProjectOp implements PlannerOp {

    final private Projection projection;
    final private PlannerOp input;

    public ProjectOp(Projection projection1, PlannerOp input) {
        this.projection = projection1;
        this.input = input.optimize();
    }

    public static final class BasicProjection implements Projection {

        private final Column[] columns;
        private final String[] fieldNames;
        private final List<CompiledSQLExpression> fields;

        public BasicProjection(String[] fieldNames, Column[] columns,
                List<CompiledSQLExpression> fields) {
            this.fieldNames = fieldNames;
            this.columns = columns;
            this.fields = fields;
        }

        @Override
        public Column[] getColumns() {
            return columns;
        }

        @Override
        public String[] getFieldNames() {
            return fieldNames;
        }

        @Override
        public DataAccessor map(DataAccessor tuple, StatementEvaluationContext context) throws StatementExecutionException {
            return new RuntimeProjectedDataAccessor(tuple, context);
        }

        private class RuntimeProjectedDataAccessor extends AbstractDataAccessor {

            final Object[] values;

            public RuntimeProjectedDataAccessor(DataAccessor wrapper, StatementEvaluationContext context) {
                this.values = new Object[fieldNames.length];
                for (int i = 0; i < fieldNames.length; i++) {
                    CompiledSQLExpression exp = fields.get(i);
                    this.values[i] = exp.evaluate(wrapper, context);
                }
            }

            @Override
            public String[] getFieldNames() {
                return fieldNames;
            }

            @Override
            public Object get(String string) {
                for (int i = 0; i < fieldNames.length; i++) {
                    if (fieldNames[i].equalsIgnoreCase(string)) {
                        return values[i];
                    }
                }
                return null;
            }

            @Override
            public Object get(int i) {
                return values[i];
            }

            @Override
            public Object[] getValues() {
                return values;
            }

            @Override
            public String toString() {
                return "RuntimeProjectedDataAccessor{" + "values=" + Arrays.toString(values) + '}';
            }
            
            
        }
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
    }

    public PlannerOp getInput() {
        return input;
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
    public PlannerOp optimize() {
        if (input instanceof TableScanOp) {
            return new ProjectedTableScanOp(this, (TableScanOp) input);
        }
        return this;
    }

    public Projection getProjection() {
        return projection;
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {

        // TODO merge projection + scan + sort + limit
        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context);
        ScanResult downstream = (ScanResult) input;
        DataScanner dataScanner = downstream.dataScanner;

        DataScanner projected = new ProjectedDataScanner(dataScanner,
                projection.getFieldNames(), projection.getColumns(), context);
        return new ScanResult(downstream.transactionId, projected);
    }

    public static class ZeroCopyProjection implements Projection {

        public ZeroCopyProjection(String[] fieldNames, Column[] columns, int[] zeroCopyProjections) {
            this.fieldNames = fieldNames;
            this.columns = columns;
            this.zeroCopyProjections = zeroCopyProjections;
        }

        private final Column[] columns;
        private final String[] fieldNames;
        private final int[] zeroCopyProjections;

        @Override
        public Column[] getColumns() {
            return columns;
        }

        @Override
        public String[] getFieldNames() {
            return fieldNames;
        }

        @Override
        public DataAccessor map(DataAccessor tuple, StatementEvaluationContext context) throws StatementExecutionException {
            return new RuntimeProjectedDataAccessor(tuple, context);
        }

        public class RuntimeProjectedDataAccessor extends AbstractDataAccessor {

            private final DataAccessor wrapped;

            public RuntimeProjectedDataAccessor(DataAccessor wrapper, StatementEvaluationContext context) {
                this.wrapped = wrapper;
            }

            @Override
            public String[] getFieldNames() {
                return fieldNames;
            }

            @Override
            public Object get(String string) {
                return wrapped.get(string);
            }

            @Override
            public Object get(int i) {
                return wrapped.get(zeroCopyProjections[i]);
            }

            @Override
            public void forEach(BiConsumer<String, Object> consumer) {
                for (int i = 0 ; i < zeroCopyProjections.length; i++) {
                    Object value = wrapped.get(zeroCopyProjections[i]);
                    consumer.accept(fieldNames[i], value);
                }
            }

            @Override
            public Object[] getValues() {
                Object[] data = new Object[fieldNames.length];
                for (int i = 0 ; i < zeroCopyProjections.length; i++) {
                    data[i] = wrapped.get(zeroCopyProjections[i]);
                }
                return data;
            }
        }

    }

    private class ProjectedDataScanner extends DataScanner {

        final DataScanner downstream;
        final StatementEvaluationContext context;

        public ProjectedDataScanner(DataScanner downstream, String[] fieldNames,
                Column[] schema, StatementEvaluationContext context) {
            super(downstream.transactionId, fieldNames, schema);
            this.downstream = downstream;
            this.context = context;
        }

        @Override
        public boolean hasNext() throws DataScannerException {
            return downstream.hasNext();
        }

        @Override
        public DataAccessor next() throws DataScannerException {
            return projection.map(downstream.next(), context);
        }

        @Override
        public void rewind() throws DataScannerException {
            downstream.rewind();
        }

        @Override
        public void close() throws DataScannerException {
            downstream.close();
        }

    }

}
