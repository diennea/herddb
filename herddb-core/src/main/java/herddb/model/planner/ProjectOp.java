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
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import herddb.utils.ProjectedDataAccessor;
import java.util.List;

/**
 * Executes a projection
 */
public class ProjectOp implements PlannerOp {

    final private String[] fieldNames;
    final private List<CompiledSQLExpression> fields;
    final private List<PlannerOp> inputs;
    final private Column[] columns;

    public ProjectOp(List<String> fieldNames, Column[] columns,
            List<CompiledSQLExpression> fields, List<PlannerOp> inputs) {
        this.fields = fields;
        this.inputs = inputs;
        this.fieldNames = fieldNames.toArray(new String[fieldNames.size()]);
        this.columns = columns;
    }

    @Override
    public String getTablespace() {
        return inputs.get(0).getTablespace();
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        if (inputs.size() > 1) {
            throw new StatementExecutionException("not supported yet, more then 1 input");
        }
        // TODO merge projection + scan + sort + limit
        StatementExecutionResult input = inputs.get(0).execute(tableSpaceManager, transactionContext, context);
        ScanResult downstream = (ScanResult) input;
        DataScanner dataScanner = downstream.dataScanner;

        DataScanner projected = new ProjectedDataScanner(dataScanner,
                fieldNames, columns, context);
        return new ScanResult(downstream.transactionId, projected);
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
            return new RuntimeProjectedDataAccessor(downstream.next());
        }

        @Override
        public void rewind() throws DataScannerException {
            downstream.rewind();
        }

        @Override
        public boolean isFinished() throws DataScannerException {
            return downstream.isFinished();
        }

        @Override
        public void close() throws DataScannerException {
            downstream.close();
        }

        private class RuntimeProjectedDataAccessor implements DataAccessor {

            final DataAccessor wrapped;
            final Object[] values;

            public RuntimeProjectedDataAccessor(DataAccessor wrapper) {
                this.wrapped = wrapper;
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

        }

    }

}
