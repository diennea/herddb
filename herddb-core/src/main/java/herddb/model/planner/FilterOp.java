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
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.SQLRecordPredicate;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;

/**
 * Generic filter
 *
 * @author eolivelli
 */
public class FilterOp implements PlannerOp {

    private final PlannerOp input;
    private final CompiledSQLExpression condition;

    public FilterOp(PlannerOp input, CompiledSQLExpression condition) {
        this.input = input;
        this.condition = condition;
    }

    public PlannerOp getInput() {
        return input;
    }

    public CompiledSQLExpression getCondition() {
        return condition;
    }
    
    @Override
    public String getTablespace() {
        return input.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        try {
            // TODO merge projection + scan + sort + limit
            StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context);
            ScanResult downstreamScanResult = (ScanResult) input;
            final DataScanner inputScanner = downstreamScanResult.dataScanner;
            FilteredDataScanner filtered = new FilteredDataScanner(inputScanner, context);
            return new ScanResult(downstreamScanResult.transactionId, filtered);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    private class FilteredDataScanner extends DataScanner {

        final DataScanner inputScanner;
        final StatementEvaluationContext context;
        DataAccessor next;

        private FilteredDataScanner(DataScanner inputScanner, StatementEvaluationContext context) throws DataScannerException {
            super(inputScanner.transactionId, inputScanner.getFieldNames(), inputScanner.getSchema());
            this.inputScanner = inputScanner;
            this.context = context;
            fetchNext();
        }

        private void fetchNext() throws DataScannerException {
            while (true) {
                if (!inputScanner.hasNext()) {
                    next = null;
                    return;
                } else {
                    DataAccessor candidate = inputScanner.next();
                    Object evaluate = condition.evaluate(candidate, context);
                    if (SQLRecordPredicate.toBoolean(evaluate)) {
                        next = candidate;
                        return;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() throws DataScannerException {
            return next != null;
        }

        @Override
        public DataAccessor next() throws DataScannerException {
            DataAccessor res = next;
            if (res == null) {
                throw new DataScannerException("illegal state");
            }
            fetchNext();
            return res;
        }

        @Override
        public void rewind() throws DataScannerException {
            inputScanner.rewind();
            fetchNext();
        }

    }

}
