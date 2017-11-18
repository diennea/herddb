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
import herddb.utils.DataAccessor;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.calcite.plan.RelOptRule.any;

/**
 * Union all
 */
public class UnionAllOp implements PlannerOp {

    private final List<PlannerOp> inputs;

    public UnionAllOp(List<PlannerOp> inputs) {
        this.inputs = inputs;
    }

    @Override
    public String getTablespace() {
        return inputs.get(0).getTablespace();
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        try {
            StatementExecutionResult input = this.inputs.get(0).execute(tableSpaceManager, transactionContext, context);
            ScanResult downstream = (ScanResult) input;
            DataScanner dataScanner = new UnionAllDataScanner(downstream.dataScanner, tableSpaceManager, transactionContext, context);

            return new ScanResult(downstream.transactionId, dataScanner);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }

    }

    private class UnionAllDataScanner extends DataScanner {

        int index = 0;
        DataScanner current;
        final TableSpaceManager tableSpaceManager;
        TransactionContext transactionContext;
        final StatementEvaluationContext context;

        public UnionAllDataScanner(DataScanner first, TableSpaceManager tableSpaceManager,
                TransactionContext transactionContext, StatementEvaluationContext context) throws DataScannerException {
            super(first.transactionId, first.getFieldNames(), first.getSchema());
            this.tableSpaceManager = tableSpaceManager;
            this.context = context;
            this.transactionContext = transactionContext;
            current = first;
            fetchNext();
        }

        private void fetchNext() throws DataScannerException {
            System.out.println("fetchNext " + current + " index " + index);
            if (current.hasNext()) {
                next = current.next();
            } else if (index == inputs.size() - 1) {
                next = null;
            } else {
                index++;
                ScanResult execute = (ScanResult) inputs.get(index).execute(tableSpaceManager, transactionContext, context);
                transactionContext = new TransactionContext(execute.transactionId);
                this.transactionId = execute.transactionId;
                current = execute.dataScanner;
                fetchNext();
            }
        }
        private DataAccessor next;

        @Override
        public boolean hasNext() throws DataScannerException {
            return next != null;
        }

        @Override
        public DataAccessor next() throws DataScannerException {
            DataAccessor current = next;
            fetchNext();
            System.out.println("returning " + current);
            return current;
        }

    }

}
