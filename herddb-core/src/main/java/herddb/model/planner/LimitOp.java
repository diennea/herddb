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
import herddb.model.LimitedDataScanner;
import herddb.model.ScanLimits;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.model.commands.ScanStatement;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;

/**
 * Limit clause
 *
 * @author eolivelli
 */
public class LimitOp implements PlannerOp, ScanLimits {

    private final PlannerOp input;
    private final CompiledSQLExpression maxRows;
    private final CompiledSQLExpression offset;

    public LimitOp(PlannerOp input, CompiledSQLExpression maxRows, CompiledSQLExpression offset) {
        this.input = input.optimize();
        this.maxRows = maxRows;
        this.offset = offset;
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
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
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        try {
            // TODO merge projection + scan + sort + limit
            StatementExecutionResult input = this.input.execute(tableSpaceManager,
                    transactionContext, context, lockRequired, forWrite);
            ScanResult downstreamScanResult = (ScanResult) input;
            final DataScanner inputScanner = downstreamScanResult.dataScanner;
            int offset = computeOffset(context);
            int maxrows = computeMaxRows(context);
            if (maxrows <= 0 && offset == 0) {
                return downstreamScanResult;
            } else {
                LimitedDataScanner limited = new LimitedDataScanner(inputScanner, maxrows, offset, context);
                return new ScanResult(downstreamScanResult.transactionId, limited);
            }
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    @Override
    public int computeMaxRows(StatementEvaluationContext context) throws StatementExecutionException {
        return this.maxRows == null ? -1 : ((Number) this.maxRows.evaluate(DataAccessor.NULL, context)).intValue();
    }

    @Override
    public int computeOffset(StatementEvaluationContext context) throws StatementExecutionException {
        return this.offset == null ? 0 : ((Number) this.offset.evaluate(DataAccessor.NULL, context)).intValue();
    }

    @Override
    public PlannerOp optimize() {
        if (input instanceof SortedBindableTableScanOp) {
            SortedBindableTableScanOp op = (SortedBindableTableScanOp) input;
            // we can change the statement, this node will be lost and the tablescan too
            ScanStatement statement = op.getStatement();
            statement.setLimits(this);
            return new LimitedSortedBindableTableScanOp(statement);
        } else if (input instanceof BindableTableScanOp) {
            BindableTableScanOp op = (BindableTableScanOp) input;
            // we can change the statement, this node will be lost and the tablescan too
            ScanStatement statement = op.getStatement();
            statement.setLimits(this);
            return new LimitedBindableTableScanOp(statement);
        }
        return this;
    }

    @Override
    public String toString() {
        return String.format("LimitOp{maxRows = %s input = {%s}}", this.maxRows.toString(), input.toString());
    }

    @Override
    public String toStringForScan() {
        return String.format("LimitOp{maxRows = %s}", this.maxRows.toString());
    }

    @Override
    public Column[] getOutputSchema() {
        return input.getOutputSchema();
    }
}
