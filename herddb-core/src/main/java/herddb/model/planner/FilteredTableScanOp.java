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
import herddb.model.Predicate;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.model.commands.ScanStatement;
import herddb.sql.SQLRecordPredicate;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.Wrapper;

/**
 * TableScanOp + FilterOp
 *
 * @author eolivelli
 */
public class FilteredTableScanOp implements PlannerOp {

    final ScanStatement statement;

    FilteredTableScanOp(FilterOp op, TableScanOp tableScan) {
        this.statement = tableScan.unwrap(ScanStatement.class);
        CompiledSQLExpression condition = op.getCondition();
        // we can alter the statement, the TableScan will be dropped from the plan
        this.statement.setPredicate(new SQLRecordPredicate(statement.getTableDef(), null, condition));
    }

    public Predicate getPredicate() {
        return statement.getPredicate();
    }

    @Override
    public String getTablespace() {
        return statement.getTableSpace();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context,
            boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        DataScanner scan = tableSpaceManager.scan(statement, context, transactionContext,
                lockRequired, forWrite);
        return new ScanResult(transactionContext.transactionId, scan);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        T unwrapped = statement.unwrap(clazz);
        if (unwrapped != null) {
            return unwrapped;
        }
        return Wrapper.unwrap(this, clazz);
    }

    @Override
    public boolean isSimpleStatementWrapper() {
        return true;
    }

    @Override
    public Column[] getSchema() {
        return statement.getSchema();
    }


}
