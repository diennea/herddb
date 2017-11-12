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

import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.LimitedDataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Limit clause
 *
 * @author eolivelli
 */
public class LimitOp implements PlannerOp {

    private final PlannerOp downstream;
    private final CompiledSQLExpression maxRows;
    private final CompiledSQLExpression offset;

    public LimitOp(PlannerOp downstream, CompiledSQLExpression maxRows, CompiledSQLExpression offset) {
        this.downstream = downstream;
        this.maxRows = maxRows;
        this.offset = offset;
    }

    @Override
    public String getTablespace() {
        return downstream.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        try {
            // TODO merge projection + scan + sort + limit
            StatementExecutionResult input = downstream.execute(tableSpaceManager, transactionContext, context);
            ScanResult downstreamScanResult = (ScanResult) input;
            final DataScanner inputScanner = downstreamScanResult.dataScanner;
            int offset = this.offset == null ? 0 : ((Number) this.offset.evaluate(DataAccessor.NULL, context)).intValue();
            int maxrows = this.maxRows == null ? -1 : ((Number) this.maxRows.evaluate(DataAccessor.NULL, context)).intValue();
            LimitedDataScanner limited = new LimitedDataScanner(inputScanner, maxrows, offset, context);
            return new ScanResult(downstreamScanResult.transactionId, limited);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

}
