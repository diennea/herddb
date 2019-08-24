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
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import java.util.Iterator;
import java.util.List;

/**
 * Constant values
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class ValuesOp implements PlannerOp {

    private final String tablespace;
    private final String[] fieldNames;
    private final Column[] columns;
    private final List<List<CompiledSQLExpression>> tuples;

    public ValuesOp(String tablespace, String[] fieldNames, Column[] columns, List<List<CompiledSQLExpression>> tuples) {
        this.tablespace = tablespace;
        this.fieldNames = fieldNames;
        this.columns = columns;
        this.tuples = tuples;
    }

    @Override
    public String getTablespace() {
        return tablespace;
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        Iterator<List<CompiledSQLExpression>> it = tuples.iterator();
        Transaction transaction = tableSpaceManager.getTransaction(transactionContext.transactionId);

        DataScanner res = new DataScanner(transaction, fieldNames, columns) {
            @Override
            public boolean hasNext() throws DataScannerException {
                return it.hasNext();
            }

            @Override
            public DataAccessor next() throws DataScannerException {
                Object[] values = new Object[fieldNames.length];
                List<CompiledSQLExpression> tuple = it.next();
                for (int i = 0; i < values.length; i++) {
                    Object value = tuple.get(i).evaluate(DataAccessor.NULL, context);
                    values[i] = value;
                }
                return new Tuple(fieldNames, values);
            }

        };
        return new ScanResult(transactionContext.transactionId, res);
    }

}
