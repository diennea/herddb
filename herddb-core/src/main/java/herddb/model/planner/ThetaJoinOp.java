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
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.CalciteEnumUtils;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import herddb.utils.SQLRecordPredicateFunctions;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * theta oin operation
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class ThetaJoinOp implements PlannerOp {

    private final PlannerOp left;
    private final PlannerOp right;
    private final String[] fieldNames;
    private final Column[] columns;
    private final JoinRelType joinRelType;
    private final CompiledSQLExpression condition;

    public ThetaJoinOp(
            String[] fieldNames,
            Column[] columns, PlannerOp left,
            PlannerOp right,
            CompiledSQLExpression condition,
            JoinRelType joinRelType,
            boolean mergeJoin) {
        this.fieldNames = fieldNames;
        this.columns = columns;
        this.left = left.optimize();
        this.right = right.optimize();
        this.joinRelType = joinRelType;
        this.condition = condition;
    }

    @Override
    public String getTablespace() {
        return left.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        ScanResult resLeft = (ScanResult) left.execute(tableSpaceManager, transactionContext,
                context, lockRequired, forWrite);
        transactionContext = new TransactionContext(resLeft.transactionId);
        ScanResult resRight = (ScanResult) right.execute(tableSpaceManager, transactionContext,
                context, lockRequired, forWrite);
        final long resTransactionId = resRight.transactionId;
        final String[] fieldNamesFromLeft = resLeft.dataScanner.getFieldNames();
        final String[] fieldNamesFromRight = resRight.dataScanner.getFieldNames();
        final Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection = resultProjection(fieldNamesFromLeft, fieldNamesFromRight);


        Enumerable<DataAccessor> result = EnumerableDefaults.nestedLoopJoin(resLeft.dataScanner.createEnumerable(),
                resRight.dataScanner.createEnumerable(),
                predicate(resultProjection, context), resultProjection,
                CalciteEnumUtils.toLinq4jJoinType(joinRelType)
        );
        EnumerableDataScanner joinedScanner = new EnumerableDataScanner(resRight.dataScanner.getTransaction(), fieldNames, columns, result);
        return new ScanResult(resTransactionId, joinedScanner);

    }

    private Predicate2<DataAccessor, DataAccessor> predicate(
            Function2<DataAccessor, DataAccessor, DataAccessor> projection,
            StatementEvaluationContext context
    ) {
        return (DataAccessor v0, DataAccessor v1) -> {
            DataAccessor currentRow = projection.apply(v0, v1);
            return SQLRecordPredicateFunctions.toBoolean(condition.evaluate(currentRow, context));
        };
    }

    private Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection(
            String[] fieldNamesFromLeft,
            String[] fieldNamesFromRight
    ) {
        final DataAccessor nullsOnLeft = DataAccessor.ALL_NULLS(fieldNamesFromLeft);
        final DataAccessor nullsOnRight = DataAccessor.ALL_NULLS(fieldNamesFromRight);

        return (DataAccessor a, DataAccessor b)
                -> new ConcatenatedDataAccessor(fieldNames,
                a != null ? a : nullsOnLeft,
                b != null ? b : nullsOnRight);

    }
}
