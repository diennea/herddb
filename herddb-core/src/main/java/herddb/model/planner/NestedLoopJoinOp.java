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
import herddb.sql.CalciteEnumUtils;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.Arrays;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * theta oin operation
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class NestedLoopJoinOp implements PlannerOp {

    private final PlannerOp left;
    private final PlannerOp right;
    private final String[] fieldNames;
    private final Column[] columns;
    private final JoinRelType joinRelType;
    private final CompiledSQLExpression condition;

    public NestedLoopJoinOp(
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

            DataScanner leftScanner = resLeft.dataScanner;
            transactionContext = new TransactionContext(resLeft.transactionId);
            ScanResult resRight = (ScanResult) right.execute(tableSpaceManager, transactionContext,
                    context, lockRequired, forWrite);
            DataScanner rightScanner = resRight.dataScanner;
            final JoinType linq4jJoinType = CalciteEnumUtils.toLinq4jJoinType(joinRelType);

            // in case of !generatesNullsOnLeft() EnumerableDefaults#nestedLoopJoin
            // will rewind the "right" (outer) scanner
            // we have to swap it to memory or disk
            // otherwise Calcite will dump it to memory (heap)!
            // this is not good, but it has to be improved inside Calcite

            if (!linq4jJoinType.generatesNullsOnLeft()
                    && !rightScanner.isRewindSupported()) {
                try {
                    MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                            .createRecordSet(rightScanner.getFieldNames(),
                                    rightScanner.getSchema());
                    rightScanner.forEach(d -> {
                        recordSet.add(d);
                    });
                    recordSet.writeFinished();
                    SimpleDataScanner materialized = new SimpleDataScanner(rightScanner.getTransaction(), recordSet);
                    rightScanner.close();
                    rightScanner = materialized;
                } catch (DataScannerException err) {
                    throw new StatementExecutionException(err);
                }

            }

            final long resTransactionId = resRight.transactionId;
            final String[] fieldNamesFromLeft = leftScanner.getFieldNames();
            final String[] fieldNamesFromRight = rightScanner.getFieldNames();
            final Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection = resultProjection(fieldNamesFromLeft, fieldNamesFromRight);
            Enumerable<DataAccessor> result = EnumerableDefaults
                    .nestedLoopJoin(leftScanner.createNonRewindableEnumerable(),
                            rightScanner.createRewindOnCloseEnumerable(),
                            predicate(resultProjection, context),
                            resultProjection, linq4jJoinType);
            EnumerableDataScanner joinedScanner = new EnumerableDataScanner(rightScanner.getTransaction(), fieldNames, columns, result, leftScanner, rightScanner);
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

    @Override
    public String toString() {
        return "NestedLoopJoinOp{" + "\nleft=" + left
                + "\n, right=" + right
                + "\n, fieldNames=" + Arrays.toString(fieldNames)
                + "\n, columns=" + Arrays.toString(columns)
                + "\n, joinRelType=" + joinRelType
                + "\n, condition=" + condition + '}';
    }

    public PlannerOp getLeft() {
        return left;
    }

    public PlannerOp getRight() {
        return right;
    }

}
