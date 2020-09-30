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
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;

/**
 * basic join operation
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public class JoinOp implements PlannerOp {

    private final int[] leftKeys;
    private final PlannerOp left;
    private final int[] rightKeys;
    private final PlannerOp right;
    private final String[] fieldNames;
    private final Column[] columns;
    private final boolean generateNullsOnLeft;
    private final boolean generateNullsOnRight;
    private final boolean mergeJoin;
    private final List<CompiledSQLExpression> nonEquiConditions;

    public JoinOp(String[] fieldNames,
            Column[] columns, int[] leftKeys, PlannerOp left,
            int[] rightKeys, PlannerOp right,
            boolean generateNullsOnLeft,
            boolean generateNullsOnRight,
            boolean mergeJoin,
            List<CompiledSQLExpression> nonEquiConditions) {
        this.fieldNames = fieldNames;
        this.columns = columns;
        this.leftKeys = leftKeys;
        this.left = left.optimize();
        this.rightKeys = rightKeys;
        this.right = right.optimize();
        this.generateNullsOnLeft = generateNullsOnLeft;
        this.generateNullsOnRight = generateNullsOnRight;
        this.nonEquiConditions = nonEquiConditions;
        this.mergeJoin = mergeJoin;
    }

    @Override
    public String getTablespace() {
        return left.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        ScanResult resLeft = (ScanResult) left.execute(tableSpaceManager, transactionContext,
                context, lockRequired, forWrite);
        transactionContext = new TransactionContext(resLeft.transactionId);
        ScanResult resRight = (ScanResult) right.execute(tableSpaceManager, transactionContext,
                context, lockRequired, forWrite);
        final long resTransactionId = resRight.transactionId;
        DataScanner leftScanner = resLeft.dataScanner;
        final String[] fieldNamesFromLeft = leftScanner.getFieldNames();
        final String[] fieldNamesFromRight = resRight.dataScanner.getFieldNames();
        Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection = resultProjection(fieldNamesFromLeft, fieldNamesFromRight);
        final Predicate2 predicate;
        DataScanner rightScanner = resRight.dataScanner;
        if (nonEquiConditions != null && !nonEquiConditions.isEmpty()) {
            if (mergeJoin) {
                throw new IllegalStateException("Unspected nonEquiConditions " + nonEquiConditions + ""
                        + "for merge join");
            }
            predicate = (Predicate2) (Object t0, Object t1) -> {
                DataAccessor da0 = (DataAccessor) t0;
                DataAccessor da1 = (DataAccessor) t1;
                DataAccessor currentRow = resultProjection.apply(da0, da1);
                for (CompiledSQLExpression exp : nonEquiConditions) {
                    Object result = exp.evaluate(currentRow, context);
                    boolean asBoolean = SQLRecordPredicateFunctions.toBoolean(result);
                    if (!asBoolean) {
                        return false;
                    }
                }
                return true;
            };
        } else {
            predicate = null;
        }
        if (!mergeJoin && !leftScanner.isRewindSupported()) {
            try {
                MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                        .createRecordSet(leftScanner.getFieldNames(),
                                leftScanner.getSchema());
                leftScanner.forEach(d -> {
                    recordSet.add(d);
                });
                recordSet.writeFinished();
                SimpleDataScanner materialized = new SimpleDataScanner(leftScanner.getTransaction(), recordSet);
                leftScanner.close();
                leftScanner = materialized;
            } catch (DataScannerException err) {
                throw new StatementExecutionException(err);
            }
        }
        if (!mergeJoin && !rightScanner.isRewindSupported()) {
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

        Enumerable<DataAccessor> result = mergeJoin
                ? EnumerableDefaults.mergeJoin(leftScanner.createNonRewindableEnumerable(),
                        rightScanner.createNonRewindableEnumerable(),
                        JoinKey.keyExtractor(leftKeys),
                        JoinKey.keyExtractor(rightKeys),
                        resultProjection,
                        generateNullsOnLeft,
                        generateNullsOnRight
                )
                : EnumerableDefaults.hashJoin(leftScanner.createRewindOnCloseEnumerable(),
                        rightScanner.createRewindOnCloseEnumerable(),
                        JoinKey.keyExtractor(leftKeys),
                        JoinKey.keyExtractor(rightKeys),
                        resultProjection,
                        null,
                        generateNullsOnLeft,
                        generateNullsOnRight,
                        predicate
                );
        EnumerableDataScanner joinedScanner = new EnumerableDataScanner(rightScanner.getTransaction(), fieldNames, columns, result, leftScanner, rightScanner);
        return new ScanResult(resTransactionId, joinedScanner);

    }

    private Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection(
            String[] fieldNamesFromLeft,
            String[] fieldNamesFromRight) {
        DataAccessor nullsOnLeft = DataAccessor.ALL_NULLS(fieldNamesFromLeft);
        DataAccessor nullsOnRight = DataAccessor.ALL_NULLS(fieldNamesFromRight);

        return (DataAccessor a, DataAccessor b)
                -> new ConcatenatedDataAccessor(fieldNames,
                        a != null ? a : nullsOnLeft,
                        b != null ? b : nullsOnRight);
    }

    @Override
    public String toString() {
        return "JoinOp{fieldNames=" + Arrays.toString(fieldNames) + ", columns=" + Arrays.toString(columns) + ","
                + "\ngenerateNullsOnLeft=" + generateNullsOnLeft + ", generateNullsOnRight=" + generateNullsOnRight + ", mergeJoin=" + mergeJoin + ","
                + "\nleftKeys=" + Arrays.toString(leftKeys) + ",left=" + left + ","
                + "\nrightKeys=" + Arrays.toString(rightKeys) + ", right=" + right + '}';
    }


    @Override
    public Column[] getOutputSchema() {
        return columns;
    }

    public PlannerOp getLeft() {
        return left;
    }

    public PlannerOp getRight() {
        return right;
    }

}
