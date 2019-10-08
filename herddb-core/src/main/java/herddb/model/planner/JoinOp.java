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
import herddb.utils.DataAccessor;
import java.util.Arrays;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.function.Function2;

/**
 * basic join operation
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
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

    public JoinOp(
            String[] fieldNames,
            Column[] columns, int[] leftKeys, PlannerOp left,
            int[] rightKeys, PlannerOp right,
            boolean generateNullsOnLeft,
            boolean generateNullsOnRight,
            boolean mergeJoin
    ) {
        this.fieldNames = fieldNames;
        this.columns = columns;
        this.leftKeys = leftKeys;
        this.left = left.optimize();
        this.rightKeys = rightKeys;
        this.right = right.optimize();
        this.generateNullsOnLeft = generateNullsOnLeft;
        this.generateNullsOnRight = generateNullsOnRight;
        this.mergeJoin = mergeJoin;
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

        Enumerable<DataAccessor> result = mergeJoin
                ? EnumerableDefaults.mergeJoin(
                resLeft.dataScanner.createEnumerable(),
                resRight.dataScanner.createEnumerable(),
                JoinKey.keyExtractor(leftKeys),
                JoinKey.keyExtractor(rightKeys),
                resultProjection(fieldNamesFromLeft, fieldNamesFromRight),
                generateNullsOnLeft,
                generateNullsOnRight
        )
                : EnumerableDefaults.join(
                resLeft.dataScanner.createEnumerable(),
                resRight.dataScanner.createEnumerable(),
                JoinKey.keyExtractor(leftKeys),
                JoinKey.keyExtractor(rightKeys),
                resultProjection(fieldNamesFromLeft, fieldNamesFromRight),
                null,
                generateNullsOnLeft,
                generateNullsOnRight
        );
        EnumerableDataScanner joinedScanner = new EnumerableDataScanner(resRight.dataScanner.getTransaction(), fieldNames, columns, result);
        return new ScanResult(resTransactionId, joinedScanner);

    }

    private Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection(
            String[] fieldNamesFromLeft,
            String[] fieldNamesFromRight
    ) {
        DataAccessor nullsOnLeft = DataAccessor.ALL_NULLS(fieldNamesFromLeft);
        DataAccessor nullsOnRight = DataAccessor.ALL_NULLS(fieldNamesFromRight);

        return (DataAccessor a, DataAccessor b)
                -> new ConcatenatedDataAccessor(fieldNames,
                a != null ? a : nullsOnLeft,
                b != null ? b : nullsOnRight);
    }

    @Override
    public String toString() {
        return "JoinOp{" + "leftKeys=" + Arrays.toString(leftKeys) + ", left=" + left + ", rightKeys=" + Arrays.toString(rightKeys) + ", right=" + right + ", fieldNames=" + Arrays.toString(fieldNames) + ", columns=" + Arrays.toString(columns) + ", generateNullsOnLeft=" + generateNullsOnLeft + ", generateNullsOnRight=" + generateNullsOnRight + ", mergeJoin=" + mergeJoin + '}';
    }


}
