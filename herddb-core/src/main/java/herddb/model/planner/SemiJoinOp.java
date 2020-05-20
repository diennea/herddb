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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;

/**
 * Semi join operation
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class SemiJoinOp implements PlannerOp {

    private final int[] leftKeys;
    private final PlannerOp left;
    private final int[] rightKeys;
    private final PlannerOp right;
    private final String[] fieldNames;
    private final Column[] columns;

    public SemiJoinOp(
            String[] fieldNames,
            Column[] columns, int[] leftKeys, PlannerOp left,
            int[] rightKeys, PlannerOp right
    ) {
        this.fieldNames = fieldNames;
        this.columns = columns;
        this.leftKeys = leftKeys;
        this.left = left.optimize();
        this.rightKeys = rightKeys;
        this.right = right.optimize();
    }

    @Override
    public String getTablespace() {
        return left.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        ScanResult resLeft = (ScanResult) left.execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        transactionContext = new TransactionContext(resLeft.transactionId);
        ScanResult resRight = (ScanResult) right.execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        final long resTransactionId = resRight.transactionId;
        Enumerable<DataAccessor> result = EnumerableDefaults.semiJoin(
                resLeft.dataScanner.createRewindOnCloseEnumerable(),
                resRight.dataScanner.createRewindOnCloseEnumerable(),
                JoinKey.keyExtractor(leftKeys),
                JoinKey.keyExtractor(rightKeys)
        );
        EnumerableDataScanner joinedScanner = new EnumerableDataScanner(resRight.dataScanner.getTransaction(), fieldNames, columns, result, resLeft.dataScanner, resRight.dataScanner);
        return new ScanResult(resTransactionId, joinedScanner);
    }

    @Override
    public String toString() {
        return String.format("SemiJoinOp {leftKeySize = %d rightKeySize = %d }", leftKeys.length , rightKeys.length);
    }
}
