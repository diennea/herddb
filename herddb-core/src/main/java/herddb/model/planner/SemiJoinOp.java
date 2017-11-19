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
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.function.Function1;

/**
 * Semi join operation
 *
 * @author eolivelli
 */
public class SemiJoinOp implements PlannerOp {

    private final int[] leftKeys;
    private final PlannerOp left;
    private final int[] rightKeys;
    private final PlannerOp right;
    private final String[] fieldNames;
    private final Column[] columns;

    public SemiJoinOp(String[] fieldNames,
            Column[] columns, int[] leftKeys, PlannerOp left,
            int[] rightKeys, PlannerOp right) {
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
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        ScanResult resLeft = (ScanResult) left.execute(tableSpaceManager, transactionContext, context);
        transactionContext = new TransactionContext(resLeft.transactionId);
        ScanResult resRight = (ScanResult) right.execute(tableSpaceManager, transactionContext, context);
        final long resTransactionId = resRight.transactionId;
        Enumerable<DataAccessor> result = EnumerableDefaults.semiJoin(
                resLeft.dataScanner.createEnumerable(),
                resRight.dataScanner.createEnumerable(),
                keyExtractor(leftKeys),
                keyExtractor(rightKeys)
        );
        EnumerableDataScanner joinedScanner = new EnumerableDataScanner(resTransactionId, fieldNames, columns, result);
        return new ScanResult(resTransactionId, joinedScanner);

    }

    private static class RecordKey {

        private final DataAccessor dataAccessor;
        private final int[] selectedFields;

        public RecordKey(DataAccessor dataAccessor, int[] selectedFields) {
            this.dataAccessor = dataAccessor;
            this.selectedFields = selectedFields;
        }

        @Override
        public boolean equals(Object obj) {

            RecordKey da = (RecordKey) obj;
            int size = this.selectedFields.length;

            // leverage zero-copy and to not create temporary arrays
            for (int i = 0; i < size; i++) {
                if (!Objects.equals(dataAccessor.get(i), da.dataAccessor.get(i))) {
                    return false;
                }
            }
            return true;
        }

        private int _hashcode = Integer.MIN_VALUE;

        @Override
        public int hashCode() {
            if (_hashcode == Integer.MIN_VALUE) {
                int size = this.selectedFields.length;
                int res = 0;
                // leverage zero-copy and to not create temporary arrays
                for (int i = 0; i < size; i++) {
                    res += Objects.hashCode(dataAccessor.get(i));
                }
                _hashcode = res;
            }
            return _hashcode;
        }

    }

    private static Function1<DataAccessor, RecordKey> keyExtractor(
            int[] projection) {
        return (DataAccessor a) -> new RecordKey(a, projection);
    }

}
