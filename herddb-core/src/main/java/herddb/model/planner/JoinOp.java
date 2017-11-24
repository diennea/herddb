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
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.sql.SQLRecordPredicate;
import herddb.utils.AbstractDataAccessor;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

/**
 * basic join operation
 *
 * @author eolivelli
 */
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

    public JoinOp(String[] fieldNames,
            Column[] columns, int[] leftKeys, PlannerOp left,
            int[] rightKeys, PlannerOp right,
            boolean generateNullsOnLeft,
            boolean generateNullsOnRight,
            boolean mergeJoin) {
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
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
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
                        keyExtractor(leftKeys),
                        keyExtractor(rightKeys),
                        resultProjection(fieldNamesFromLeft, fieldNamesFromRight),
                        generateNullsOnLeft,
                        generateNullsOnRight
                )
                : EnumerableDefaults.join(
                        resLeft.dataScanner.createEnumerable(),
                        resRight.dataScanner.createEnumerable(),
                        keyExtractor(leftKeys),
                        keyExtractor(rightKeys),
                        resultProjection(fieldNamesFromLeft, fieldNamesFromRight),
                        null,
                        generateNullsOnLeft,
                        generateNullsOnRight
                );
        EnumerableDataScanner joinedScanner = new EnumerableDataScanner(resTransactionId, fieldNames, columns, result);
        return new ScanResult(resTransactionId, joinedScanner);

    }

    private static class RecordKey implements Comparable<RecordKey> {

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
                final Object fromThis = dataAccessor.get(selectedFields[i]);
                final Object fromObj = da.dataAccessor.get(da.selectedFields[i]);
                int res = SQLRecordPredicate
                        .compare(fromThis, fromObj);
                if (res != 0) {
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
                    res += Objects.hashCode(dataAccessor.get(selectedFields[i]));
                }
                _hashcode = res;
            }
            return _hashcode;
        }

        @Override
        public int compareTo(RecordKey o) {
            RecordKey da = (RecordKey) o;
            int size = this.selectedFields.length;

            // leverage zero-copy and to not create temporary arrays
            for (int i = 0; i < size; i++) {
                final Object fromThis = dataAccessor.get(selectedFields[i]);
                final Object fromObj = da.dataAccessor.get(da.selectedFields[i]);
                int res = SQLRecordPredicate
                        .compare(fromThis, fromObj);
                
                if (res != 0) {
                    return res;
                }
            }
            return 0;
        }

        @Override
        public String toString() {
            return "RecordKey{" + "dataAccessor=" + dataAccessor
                    + ", selectedFields=" + Arrays.toString(selectedFields) + '}';
        }

    }

    private static Function1<DataAccessor, RecordKey> keyExtractor(
            int[] projection) {
        return (DataAccessor a) -> new RecordKey(a, projection);
    }

    private Function2<DataAccessor, DataAccessor, DataAccessor> resultProjection(
            String[] fieldNamesFromLeft,
            String[] fieldNamesFromRight) {
        DataAccessor nullsOnLeft = DataAccessor.ALL_NULLS(fieldNamesFromLeft);
        DataAccessor nullsOnRight = DataAccessor.ALL_NULLS(fieldNamesFromRight);
        
        return (DataAccessor a, DataAccessor b) ->
                new ConcatenatedDataAccessor(fieldNames,
                        a != null ? a : nullsOnLeft,
                        b != null ? b : nullsOnRight);
    }

    private static class ConcatenatedDataAccessor extends AbstractDataAccessor {

        private final String[] fieldNames;
        private final DataAccessor a;
        private final DataAccessor b;
        private final int endOfA;

        public ConcatenatedDataAccessor(String[] fieldNames, DataAccessor a, DataAccessor b) {
            this.fieldNames = fieldNames;
            this.a = a;
            this.b = b;
            this.endOfA = a.getNumFields();
        }

        @Override
        public Object get(String property) {
            for (int i = 0; i < fieldNames.length; i++) {
                if (property.equals(fieldNames[i])) {
                    return get(i);
                }
            }
            return null;
        }

        @Override
        public String[] getFieldNames() {
            return fieldNames;
        }

        @Override
        public int getNumFields() {
            return endOfA + b.getNumFields();
        }

        @Override
        public Object get(int index) {
            return index < endOfA ? a.get(index) : b.get(index - endOfA);
        }

        @Override
        public Object[] getValues() {
            Object[] res = new Object[endOfA + b.getNumFields()];
            for (int i = 0; i < res.length; i++) {
                res[i] = get(i);
            }
            return res;
        }

    }

}
