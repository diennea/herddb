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
package herddb.sql.expressions;

import herddb.model.Predicate;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.ObjectSizeUtils;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.Collections;
import java.util.List;

/**
 * A specific implementation of a predicate
 *
 * @author enrico.olivelli
 */
public interface CompiledSQLExpression {

    /**
     * Evaluates the expression
     *
     * @param bean
     * @param context
     * @return
     * @throws StatementExecutionException
     */
    Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException;

    default boolean opEqualsTo(herddb.utils.DataAccessor bean, StatementEvaluationContext context, CompiledSQLExpression right) throws StatementExecutionException {
        Object leftValue = this.evaluate(bean, context);
        if (leftValue == null) {
            // NULL is never equal to any other value, even NULL is not equal to NULL
            return false;
        }
        Object rightValue = right.evaluate(bean, context);
        if (rightValue == null) {
            // NULL is never equal to any other value, even NULL is not equal to NULL
            return false;
        }
        return SQLRecordPredicateFunctions.objectEquals(leftValue, rightValue);
    }

    default boolean opNotEqualsTo(herddb.utils.DataAccessor bean, StatementEvaluationContext context, CompiledSQLExpression right) throws StatementExecutionException {
        Object leftValue = this.evaluate(bean, context);
        if (leftValue == null) {
            // NULL is never non-equal to any other value, even NULL is not non-equal to NULL
            return false;
        }
        Object rightValue = right.evaluate(bean, context);
        if (rightValue == null) {
            // NULL is never non-equal to any other value, even NULL is not non-equal to NULL
            return false;
        }
        return SQLRecordPredicateFunctions.objectNotEquals(leftValue, rightValue);
    }

    default SQLRecordPredicateFunctions.CompareResult opCompareTo(herddb.utils.DataAccessor bean, StatementEvaluationContext context, CompiledSQLExpression right) throws StatementExecutionException {
        Object leftValue = this.evaluate(bean, context);
        Object rightValue = right.evaluate(bean, context);
        return SQLRecordPredicateFunctions.compareConsiderNull(leftValue, rightValue);
    }

    /**
     * Validates the expression without actually doing complex operation
     *
     * @param context
     * @throws StatementExecutionException
     */
    default void validate(StatementEvaluationContext context) throws StatementExecutionException {
    }

    default List<CompiledSQLExpression> scanForConstraintedValueOnColumnWithOperator(
            String column, String operator, BindableTableScanColumnNameResolver columnNameResolver
    ) {
        return Collections.emptyList();
    }

    default List<CompiledSQLExpression> scanForConstraintsOnColumn(
            String column, BindableTableScanColumnNameResolver columnNameResolver
    ) {
        return Collections.emptyList();
    }

    default CompiledSQLExpression cast(int type) {
        return new CastExpression(this, type);
    }

    /**
     * the function {@link Predicate#matchesRawPrimaryKey(herddb.utils.Bytes, herddb.model.StatementEvaluationContext)} works on a projection of the table wich contains only the pk fields of the table
     * for instance if the predicate wants to access first element of the pk, and this field is the 3rd in the column list then you will find {@link AccessCurrentRowExpression} with index=2. To this
     * expression you have to apply the projection and map 2 (3rd element of the table) to 0 (1st element of the pk)
     *
     * @param projection a map from index on table to the index on pk
     */
    default CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        throw new IllegalStateException("not implemented for " + this.getClass());
    }

    /**
     * Estimate Object size for the PlanCache. see {@link ObjectSizeUtils} for the limitations of this computation.
     */
    default int estimateObjectSizeForCache() {
        return ObjectSizeUtils.DEFAULT_OBJECT_SIZE_OVERHEAD;
    }
    
    /**
     * Naive simplifications, for jSQLParser based planner
     */
    default CompiledSQLExpression simplify() {
        return this;
    }
}
