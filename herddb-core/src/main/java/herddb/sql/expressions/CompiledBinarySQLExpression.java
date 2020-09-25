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

import herddb.model.Column;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.ObjectSizeUtils;
import java.util.Collections;
import java.util.List;

/**
 * Parent for AND/OR logical expression
 */
public abstract class CompiledBinarySQLExpression implements CompiledSQLExpression {

    protected final CompiledSQLExpression left;
    protected final CompiledSQLExpression right;

    public CompiledBinarySQLExpression(CompiledSQLExpression left, CompiledSQLExpression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        left.validate(context);
        right.validate(context);
    }

    public String getOperator() {
        return "";
    }

    @Override
    public List<CompiledSQLExpression> scanForConstraintedValueOnColumnWithOperator(String column, String operator, BindableTableScanColumnNameResolver columnNameResolver) {
        if (!operator.equals(getOperator())) {
            return Collections.emptyList();
        }
        if ((right instanceof ConstantExpression
                || right instanceof TypedJdbcParameterExpression
                || right instanceof JdbcParameterExpression)
                && (left instanceof AccessCurrentRowExpression)) {
            AccessCurrentRowExpression ex = (AccessCurrentRowExpression) left;
            Column colName = columnNameResolver.resolveColumName(ex.getIndex());
            if (column.equals(colName.name)) {
                return Collections.singletonList(right.cast(colName.type));
            }
        }
        return Collections.emptyList();
    }

    @Override
    public List<CompiledSQLExpression> scanForConstraintsOnColumn(String column, BindableTableScanColumnNameResolver columnNameResolver) {

        if ((right instanceof ConstantExpression
                || right instanceof TypedJdbcParameterExpression
                || right instanceof JdbcParameterExpression)
                && (left instanceof AccessCurrentRowExpression)) {
            AccessCurrentRowExpression ex = (AccessCurrentRowExpression) left;
            Column colName = columnNameResolver.resolveColumName(ex.getIndex());
            if (column.equals(colName.name)) {
                return Collections.singletonList(this);
            }
        }
        return Collections.emptyList();
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        throw new IllegalStateException("no implemented");
    }

    @Override
    public int estimateObjectSizeForCache() {
        return ObjectSizeUtils.DEFAULT_OBJECT_SIZE_OVERHEAD + right.estimateObjectSizeForCache() + left.estimateObjectSizeForCache();
    }

    @Override
    public String toString() {
        return "BINARY-EXP{op=" + getOperator() + ", left=" + left + ", right=" + right + '}';
    }

    public boolean isNegateSupported() {
        return false;
    }

    public CompiledBinarySQLExpression negate() {
        throw new UnsupportedOperationException();
    }

}
