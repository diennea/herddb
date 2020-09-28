/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql.expressions;

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.ArrayList;
import java.util.List;

public class CompiledAndExpression extends CompiledBinarySQLExpression {

    public CompiledAndExpression(CompiledSQLExpression left, CompiledSQLExpression right) {
        super(left, right);
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        boolean ok = SQLRecordPredicateFunctions.toBoolean(left.evaluate(bean, context));
        if (!ok) {
            return false;
        }
        return SQLRecordPredicateFunctions.toBoolean(right.evaluate(bean, context));
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        left.validate(context);
        right.validate(context);
    }

    @Override
    public String toString() {
        return "CompiledAndExpression{" + "left=" + left + ", right=" + right + '}';
    }

    @Override
    public List<CompiledSQLExpression> scanForConstraintsOnColumn(String column, BindableTableScanColumnNameResolver columnNameResolver) {
        List<CompiledSQLExpression> res = new ArrayList<>();
        res.addAll(left.scanForConstraintsOnColumn(column, columnNameResolver));
        res.addAll(right.scanForConstraintsOnColumn(column, columnNameResolver));
        return res;
    }

    @Override
    public List<CompiledSQLExpression> scanForConstraintedValueOnColumnWithOperator(String column, String operator, BindableTableScanColumnNameResolver columnNameResolver) {
        List<CompiledSQLExpression> res = new ArrayList<>();
        res.addAll(left.scanForConstraintedValueOnColumnWithOperator(column, operator, columnNameResolver));
        res.addAll(right.scanForConstraintedValueOnColumnWithOperator(column, operator, columnNameResolver));
        return res;
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        CompiledSQLExpression remappedLeft = left.remapPositionalAccessToToPrimaryKeyAccessor(projection);
        CompiledSQLExpression remappedRight = right.remapPositionalAccessToToPrimaryKeyAccessor(projection);
        return new CompiledAndExpression(remappedLeft, remappedRight);
    }

    @Override
    public CompiledSQLExpression simplify() {
        // naive simplifications, needed only for jSQLParser based planner
        
        // select * from tblspace1.tsql where n1 = 1 and n1 is not null
        // -> "n1 is not null" is redundant
        if (left instanceof CompiledBinarySQLExpression) {
            CompiledBinarySQLExpression l = (CompiledBinarySQLExpression) left;
            if (l instanceof CompiledEqualsExpression) {
                CompiledEqualsExpression eq = (CompiledEqualsExpression) l;
                if (eq.getLeft() instanceof AccessCurrentRowExpression) {
                    AccessCurrentRowExpression accessField = (AccessCurrentRowExpression) eq.getLeft();
                    
                    if (right instanceof CompiledIsNullExpression) {
                        CompiledIsNullExpression r = (CompiledIsNullExpression) right;
                        if (r.isNot() && r.getLeft() instanceof AccessCurrentRowExpression) {
                            AccessCurrentRowExpression accessFieldOnRight = (AccessCurrentRowExpression) r.getLeft();
                            
                            if (accessFieldOnRight.getIndex() == accessField.getIndex()) {
                                return left;
                            }
                            
                        }
                    }
                    
                }
            }
        }
        return null;
    }
}
