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

import herddb.model.ExecutionPlan;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;

import static herddb.sql.expressions.SQLExpressionCompiler.compileExpression;
import herddb.utils.DataAccessor;
import static herddb.utils.SQLRecordPredicateFunctions.objectEquals;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class CompiledInExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression left;
    private final List<CompiledSQLExpression> inExpressions;
    private final PlainSelect inSubSelectPlain;
    private final boolean not;

    public CompiledInExpression(boolean not, CompiledSQLExpression left,
            List<CompiledSQLExpression> inExpressions, SubSelect inSubSelect) {
        this.left = left;
        this.not = not;
        this.inExpressions = inExpressions;
        if (inSubSelect != null) {
            if (inSubSelect.getSelectBody() instanceof PlainSelect) {
                this.inSubSelectPlain = (PlainSelect) inSubSelect.getSelectBody();
            } else {
                throw new StatementExecutionException("unsupported operand \"IN\""
                        + " with subquery of type " + inSubSelect.getSelectBody().getClass()
                        + "(" + inSubSelect.getSelectBody() + ")");
            }
        } else {
            this.inSubSelectPlain = null;
        }
    }

    public static CompiledInExpression create(InExpression in, String validatedTableAlias) {
        if (in.getLeftItemsList() != null) {
            throw new StatementExecutionException("Unsupported operand " + in.getClass() + " with a non-expression left argument (" + in + ")");
        }
        CompiledSQLExpression left = compileExpression(validatedTableAlias, in.getLeftExpression());
        if (left == null) {
            return null;
        }

        if (in.getRightItemsList() instanceof ExpressionList) {
            List<CompiledSQLExpression> expList = new ArrayList<>();
            ExpressionList exps = (ExpressionList) in.getRightItemsList();
            for (Expression exp : exps.getExpressions()) {
                CompiledSQLExpression newExp = compileExpression(validatedTableAlias, exp);
                if (newExp == null) {
                    return null;
                }
                expList.add(newExp);
            }
            return new CompiledInExpression(in.isNot(), left, expList, null);
        }

        if (in.getRightItemsList() instanceof SubSelect) {
            SubSelect ss = (SubSelect) in.getRightItemsList();
            if (!(ss.getSelectBody() instanceof PlainSelect)) {
                throw new StatementExecutionException("unsupported operand " + in.getClass()
                        + " with subquery of type " + ss.getClass() + "(" + ss + ")");
            }
            return new CompiledInExpression(in.isNot(), left, null, ss);
        }

        throw new StatementExecutionException("unsupported operand " + in.getClass()
                + " with argument of type " + in.getRightItemsList().getClass() + "(" + in + ")");
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {

        Object leftValue = left.evaluate(bean, context);
        boolean res = false;

        if (inExpressions != null) {
            for (CompiledSQLExpression exp : inExpressions) {
                Object expValue = exp.evaluate(bean, context);
                if (objectEquals(leftValue, expValue)) {
                    res = true;
                    break;
                }
            }

        } else if (inSubSelectPlain != null) {
            List<DataAccessor> subQueryResult = context.executeSubquery(inSubSelectPlain);
            for (DataAccessor t : subQueryResult) {
                String[] fieldNames = t.getFieldNames();
                if (fieldNames.length > 1) {
                    throw new StatementExecutionException("subquery returned more than one column");
                }
                Object tuple_value = t.get(fieldNames[0]);
                if (objectEquals(leftValue, tuple_value)) {
                    res = true;
                    break;
                }
            }
        } else {
            throw new StatementExecutionException("Internal error");
        }

        if (not) {
            return !res;
        } else {
            return res;
        }

    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        if (this.left != null) {
            left.validate(context);
        }
        if (this.inExpressions != null) {
            this.inExpressions.forEach(s -> s.validate(context));
        }
        if (inSubSelectPlain != null) {
            ExecutionPlan plan = context.compileSubplan(inSubSelectPlain);
            plan.validateContext(context);
        }
    }

}
