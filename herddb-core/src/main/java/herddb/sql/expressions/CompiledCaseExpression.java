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

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import static herddb.sql.SQLRecordPredicate.toBoolean;
import static herddb.sql.expressions.SQLExpressionCompiler.compileExpression;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WhenClause;

public class CompiledCaseExpression implements CompiledSQLExpression {

    private final List<Entry<CompiledSQLExpression, CompiledSQLExpression>> whenExpressions;
    private final CompiledSQLExpression elseExpression;

    public CompiledCaseExpression(List<Entry<CompiledSQLExpression, CompiledSQLExpression>> whenExpressions, CompiledSQLExpression elseExpression) {
        this.whenExpressions = whenExpressions;
        this.elseExpression = elseExpression;
    }

    public static CompiledCaseExpression create(String validatedTableAlias, CaseExpression caseExpression) {
        Expression switchExpression = caseExpression.getSwitchExpression();
        if (switchExpression != null) {
            throw new StatementExecutionException("unhandled expression CASE SWITCH, type " + caseExpression.getClass() + ": " + caseExpression);
        }
        List<Entry<CompiledSQLExpression, CompiledSQLExpression>> whens = null;
        if (caseExpression.getWhenClauses() != null) {
            whens = new ArrayList<>();
            for (Expression when : caseExpression.getWhenClauses()) {
                WhenClause whenClause = (WhenClause) when;
                CompiledSQLExpression whenCondition = compileExpression(validatedTableAlias, whenClause.getWhenExpression());
                if (whenCondition == null) {
                    return null;
                }
                CompiledSQLExpression thenExpr = compileExpression(validatedTableAlias, whenClause.getThenExpression());
                whens.add(new AbstractMap.SimpleImmutableEntry<>(whenCondition, thenExpr));
            }
        }
        Expression elseExp = caseExpression.getElseExpression();
        if (elseExp != null) {
            CompiledSQLExpression elseExpression = compileExpression(validatedTableAlias, elseExp);
            if (elseExpression == null) {
                return null;
            }
            return new CompiledCaseExpression(whens, elseExpression);
        } else {
            return new CompiledCaseExpression(whens, null);
        }
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        if (whenExpressions != null) {
            for (Entry<CompiledSQLExpression, CompiledSQLExpression> entry : whenExpressions) {
                Object whenValue = entry.getKey().evaluate(bean, context);
                if (toBoolean(whenValue)) {
                    return entry.getValue().evaluate(bean, context);
                }
            }
        }
        if (elseExpression != null) {
            return elseExpression.evaluate(bean, context);
        } else {
            return null;
        }
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        if (whenExpressions != null) {
            for (Entry<CompiledSQLExpression, CompiledSQLExpression> entry : whenExpressions) {
                entry.getKey().validate(context);
                entry.getValue().validate(context);
            }
        }
        if (elseExpression != null) {
            elseExpression.validate(context);
        }
    }

  
}
