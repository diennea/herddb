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
package herddb.sql;

import herddb.codec.RecordSerializer;
import herddb.model.Column;
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

/**
 * Predicate expressed using SQL syntax
 *
 * @author enrico.olivelli
 */
public class SQLRecordPredicate extends Predicate {

    private final Table table;
    private final Expression where;
    private final int firstParameterPos;

    private class EvaluationState {

        int parameterPos;
        List<Object> parameters;

        public EvaluationState(int parameterPos, List<Object> parameters) {
            this.parameterPos = parameterPos;
            this.parameters = parameters;
        }
    }

    public SQLRecordPredicate(Table table, Expression where, int parameterPos) {
        this.table = table;
        this.where = where;
        this.firstParameterPos = parameterPos;
    }

    @Override
    public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
        SQLStatementEvaluationContext sqlContext = (SQLStatementEvaluationContext) context;
        Map<String, Object> bean = RecordSerializer.toBean(record, table);
        EvaluationState state = new EvaluationState(firstParameterPos, sqlContext.jdbcParameters);
        return toBoolean(evaluateExpression(where, bean, state));
    }

    private static boolean toBoolean(Object result) {
        if (result == null) {
            return false;
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        return Boolean.parseBoolean(result.toString());
    }

    private Object handleNot(boolean not, Object result) {
        if (not) {
            return !toBoolean(result);
        }
        return result;
    }

    private Object evaluateExpression(Expression expression, Map<String, Object> bean, EvaluationState state) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            return state.parameters.get(state.parameterPos++);
        }
        if (expression instanceof EqualsTo) {
            EqualsTo e = (EqualsTo) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), Objects.equals(left, right));
        }
        if (expression instanceof NotEqualsTo) {
            NotEqualsTo e = (NotEqualsTo) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), !Objects.equals(left, right));
        }
        if (expression instanceof AndExpression) {
            AndExpression a = (AndExpression) expression;

            if (!toBoolean(evaluateExpression(a.getLeftExpression(), bean, state))) {
                return !a.isNot();
            }
            return handleNot(a.isNot(), toBoolean(evaluateExpression(a.getRightExpression(), bean, state)));
        }
        if (expression instanceof OrExpression) {
            OrExpression a = (OrExpression) expression;
            if (toBoolean(evaluateExpression(a.getLeftExpression(), bean, state))) {
                return !a.isNot();
            }
            return handleNot(a.isNot(), toBoolean(evaluateExpression(a.getRightExpression(), bean, state)));
        }
        if (expression instanceof net.sf.jsqlparser.schema.Column) {
            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) expression;
            if (c.getTable() != null && (c.getTable().getName() != null || c.getTable().getAlias() != null)) {
                throw new StatementExecutionException("unsupported fully qualified column reference" + expression);
            }
            return bean.get(c.getColumnName());
        }
        if (expression instanceof Parenthesis) {
            Parenthesis p = (Parenthesis) expression;
            return handleNot(p.isNot(), evaluateExpression(p.getExpression(), bean, state));
        }
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        }
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }
        if (expression instanceof IsNullExpression) {
            IsNullExpression e = (IsNullExpression) expression;
            Object value = evaluateExpression(e.getLeftExpression(), bean, state);
            boolean result = value == null;
            return handleNot(e.isNot(), result);
        }
        throw new StatementExecutionException("unsupported operand " + expression.getClass());
    }

}
