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
import herddb.model.Tuple;
import herddb.sql.SQLRecordPredicate;
import static herddb.sql.SQLRecordPredicate.compare;
import static herddb.sql.SQLRecordPredicate.like;
import static herddb.sql.SQLRecordPredicate.objectEquals;
import static herddb.sql.SQLRecordPredicate.subtract;
import static herddb.sql.SQLRecordPredicate.toBoolean;
import herddb.sql.SQLStatementEvaluationContext;
import herddb.sql.functions.BuiltinFunctions;
import static herddb.sql.functions.BuiltinFunctions.CURRENT_TIMESTAMP;
import herddb.utils.RawString;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * A generic 'interpreter' for SQL expression. This is just a fallback
 *
 * @author enrico.olivelli
 */
public class InterpretedSQLExpression implements CompiledSQLExpression {

    private final String validateTableAlias;
    private final Expression expression;

    public InterpretedSQLExpression(String validateTableAlias, Expression expression) {
        this.validateTableAlias = validateTableAlias;
        this.expression = expression;
    }

    @Override
    public Object evaluate(Map<String, Object> bean, StatementEvaluationContext context) throws StatementExecutionException {
        EvaluationState state = new EvaluationState(context.getJdbcParameters(), context);
        ExpressionEvaluator evaluator = new ExpressionEvaluator(bean, state, validateTableAlias);
        return evaluator.evaluateExpression(expression);
    }

    private static final class EvaluationState {

        final List<Object> parameters;
        final StatementEvaluationContext sqlContext;

        public EvaluationState(List<Object> parameters, StatementEvaluationContext sqlContext) {
            this.parameters = parameters;
            this.sqlContext = sqlContext;
        }
    }

    private final static class ExpressionEvaluator {

        private final Map<String, Object> record;
        private final EvaluationState state;
        private final String validatedTableAlias;

        ExpressionEvaluator(Map<String, Object> record, EvaluationState state, String validatedTableAlias) {
            this.record = record;
            this.state = state;
            this.validatedTableAlias = validatedTableAlias;
        }

        Object evaluateExpression(final Expression exp) throws StatementExecutionException {
            if (exp instanceof net.sf.jsqlparser.schema.Column) {
                return evaluateColumn(exp);
            } else if (exp instanceof StringValue) {
                return RawString.of(((StringValue) exp).getValue());
            } else if (exp instanceof LongValue) {
                return ((LongValue) exp).getValue();
            } else if (exp instanceof NullValue) {
                return null;
            } else if (exp instanceof TimestampValue) {
                return ((TimestampValue) exp).getValue();
            } else if (exp instanceof JdbcParameter) {
                int index = ((JdbcParameter) exp).getIndex();
                return state.parameters.get(index);
            } else if (exp instanceof Function) {
                Function f = (Function) exp;
                return computeFunction(f);
            } else if (exp instanceof Addition) {
                return evaluateAddition(exp);
            } else if (exp instanceof Subtraction) {
                return evaluateSubtraction(exp);
            } else if (exp instanceof TimeKeyExpression) {
                TimeKeyExpression ext = (TimeKeyExpression) exp;
                if (CURRENT_TIMESTAMP.equalsIgnoreCase(ext.getStringValue())) {
                    return new java.sql.Timestamp(System.currentTimeMillis());
                } else {
                    throw new StatementExecutionException("unhandled expression " + exp);
                }
            } else if (exp instanceof AndExpression) {
                return evaluateAndExpression(exp);
            } else if (exp instanceof OrExpression) {
                return evaluateOrExpression(exp);
            } else if (exp instanceof Parenthesis) {
                return evaluateParenthesis(exp);
            } else if (exp instanceof EqualsTo) {
                return evaluateEqualsTo(exp);
            } else if (exp instanceof NotEqualsTo) {
                return !evaluateEqualsTo(exp);
            } else if (exp instanceof MinorThan) {
                return evaluateMinorThan(exp);
            } else if (exp instanceof MinorThanEquals) {
                return evaluateMinorThanEquals(exp);
            } else if (exp instanceof GreaterThan) {
                return evaluateGreaterThan(exp);
            } else if (exp instanceof GreaterThanEquals) {
                return evaluateGreaterThanEquals(exp);
            } else if (exp instanceof LikeExpression) {
                return evaluateLike(exp);
            } else if (exp instanceof Between) {
                return evaluateBetween(exp);
            } else if (exp instanceof SignedExpression) {
                return evaluateSignedEpression(exp);
            } else if (exp instanceof TimeKeyExpression) {
                return evaluateTimeKeyExpression(exp);
            } else if (exp instanceof InExpression) {
                return evaluateInExpression(exp);
            } else if (exp instanceof IsNullExpression) {
                return evaluateIsNull(exp);
            } else if (exp instanceof CaseExpression) {
                return evaluateCaseWhen(exp);
            }
            throw new StatementExecutionException("unsupported operand " + exp.getClass() + ", expression is " + exp);
        }

        private Object evaluateCaseWhen(Expression exp) throws StatementExecutionException {
            CaseExpression caseExpression = (CaseExpression) exp;
            Expression switchExpression = caseExpression.getSwitchExpression();
            if (switchExpression != null) {
                throw new StatementExecutionException("unhandled expression CASE SWITCH, type " + exp.getClass() + ": " + exp);
            }
            if (caseExpression.getWhenClauses() != null) {
                for (Expression when : caseExpression.getWhenClauses()) {
                    WhenClause whenClause = (WhenClause) when;
                    Expression whenCondition = whenClause.getWhenExpression();
                    Object expressionValue = evaluateExpression(whenCondition);
                    if (expressionValue != null && Boolean.parseBoolean(expressionValue.toString())) {
                        return evaluateExpression(whenClause.getThenExpression());
                    }
                }
            }
            Expression elseExpression = caseExpression.getElseExpression();
            if (elseExpression != null) {
                return evaluateExpression(elseExpression);
            } else {
                return null;
            }
        }

        private Object evaluateIsNull(Expression exp) throws StatementExecutionException {
            IsNullExpression e = (IsNullExpression) exp;
            Object value = evaluateExpression(e.getLeftExpression());
            boolean result = value == null;
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateInExpression(Expression exp) throws StatementExecutionException {
            InExpression in = (InExpression) exp;
            if (in.getLeftItemsList() != null) {
                throw new StatementExecutionException("unsupported IN syntax <" + exp + ">");
            }
            boolean res = executeInClause(in, exp);
            if (in.isNot()) {
                return !res;
            } else {
                return res;
            }
        }

        private boolean executeInClause(InExpression in, Expression exp) throws StatementExecutionException {
            Object value = evaluateExpression(in.getLeftExpression());
            if (in.getRightItemsList() instanceof ExpressionList) {
                ExpressionList el = (ExpressionList) in.getRightItemsList();
                for (Expression e : el.getExpressions()) {
                    Object other = evaluateExpression(e);
                    if (objectEquals(value, other)) {
                        return true;
                    }
                }
                return false;
            } else if (in.getRightItemsList() instanceof SubSelect) {
                SubSelect ss = (SubSelect) in.getRightItemsList();
                SelectBody body = ss.getSelectBody();
                if (body instanceof PlainSelect) {
                    PlainSelect ps = (PlainSelect) body;
                    List<Tuple> subQueryResult = state.sqlContext.executeSubquery(ps);
                    for (Tuple t : subQueryResult) {
                        if (t.fieldNames.length > 1) {
                            throw new StatementExecutionException("subquery returned more than one column");
                        }
                        Object tuple_value = t.get(0);
                        if (objectEquals(value, tuple_value)) {
                            return true;
                        }
                    }
                    return false;
                }

            }
            throw new StatementExecutionException("unsupported operand " + exp.getClass() + " with argument of type " + in.getRightItemsList());
        }

        private Object evaluateTimeKeyExpression(Expression exp) throws StatementExecutionException {
            TimeKeyExpression ext = (TimeKeyExpression) exp;
            if (CURRENT_TIMESTAMP.equalsIgnoreCase(ext.getStringValue())) {
                return new java.sql.Timestamp(System.currentTimeMillis());
            } else {
                throw new StatementExecutionException("unhandled select expression " + exp);
            }
        }

        private Object evaluateSignedEpression(Expression exp) throws StatementExecutionException {
            SignedExpression s = (SignedExpression) exp;
            Object evaluated = evaluateExpression(s.getExpression());
            switch (s.getSign()) {
                case '-':
                    if (evaluated instanceof Integer) {
                        return ((Integer) evaluated) * -1;
                    }
                    if (evaluated instanceof Long) {
                        return ((Long) evaluated) * -1;
                    }
                    if (evaluated instanceof Double) {
                        return ((Double) evaluated) * -1;
                    }
                    if (evaluated instanceof Float) {
                        return ((Float) evaluated) * -1;
                    }
                    if (evaluated instanceof Short) {
                        return ((Short) evaluated) * -1;
                    }
                    if (evaluated instanceof Byte) {
                        return ((Byte) evaluated) * -1;
                    }
                    throw new StatementExecutionException("invalid signed expression, expression is " + exp);
                case '+':
                    return evaluated;
                default:
                    throw new StatementExecutionException("invalid sign '" + s.getSign() + "': expression is " + exp);
            }
        }

        private Object evaluateBetween(Expression exp) throws StatementExecutionException {
            Between e = (Between) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object start = evaluateExpression(e.getBetweenExpressionStart());
            Object end = evaluateExpression(e.getBetweenExpressionEnd());
            boolean result = (objectEquals(start, end) || compare(start, end) < 0) // check impossible range
                && (objectEquals(left, start)
                || objectEquals(left, end)
                || (compare(left, start) > 0 && compare(left, end) < 0)); // check value in range;
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateLike(Expression exp) throws StatementExecutionException {
            LikeExpression e = (LikeExpression) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());
            boolean result = like(left, right);
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateGreaterThanEquals(Expression exp) throws StatementExecutionException {
            GreaterThanEquals e = (GreaterThanEquals) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());
            boolean result = compare(left, right) >= 0;
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateGreaterThan(Expression exp) throws StatementExecutionException {
            GreaterThan e = (GreaterThan) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());
            boolean result = compare(left, right) > 0;
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateMinorThanEquals(Expression exp) throws StatementExecutionException {
            MinorThanEquals e = (MinorThanEquals) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());
            boolean result = compare(left, right) <= 0;
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateMinorThan(Expression exp) throws StatementExecutionException {
            MinorThan e = (MinorThan) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());

            boolean result = compare(left, right) < 0;
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private boolean evaluateEqualsTo(Expression exp) throws StatementExecutionException {
            BinaryExpression e = (BinaryExpression) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());
            boolean result = objectEquals(left, right);
            if (e.isNot()) {
                return !result;
            } else {
                return result;
            }
        }

        private Object evaluateParenthesis(Expression exp) throws StatementExecutionException {
            Parenthesis p = (Parenthesis) exp;
            Object inner = evaluateExpression(p.getExpression());
            if (!p.isNot()) {
                return inner;
            } else {
                return !toBoolean(inner);
            }
        }

        private Object evaluateOrExpression(Expression exp) throws StatementExecutionException {
            OrExpression a = (OrExpression) exp;
            if (toBoolean(evaluateExpression(a.getLeftExpression()))) {
                return !a.isNot();
            }
            if (toBoolean(evaluateExpression(a.getRightExpression()))) {
                return !a.isNot();
            }
            return a.isNot();
        }

        private Object evaluateAndExpression(Expression exp) throws StatementExecutionException {
            AndExpression a = (AndExpression) exp;
            if (!toBoolean(evaluateExpression(a.getLeftExpression()))) {
                return a.isNot();
            }
            if (!toBoolean(evaluateExpression(a.getRightExpression()))) {
                return a.isNot();
            }
            return !a.isNot();
        }

        private Object evaluateSubtraction(Expression exp) throws StatementExecutionException {
            Subtraction e = (Subtraction) exp;
            Object left = evaluateExpression(e.getLeftExpression());
            Object right = evaluateExpression(e.getRightExpression());
            return subtract(left, right);
        }

        private Object evaluateAddition(Expression exp) throws StatementExecutionException {
            Addition add = (Addition) exp;
            Object left = evaluateExpression(add.getLeftExpression());
            Object right = evaluateExpression(add.getRightExpression());
            return SQLRecordPredicate.add(left, right);
        }

        private Object evaluateColumn(Expression exp) throws StatementExecutionException {
            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
            if (validatedTableAlias != null) {
                if (c.getTable() != null && c.getTable().getName() != null
                    && !c.getTable().getName().equals(validatedTableAlias)) {
                    throw new StatementExecutionException("invalid column name " + c.getColumnName()
                        + " invalid table name " + c.getTable().getName() + ", expecting " + validatedTableAlias);
                }
            }
            String columnName = c.getColumnName();
            switch (columnName) {
                case BuiltinFunctions.BOOLEAN_TRUE:
                    return Boolean.TRUE;
                case BuiltinFunctions.BOOLEAN_FALSE:
                    return Boolean.FALSE;
                default:
                    return record.get(columnName);
            }
        }

        Object computeFunction(Function f) throws StatementExecutionException {
            String name = f.getName();
            switch (name) {
                case BuiltinFunctions.COUNT:
                case BuiltinFunctions.SUM:
                case BuiltinFunctions.MIN:
                case BuiltinFunctions.MAX:
                    // AGGREGATED FUNCTION
                    return null;
                case BuiltinFunctions.LOWER: {
                    Object computed = evaluateExpression(f.getParameters().getExpressions().get(0));
                    if (computed == null) {
                        return null;
                    }
                    return computed.toString().toLowerCase();
                }
                case BuiltinFunctions.UPPER: {
                    Object computed = evaluateExpression(f.getParameters().getExpressions().get(0));
                    if (computed == null) {
                        return null;
                    }
                    return computed.toString().toUpperCase();
                }
                default:
                    throw new StatementExecutionException("unhandled function " + name);
            }
        }
    }

    public static Object evaluateFreeColumn(Expression exp, String validatedTableAlias, Map<String, Object> record) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
        if (validatedTableAlias != null) {
            if (c.getTable() != null && c.getTable().getName() != null
                && !c.getTable().getName().equals(validatedTableAlias)) {
                throw new StatementExecutionException("invalid column name " + c.getColumnName()
                    + " invalid table name " + c.getTable().getName() + ", expecting " + validatedTableAlias);
            }
        }
        String columnName = c.getColumnName();
        switch (columnName) {
            case BuiltinFunctions.BOOLEAN_TRUE:
                return Boolean.TRUE;
            case BuiltinFunctions.BOOLEAN_FALSE:
                return Boolean.FALSE;
            default:
                return record.get(columnName);
        }
    }

}
