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
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.Tuple;
import herddb.model.TuplePredicate;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.RawString;
import static herddb.sql.functions.BuiltinFunctions.CURRENT_TIMESTAMP;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
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
 * Predicate expressed using SQL syntax
 *
 * @author enrico.olivelli
 */
public class SQLRecordPredicate extends Predicate implements TuplePredicate {

    private static final Logger LOGGER = Logger.getLogger(SQLRecordPredicate.class.getName());

    static boolean isConstant(Expression exp) {
        return exp instanceof StringValue
            || exp instanceof LongValue
            || exp instanceof NullValue
            || exp instanceof TimestampValue
            || exp instanceof JdbcParameter;
    }

    private final Table table;
    private final String validatedTableAlias;
    private final Expression where;
    private Expression primaryKeyFilter;

    private static final class EvaluationState {

        final List<Object> parameters;
        final SQLStatementEvaluationContext sqlContext;

        public EvaluationState(List<Object> parameters, SQLStatementEvaluationContext sqlContext) {
            this.parameters = parameters;
            this.sqlContext = sqlContext;
        }
    }

    public SQLRecordPredicate(Table table, String tableAlias, Expression where) {
        this.table = table;
        this.where = where;
        this.validatedTableAlias = tableAlias;
    }

    @Override
    public boolean matchesRawPrimaryKey(Bytes key, StatementEvaluationContext context) throws StatementExecutionException {
        if (primaryKeyFilter == null) {
            return true;
        }
        Map<String, Object> bean = RecordSerializer.deserializePrimaryKeyAsMap(key.data, table);
        return evaluatePredicate(primaryKeyFilter, bean, context, validatedTableAlias);
    }

    @Override
    public boolean matches(Tuple a, StatementEvaluationContext context) throws StatementExecutionException {
        Map<String, Object> bean = a.toMap();
        return evaluatePredicate(where, bean, context, validatedTableAlias);
    }

    @Override
    public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
        Map<String, Object> bean = record.toBean(table);
        return evaluatePredicate(where, bean, context, validatedTableAlias);
    }

    private static boolean evaluatePredicate(Expression where, Map<String, Object> bean, StatementEvaluationContext context, String validatedTableAlias) throws StatementExecutionException {
        return toBoolean(evaluateExpression(where, bean, context, validatedTableAlias));
    }

    public static Object evaluateExpression(Expression where, Map<String, Object> bean, StatementEvaluationContext context, String validatedTableAlias) throws StatementExecutionException {
        SQLStatementEvaluationContext sqlContext = (SQLStatementEvaluationContext) context;
        EvaluationState state = new EvaluationState(sqlContext.jdbcParameters, sqlContext);
        ExpressionEvaluator evaluator = new ExpressionEvaluator(bean, state, validatedTableAlias);
        return evaluator.evaluateExpression(where);
    }

    public static boolean toBoolean(Object result) {
        if (result == null) {
            return false;
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        return "true".equals(result.toString());
    }

    public static boolean minorThan(Object a, Object b) throws StatementExecutionException {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b) < 0;
        }
        if (a instanceof RawString && b instanceof String) {
            return a.toString().compareTo((String) b) < 0;
        }
        if (a instanceof String && b instanceof RawString) {
            return ((String) a).compareTo(b.toString()) < 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() < ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() < ((java.util.Date) b).getTime();
        }
        throw new StatementExecutionException("uncompable objects " + a.getClass() + " vs " + b.getClass());
    }

    public static boolean minorThanEquals(Object a, Object b) throws StatementExecutionException {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b) <= 0;
        }
        if (a instanceof RawString && b instanceof String) {
            return a.toString().compareTo((String) b) <= 0;
        }
        if (a instanceof String && b instanceof RawString) {
            return ((String) a).compareTo(b.toString()) <= 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() <= ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() <= ((java.util.Date) b).getTime();
        }
        if (Objects.equals(a, b)) {
            return true;
        }
        throw new StatementExecutionException("uncompable objects " + a.getClass() + " vs " + b.getClass());
    }

    public static boolean greaterThan(Object a, Object b) throws StatementExecutionException {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b) > 0;
        }
        if (a instanceof RawString && b instanceof String) {
            return a.toString().compareTo((String) b) > 0;
        }
        if (a instanceof String && b instanceof RawString) {
            return ((String) a).compareTo(b.toString()) > 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() > ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() > ((java.util.Date) b).getTime();
        }
        throw new StatementExecutionException("uncompable objects " + a.getClass() + " vs " + b.getClass());
    }

    public static boolean greaterThanEquals(Object a, Object b) throws StatementExecutionException {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b) >= 0;
        }
        if (a instanceof RawString && b instanceof String) {
            return a.toString().compareTo((String) b) >= 0;
        }
        if (a instanceof String && b instanceof RawString) {
            return ((String) a).compareTo(b.toString()) >= 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() >= ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() >= ((java.util.Date) b).getTime();
        }
        if (Objects.equals(a, b)) {
            return true;
        }
        throw new StatementExecutionException("uncompable objects " + a.getClass() + " vs " + b.getClass());
    }

    public static Object add(Object a, Object b) throws StatementExecutionException {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).longValue() + ((Number) b).longValue();
        }
        throw new StatementExecutionException("cannot add " + a + " and " + b);
    }

    public static Object subtract(Object a, Object b) throws StatementExecutionException {
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).longValue() - ((Number) b).longValue();
        }
        throw new StatementExecutionException("cannot subtract " + a + " and " + b);
    }

    public static boolean objectEquals(Object a, Object b) {
        if (Objects.equals(a, b)) {
            return true;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() == ((java.util.Date) b).getTime();
        }
        if (a instanceof java.lang.Boolean
            && b != null
            && (Boolean.parseBoolean(b.toString()) == ((Boolean) a))) {
            return true;
        }
        if (b instanceof java.lang.Boolean
            && a != null
            && (Boolean.parseBoolean(a.toString()) == ((Boolean) b))) {
            return true;
        }
        return false;
    }

    public static boolean like(Object a, Object b) {
        if (a == null || b == null) {
            return false;
        }
        String like = b.toString()
            .replace(".", "\\.")
            .replace("\\*", "\\*")
            .replace("%", ".*")
            .replace("_", ".?");
        return a.toString().matches(like);
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
                return ((StringValue) exp).getValue();
            } else if (exp instanceof LongValue) {
                return ((LongValue) exp).getValue();
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
            boolean result = (objectEquals(start, end) || minorThan(start, end)) // check impossible range
                && (objectEquals(left, start)
                || objectEquals(left, end)
                || (greaterThan(left, start) && minorThan(left, end))); // check value in range;
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
            boolean result = greaterThanEquals(left, right);
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
            boolean result = greaterThan(left, right);
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
            boolean result = minorThanEquals(left, right);
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

            boolean result = minorThan(left, right);
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

    @Override
    public String toString() {
        if (table != null) {
            return "SQLRecordPredicate{" + "table=" + table.name + ", tableAlias=" + validatedTableAlias + ", where=" + where + ", indexOp=" + getIndexOperation() + '}';
        } else {
            return "SQLRecordPredicate{" + "table=null" + ", tableAlias=" + validatedTableAlias + ", where=" + where + ", indexOp=" + getIndexOperation() + '}';
        }
    }

    public Expression getPrimaryKeyFilter() {
        return primaryKeyFilter;
    }

    public void setPrimaryKeyFilter(Expression primaryKeyFilter) {
        this.primaryKeyFilter = primaryKeyFilter;
    }

}
