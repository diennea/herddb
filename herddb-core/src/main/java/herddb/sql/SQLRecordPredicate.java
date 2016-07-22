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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
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
public class SQLRecordPredicate extends Predicate {

    private static final Logger LOGGER = Logger.getLogger(SQLRecordPredicate.class.getName());

    private final Table table;
    private final String tableAlias;
    private final Expression where;
    private final int firstParameterPos;

    private class EvaluationState {

        int parameterPos;
        final List<Object> parameters;
        final SQLStatementEvaluationContext sqlContext;

        public EvaluationState(int parameterPos, List<Object> parameters, SQLStatementEvaluationContext sqlContext) {
            this.parameterPos = parameterPos;
            this.parameters = parameters;
            this.sqlContext = sqlContext;
        }
    }

    public SQLRecordPredicate(Table table, String tableAlias, Expression where, int parameterPos) {
        this.table = table;
        this.where = where;
        this.firstParameterPos = parameterPos;
        this.tableAlias = tableAlias;
    }

    @Override
    public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
        SQLStatementEvaluationContext sqlContext = (SQLStatementEvaluationContext) context;
        Map<String, Object> bean = RecordSerializer.toBean(record, table);
        EvaluationState state = new EvaluationState(firstParameterPos, sqlContext.jdbcParameters, sqlContext);
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

    private boolean minorThan(Object a, Object b) throws StatementExecutionException {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b) < 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() < ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() < ((java.util.Date) b).getTime();
        }
        throw new StatementExecutionException("uncompable objects " + a.getClass() + " vs " + b.getClass());
    }

    private boolean greaterThan(Object a, Object b) throws StatementExecutionException {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b) > 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() > ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() > ((java.util.Date) b).getTime();
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
        throw new StatementExecutionException("cannot add " + a + " and " + b);
    }

    private boolean objectEquals(Object a, Object b) {
        if (Objects.equals(a, b)) {
            return true;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() == ((java.util.Date) b).getTime();
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

    private Object evaluateExpression(Expression expression, Map<String, Object> bean, EvaluationState state) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            return state.parameters.get(state.parameterPos++);
        }
        if (expression instanceof AndExpression) {
            AndExpression a = (AndExpression) expression;

            if (!toBoolean(evaluateExpression(a.getLeftExpression(), bean, state))) {
                return a.isNot();
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
            if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equalsIgnoreCase(tableAlias)) {
                throw new StatementExecutionException("invalid column name " + c.getColumnName() + " invalid table name " + c.getTable().getName() + ", expecting " + tableAlias);
            }
            return bean.get(c.getColumnName().toLowerCase());
        }
        if (expression instanceof Parenthesis) {
            Parenthesis p = (Parenthesis) expression;
            return handleNot(p.isNot(), evaluateExpression(p.getExpression(), bean, state));
        }
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        }
        if (expression instanceof EqualsTo) {
            EqualsTo e = (EqualsTo) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), objectEquals(left, right));
        }
        if (expression instanceof NotEqualsTo) {
            NotEqualsTo e = (NotEqualsTo) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), !objectEquals(left, right));
        }
        if (expression instanceof MinorThan) {
            MinorThan e = (MinorThan) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), minorThan(left, right));
        }
        if (expression instanceof MinorThanEquals) {
            MinorThanEquals e = (MinorThanEquals) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), objectEquals(left, right) || minorThan(left, right));
        }
        if (expression instanceof GreaterThan) {
            GreaterThan e = (GreaterThan) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), greaterThan(left, right));
        }
        if (expression instanceof GreaterThanEquals) {
            GreaterThanEquals e = (GreaterThanEquals) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), objectEquals(left, right) || greaterThan(left, right));
        }
        if (expression instanceof LikeExpression) {
            LikeExpression e = (LikeExpression) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return handleNot(e.isNot(), like(left, right));
        }
        if (expression instanceof Between) {
            Between e = (Between) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object start = evaluateExpression(e.getBetweenExpressionStart(), bean, state);
            Object end = evaluateExpression(e.getBetweenExpressionEnd(), bean, state);
            return handleNot(e.isNot(),
                    (objectEquals(start, end) || minorThan(start, end)) // check impossible range
                    && (objectEquals(left, start)
                    || objectEquals(left, end)
                    || (greaterThan(left, start) && minorThan(left, end))) // check value in range
            );
        }

        if (expression instanceof SignedExpression) {
            SignedExpression s = (SignedExpression) expression;
            Object evaluated = evaluateExpression(s.getExpression(), bean, state);
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
                    throw new StatementExecutionException("invalid signed expression, expression is " + expression);
                case '+':
                    return evaluated;
                default:
                    throw new StatementExecutionException("invalid sign '" + s.getSign() + "': expression is " + expression);
            }
        }
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }
        if (expression instanceof TimestampValue) {
            return ((TimestampValue) expression).getValue();
        }
        if (expression instanceof InExpression) {
            InExpression in = (InExpression) expression;
            if (in.getLeftItemsList() != null) {
                throw new StatementExecutionException("unsupported IN syntax <" + expression + ">");
            }
            Object value = evaluateExpression(in.getLeftExpression(), bean, state);
            if (in.getRightItemsList() instanceof ExpressionList) {
                ExpressionList el = (ExpressionList) in.getRightItemsList();
                for (Expression e : el.getExpressions()) {
                    Object other = evaluateExpression(e, bean, state);
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
            throw new StatementExecutionException("unsupported operand " + expression.getClass() + " with argument of type " + in.getRightItemsList());
        }
        if (expression instanceof IsNullExpression) {
            IsNullExpression e = (IsNullExpression) expression;
            Object value = evaluateExpression(e.getLeftExpression(), bean, state);
            boolean result = value == null;
            return handleNot(e.isNot(), result);
        }
        if (expression instanceof Addition) {
            Addition e = (Addition) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return add(left, right);
        }
        if (expression instanceof Subtraction) {
            Subtraction e = (Subtraction) expression;
            Object left = evaluateExpression(e.getLeftExpression(), bean, state);
            Object right = evaluateExpression(e.getRightExpression(), bean, state);
            return subtract(left, right);
        }
        throw new StatementExecutionException("unsupported operand " + expression.getClass() + ", expression is " + expression);
    }

    @Override
    public String toString() {
        return "SQLRecordPredicate{" + "table=" + table + ", tableAlias=" + tableAlias + ", where=" + where + ", firstParameterPos=" + firstParameterPos + '}';
    }

}
