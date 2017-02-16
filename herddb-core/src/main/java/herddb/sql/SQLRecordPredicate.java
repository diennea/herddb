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
import herddb.utils.RawString;
import herddb.utils.Bytes;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import herddb.sql.expressions.SQLExpressionCompiler;
import herddb.sql.expressions.CompiledSQLExpression;

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
    private final CompiledSQLExpression where;
    private CompiledSQLExpression primaryKeyFilter;

    public SQLRecordPredicate(Table table, String tableAlias, Expression where) {
        this.table = table;
        this.validatedTableAlias = tableAlias;
        this.where = SQLExpressionCompiler.compileExpression(validatedTableAlias, where);
    }

    @Override
    public PrimaryKeyMatchOutcome matchesRawPrimaryKey(Bytes key, StatementEvaluationContext context) throws StatementExecutionException {
        if (primaryKeyFilter == null) {
            return PrimaryKeyMatchOutcome.NEED_FULL_RECORD_EVALUATION;
        }
        Map<String, Object> bean = RecordSerializer.deserializePrimaryKeyAsMap(key, table);

        boolean result = toBoolean(primaryKeyFilter.evaluate(bean, context));

        if (!result) {
            return PrimaryKeyMatchOutcome.FAILED;
        } else {
            return where == primaryKeyFilter
                ? PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED
                : PrimaryKeyMatchOutcome.NEED_FULL_RECORD_EVALUATION;
        }
    }

    @Override
    public boolean matches(Tuple a, StatementEvaluationContext context) throws StatementExecutionException {
        Map<String, Object> bean = a.toMap();
        return toBoolean(where.evaluate(bean, context));
    }

    @Override
    public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
        Map<String, Object> bean = record.toBean(table);
        return toBoolean(where.evaluate(bean, context));
    }

    public static Object evaluateExpression(Expression expression, Map<String, Object> bean, StatementEvaluationContext context, String validatedTableAlias) throws StatementExecutionException {
        return SQLExpressionCompiler.compileExpression(validatedTableAlias, expression).evaluate(bean, context);

    }

    public static boolean toBoolean(Object result) {
        if (result == null) {
            return false;
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        return "true".equalsIgnoreCase(result.toString());
    }

    public static int compare(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null && b != null) {
            return 1;
        }
        if (b == null && a != null) {
            return -1;
        }
        if (a instanceof RawString && b instanceof RawString) {
            return ((RawString) a).compareTo((RawString) b);
        }
        if (a instanceof RawString && b instanceof String) {
            return ((RawString) a).compareToString((String) b);
        }
        if (a instanceof String && b instanceof RawString) {
            return -((RawString) b).compareToString((String) a);
        }
        if (a instanceof Integer && b instanceof Integer) {
            return ((Number) a).intValue() - ((Number) b).intValue();
        }
        if (a instanceof Long && b instanceof Long) {
            double delta = ((Number) a).longValue() - ((Number) b).longValue();
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof Number && b instanceof Number) {
            double delta = ((Number) a).doubleValue() - ((Number) b).doubleValue();
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            long delta = ((java.util.Date) a).getTime() - ((java.util.Date) b).getTime();
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b);
        }
        throw new IllegalArgumentException("uncompable objects " + a.getClass() + " vs " + b.getClass());
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
            return ((Number) a).longValue() - ((Number) b).longValue();
        }
        throw new StatementExecutionException("cannot subtract " + a + " and " + b);
    }

    public static boolean objectEquals(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof RawString) {
            return a.equals(b);
        }
        if (b instanceof RawString) {
            return b.equals(a);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() == ((java.util.Date) b).getTime();
        }
        if (a instanceof java.lang.Boolean
            && (Boolean.parseBoolean(b.toString()) == ((Boolean) a))) {
            return true;
        }
        if (b instanceof java.lang.Boolean
            && (Boolean.parseBoolean(a.toString()) == ((Boolean) b))) {
            return true;
        }
        return Objects.equals(a, b);
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

    @Override
    public String toString() {
        if (table != null) {
            return "SQLRecordPredicate{" + "table=" + table.name + ", tableAlias=" + validatedTableAlias + ", where=" + where + ", indexOp=" + getIndexOperation() + " " + primaryKeyFilter + '}';
        } else {
            return "SQLRecordPredicate{" + "table=null" + ", tableAlias=" + validatedTableAlias + ", where=" + where + ", indexOp=" + getIndexOperation() + " " + primaryKeyFilter + '}';
        }
    }

    public CompiledSQLExpression getPrimaryKeyFilter() {
        return primaryKeyFilter;
    }

    public void setPrimaryKeyFilter(Expression primaryKeyFilter) {
        if (primaryKeyFilter != null) {
            this.primaryKeyFilter = SQLExpressionCompiler.compileExpression(validatedTableAlias, primaryKeyFilter);
        } else {
            this.primaryKeyFilter = null;
        }
    }

    public CompiledSQLExpression getWhere() {
        return where;
    }

}
