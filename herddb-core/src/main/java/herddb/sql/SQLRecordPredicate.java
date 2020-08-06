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
import herddb.model.ColumnTypes;
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.Tuple;
import herddb.model.TuplePredicate;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.ConstantExpression;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.logging.Logger;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;

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

    static boolean isConstant(CompiledSQLExpression exp) {
        return exp instanceof ConstantExpression;
    }

    private final Table table;
    private final String validatedTableAlias;
    private final CompiledSQLExpression where;
    private CompiledSQLExpression primaryKeyFilter;

    public SQLRecordPredicate(Table table, String tableAlias, CompiledSQLExpression where) {
        this.table = table;
        this.validatedTableAlias = tableAlias;
        this.where = where;
    }

    @Override
    public PrimaryKeyMatchOutcome matchesRawPrimaryKey(Bytes key, StatementEvaluationContext context) throws StatementExecutionException {
        if (primaryKeyFilter == null) {
            return PrimaryKeyMatchOutcome.NEED_FULL_RECORD_EVALUATION;
        }
        DataAccessor bean = RecordSerializer.buildRawDataAccessorForPrimaryKey(key, table);

        boolean result = SQLRecordPredicateFunctions.toBoolean(primaryKeyFilter.evaluate(bean, context));

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
        return SQLRecordPredicateFunctions.toBoolean(where.evaluate(a, context));
    }

    @Override
    public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
        DataAccessor bean = record.getDataAccessor(table);
        return SQLRecordPredicateFunctions.toBoolean(where.evaluate(bean, context));
    }

    @Override
    public void validateContext(StatementEvaluationContext context) throws StatementExecutionException {
        where.validate(context);
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

    public void setPrimaryKeyFilter(CompiledSQLExpression primaryKeyFilter) {
        this.primaryKeyFilter = primaryKeyFilter;
    }

    public CompiledSQLExpression getWhere() {
        return where;
    }

    public static Object cast(Object value, int type) {
        if (value == null) {
            return null;
        }
        try {
            switch (type) {
                case ColumnTypes.BOOLEAN:
                case ColumnTypes.NOTNULL_BOOLEAN: {
                    if (value instanceof Boolean) {
                        return ((Boolean) value);
                    }
                    if (value instanceof Number) {
                        int asInt = ((Number) value).intValue();
                        switch (asInt) {
                            case 1:
                                return Boolean.TRUE;
                            case 0:
                                return Boolean.FALSE;
                            default:
                                throw new IllegalArgumentException("Numeric value " + value + " cannot be converted to a Boolean");
                        }
                    }
                    String s = value.toString();
                    switch (s) {
                        case "true":
                        case "1":
                            return Boolean.TRUE;
                        case "0":
                        case "false":
                            return Boolean.FALSE;
                        default:
                            throw new IllegalArgumentException("Value " + value + " cannot be converted to a Boolean");
                    }
                }
                case ColumnTypes.STRING:
                case ColumnTypes.NOTNULL_STRING:
                    if (value instanceof RawString
                            || value instanceof String) {
                        return value;
                    }
                    if (value instanceof Number
                            || value instanceof Boolean) {
                        return value.toString();
                    }
                    throw new IllegalArgumentException("Value " + value + " (" + value.getClass() + ") cannot be converted to a Boolean");
                case ColumnTypes.INTEGER:
                case ColumnTypes.NOTNULL_INTEGER: {
                    if (value instanceof Boolean) {
                        return ((Boolean) value) ? 1 : 0;
                    }
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    if (value instanceof RawString) {
                        return Integer.parseInt(((RawString) value).toString());
                    }
                    if (value instanceof String) {
                        return Integer.parseInt(((String) value));
                    }
                    throw new IllegalArgumentException("type " + value.getClass() + " (value  " + value + ") cannot be cast to INTEGER");
                }
                case ColumnTypes.LONG:
                case ColumnTypes.NOTNULL_LONG: {
                    if (value instanceof Boolean) {
                        return ((Boolean) value) ? 1L : 0L;
                    }

                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    if (value instanceof RawString) {
                        return Long.parseLong(((RawString) value).toString());
                    }
                    if (value instanceof String) {
                        return Long.parseLong(((String) value));
                    }
                    throw new IllegalArgumentException("type " + value.getClass() + " (value  " + value + ") cannot be cast to LONG");
                }
                case ColumnTypes.DOUBLE:
                case ColumnTypes.NOTNULL_DOUBLE: {
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    if (value instanceof RawString) {
                        return Double.parseDouble(((RawString) value).toString());
                    }
                    if (value instanceof String) {
                        return Double.parseDouble(((String) value));
                    }
                    if (value instanceof Boolean) {
                        return ((Boolean) value) ? 1d : 0d;
                    }
                    throw new IllegalArgumentException("type " + value.getClass() + " (value  " + value + ") cannot be cast to DOUBLE");
                }
                case ColumnTypes.TIMESTAMP:
                case ColumnTypes.NOTNULL_TIMESTAMP: {
                    if (value instanceof java.sql.Timestamp) {
                        return (java.sql.Timestamp) value;
                    }
                    if (value instanceof Number) {
                        return new java.sql.Timestamp(((Number) value).longValue());
                    }
                    if (value instanceof RawString) {
                        return java.sql.Timestamp.valueOf(((RawString) value).toString());
                    }
                    if (value instanceof String) {
                        return java.sql.Timestamp.valueOf(((String) value));
                    }
                    throw new IllegalArgumentException("type " + value.getClass() + " (value  " + value + ") cannot be cast to TIMESTAMP");
                }
                default:
                    return value;
            }
        } catch (ClassCastException err) {
            throw new IllegalArgumentException("Unexpected error on cast of value " + value + " (" + value.getClass() + "): " + err, err);
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.equals(CompiledSQLExpression.class)) {
            return (T) where;
        }
        return super.unwrap(clazz);
    }

    @Override
    public int estimateObjectSizeForCache() {
        return where.estimateObjectSizeForCache();
    }

}
