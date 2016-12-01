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
package herddb.sql.functions;

import java.util.List;
import java.util.Map;

import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.StatementExecutionException;
import herddb.sql.AggregatedColumnCalculator;
import herddb.sql.SQLRecordPredicate;
import herddb.utils.IntHolder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;

/**
 * SQL aggregate functions
 *
 * @author enrico.olivelli
 */
public class BuiltinFunctions {

    // aggregate
    public static final String COUNT = "count";
    public static final String SUM = "sum";
    public static final String MIN = "min";
    public static final String MAX = "max";
    // scalar
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";
    // special
    public static final String CURRENT_TIMESTAMP = "current_timestamp";

    public static Column toAggregatedOutputColumn(String fieldName, Function f) {
        if (f.getName().equalsIgnoreCase(BuiltinFunctions.COUNT)) {
            return Column.column(fieldName, ColumnTypes.LONG);
        }
        if (f.getName().equalsIgnoreCase(BuiltinFunctions.SUM) && f.getParameters() != null && f.getParameters().getExpressions() != null && f.getParameters().getExpressions().size() == 1) {
            return Column.column(fieldName, ColumnTypes.LONG);
        }
        if (f.getName().equalsIgnoreCase(BuiltinFunctions.MIN) && f.getParameters() != null && f.getParameters().getExpressions() != null && f.getParameters().getExpressions().size() == 1) {
            return Column.column(fieldName, ColumnTypes.LONG);
        }
        if (f.getName().equalsIgnoreCase(BuiltinFunctions.MAX) && f.getParameters() != null && f.getParameters().getExpressions() != null && f.getParameters().getExpressions().size() == 1) {
            return Column.column(fieldName, ColumnTypes.LONG);
        }
        return null;
    }

    public static AggregatedColumnCalculator getColumnCalculator(Function f, String fieldName) throws StatementExecutionException {
        String fuctionName = f.getName().toLowerCase();
        switch (fuctionName) {
            case COUNT:
                return new CountColumnCalculator(fieldName);
            case SUM:
                return new SumColumnCalculator(fieldName, f.getParameters().getExpressions().get(0));
            case MIN:
                return new MinColumnCalculator(fieldName, f.getParameters().getExpressions().get(0));
            case MAX:
                return new MaxColumnCalculator(fieldName, f.getParameters().getExpressions().get(0));
            default:
                return null;
        }
    }

    public static boolean isScalarFunction(String name) {
        switch (name.toLowerCase()) {
            case LOWER:
            case UPPER:
                return true;
            default:
                return false;
        }
    }

    public static boolean isAggregateFunction(String name) {
        name = name.toLowerCase();
        switch (name) {
            case COUNT:
            case SUM:
            case MIN:
            case MAX:
                return true;
            default:
                return false;
        }
    }

    public static int typeOfFunction(String name) throws StatementExecutionException {
        switch (name.toLowerCase()) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
            case BuiltinFunctions.MAX:
                return ColumnTypes.LONG;

            case BuiltinFunctions.LOWER:
            case BuiltinFunctions.UPPER:
                return ColumnTypes.STRING;
            default:
                throw new StatementExecutionException("unhandled function " + name);

        }
    }

    public static Object computeValue(Expression exp, Map<String, Object> record) throws StatementExecutionException {
        return computeValue(exp, record, null);
    }

    public static Object computeValue(Expression exp, Map<String, Object> record, List<Object> jdbcParameters) throws StatementExecutionException {
        Object value;
        if (exp instanceof net.sf.jsqlparser.schema.Column) {
            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
            value = record.get(c.getColumnName().toLowerCase());
        } else if (exp instanceof StringValue) {
            value = ((StringValue) exp).getValue();
        } else if (exp instanceof LongValue) {
            value = ((LongValue) exp).getValue();
        } else if (exp instanceof TimestampValue) {
            value = ((TimestampValue) exp).getValue();
        } else if (exp instanceof Function) {
            Function f = (Function) exp;
            value = computeFunction(f, record, jdbcParameters);
        } else if (exp instanceof Addition) {
            Addition add = (Addition) exp;
            Object left = computeValue(add.getLeftExpression(), record, jdbcParameters);
            Object right = computeValue(add.getRightExpression(), record, jdbcParameters);
            return SQLRecordPredicate.add(left, right);
        } else if (exp instanceof Subtraction) {
            Subtraction add = (Subtraction) exp;
            Object left = computeValue(add.getLeftExpression(), record, jdbcParameters);
            Object right = computeValue(add.getRightExpression(), record, jdbcParameters);
            return SQLRecordPredicate.subtract(left, right);
        } else if (exp instanceof TimeKeyExpression) {
            TimeKeyExpression ext = (TimeKeyExpression) exp;
            if (CURRENT_TIMESTAMP.equalsIgnoreCase(ext.getStringValue())) {
                return new java.sql.Timestamp(System.currentTimeMillis());
            } else {
                throw new StatementExecutionException("unhandled expression " + exp);
            }
        } else if (exp instanceof JdbcParameter) {
            if (jdbcParameters == null) {
                throw new StatementExecutionException("jdbcparameter expression without parameters");
            }
            int index = ((JdbcParameter) exp).getIndex();
            if (jdbcParameters.size() < index) {
                throw new StatementExecutionException("jdbcparameter wrong argument count: expected at least "
                    + index + " got " + jdbcParameters.size());
            }
            return jdbcParameters.get(index);
        } else {
            throw new StatementExecutionException("unhandled expression type " + exp.getClass() + ": " + exp);
        }
        return value;
    }

    public static Object computeFunction(Function f, Map<String, Object> record, List<Object> jdbcParameters) throws StatementExecutionException {
        String name = f.getName().toLowerCase();
        switch (name) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
            case BuiltinFunctions.MAX:
                // AGGREGATED FUNCTION
                return null;
            case BuiltinFunctions.LOWER: {
                Object computed = computeValue(f.getParameters().getExpressions().get(0), record, jdbcParameters);
                if (computed == null) {
                    return null;
                }
                return computed.toString().toLowerCase();
            }
            case BuiltinFunctions.UPPER: {
                Object computed = computeValue(f.getParameters().getExpressions().get(0), record, jdbcParameters);
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
