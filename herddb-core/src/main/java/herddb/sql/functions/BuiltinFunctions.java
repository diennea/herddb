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

import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.AggregatedColumnCalculator;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.SQLExpressionCompiler;
import net.sf.jsqlparser.expression.Function;

/**
 * SQL aggregate functions
 *
 * @author enrico.olivelli
 */
public class BuiltinFunctions {

    // aggregate
    public static final String COUNT = "count";
    public static final String SUM = "sum";
    public static final String SUM0 = "$sum0";//Calcite Sum empty is zero
    public static final String MIN = "min";
    public static final String MAX = "max";
    // scalar
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";
    public static final String ABS = "abs";
    public static final String ROUND = "round";
    // special
    public static final String CURRENT_TIMESTAMP = "current_timestamp";
    public static final String BOOLEAN_TRUE = "true";
    public static final String BOOLEAN_FALSE = "false";

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

    public static AggregatedColumnCalculator getColumnCalculator(Function f, String fieldName,
            StatementEvaluationContext context) throws StatementExecutionException {
        String functionName = f.getName();
        CompiledSQLExpression firstParam = f.getParameters().getExpressions().isEmpty() ? null
                : SQLExpressionCompiler.compileExpression(null, f.getParameters().getExpressions().get(0));
        return getColumnCalculator(functionName, fieldName, firstParam, context);
    }

    public static AggregatedColumnCalculator getColumnCalculator(String functionName, String fieldName,
            CompiledSQLExpression firstParam, StatementEvaluationContext context) throws StatementExecutionException {
        switch (functionName) {
            case COUNT:
                return new CountColumnCalculator(fieldName);
            case SUM:
            case SUM0:
                return new SumColumnCalculator(fieldName, firstParam, context);
            case MIN:
                return new MinColumnCalculator(fieldName, firstParam, context);
            case MAX:
                return new MaxColumnCalculator(fieldName, firstParam, context);
            default:
                return null;
        }
    }

    public static boolean isScalarFunction(String name) {
        switch (name) {
            case LOWER:
            case UPPER:
            case ABS:
            case ROUND:
                return true;
            default:
                return false;
        }
    }

    public static boolean isAggregateFunction(String name) {
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

    public static int typeOfFunction(String lowerCaseName) throws StatementExecutionException {
        switch (lowerCaseName) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
            case BuiltinFunctions.MAX:
            case BuiltinFunctions.ABS:
            case BuiltinFunctions.ROUND:
                return ColumnTypes.LONG;

            case BuiltinFunctions.LOWER:
            case BuiltinFunctions.UPPER:
                return ColumnTypes.STRING;
            default:
                throw new StatementExecutionException("unhandled function " + lowerCaseName);

        }
    }

}
