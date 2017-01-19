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
    public static final String MIN = "min";
    public static final String MAX = "max";
    // scalar
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";
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
        String fuctionName = f.getName().toLowerCase();
        switch (fuctionName) {
            case COUNT:
                return new CountColumnCalculator(fieldName);
            case SUM:
                return new SumColumnCalculator(fieldName, f.getParameters().getExpressions().get(0), context);
            case MIN:
                return new MinColumnCalculator(fieldName, f.getParameters().getExpressions().get(0), context);
            case MAX:
                return new MaxColumnCalculator(fieldName, f.getParameters().getExpressions().get(0), context);
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

    public static int typeOfFunction(String lowerCaseName) throws StatementExecutionException {
        switch (lowerCaseName) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
            case BuiltinFunctions.MAX:
                return ColumnTypes.LONG;

            case BuiltinFunctions.LOWER:
            case BuiltinFunctions.UPPER:
                return ColumnTypes.STRING;
            default:
                throw new StatementExecutionException("unhandled function " + lowerCaseName);

        }
    }

}
