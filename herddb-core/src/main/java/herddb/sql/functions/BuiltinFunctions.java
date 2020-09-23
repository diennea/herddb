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

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.AggregatedColumnCalculator;
import herddb.sql.expressions.CompiledSQLExpression;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;

/**
 * SQL aggregate functions
 *
 * @author enrico.olivelli
 */
public class BuiltinFunctions {

    // aggregate
    public static final String COUNT = "count";
    public static final String SUM = "sum";
    /**
     * @see SqlSumEmptyIsZeroAggFunction
     */
    public static final String SUM0 = "$sum0"; //Calcite Sum empty is zero
    public static final String MIN = "min";
    public static final String MAX = "max";
    /**
     * @see SqlSingleValueAggFunction
     */
    public static final String SINGLEVALUE = "single_value";
    // scalar
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";
    public static final String ABS = "abs";
    public static final String ROUND = "round";
    public static final String EXTRACT = "extract";
    public static final String FLOOR = "floor";
    public static final String RAND = "rand";

    // special
    public static final String CURRENT_TIMESTAMP = "current_timestamp";
    public static final String BOOLEAN_TRUE = "true";
    public static final String BOOLEAN_FALSE = "false";

    // aggregate
    public static final String NAME_COUNT = "COUNT";
    public static final String NAME_SUM = "SUM";
    public static final String NAME_MIN = "MIN";
    public static final String NAME_MAX = "MAX";
    // scalar
    public static final String NAME_LOWERCASE = "LOWER";
    public static final String NAME_UPPER = "UPPER";
    public static final String NAME_ABS = "ABS";
    public static final String NAME_ROUND = "ROUND";
    public static final String NAME_EXTRACT = "EXTRACT";
    public static final String NAME_FLOOR = "FLOOR";
    public static final String NAME_RAND = "RAND";

    public static final String NAME_REINTERPRET = "Reinterpret";
    // special
    public static final String NAME_CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

    public static AggregatedColumnCalculator getColumnCalculator(
            String functionName, String fieldName,
            CompiledSQLExpression firstParam, StatementEvaluationContext context
    ) throws StatementExecutionException {
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
            case SINGLEVALUE:
                return new SingleValueCalculator(fieldName, firstParam, context);
            default:
                return null;
        }
    }

    public static boolean isAggregatedFunction(String functionNameLowercase) {
        switch (functionNameLowercase) {
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
                return true;
            default:
                return false;
        }
    }

}
