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

import herddb.model.ColumnTypes;
import herddb.model.StatementExecutionException;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

/**
 * SQL aggregate functions
 *
 * @author enrico.olivelli
 */
public class BuiltinFunctions {

    public static final String COUNT = "count";
    public static final String LOWER = "lower";
    public static final String UPPER = "upper";

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
                return true;
            default:
                return false;
        }
    }

    public static int typeOfFunction(String name) throws StatementExecutionException {
        switch (name.toLowerCase()) {
            case BuiltinFunctions.COUNT:
                return ColumnTypes.LONG;

            case BuiltinFunctions.LOWER:
            case BuiltinFunctions.UPPER:
                return ColumnTypes.STRING;
            default:
                throw new StatementExecutionException("unhandled function " + name);

        }
    }
    
    public static Object computeValue(Expression exp, Map<String, Object> record) throws StatementExecutionException {
        Object value;
        if (exp instanceof net.sf.jsqlparser.schema.Column) {
            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
            value = record.get(c.getColumnName());
        } else {
            if (exp instanceof StringValue) {
                value = ((StringValue) exp).getValue();
            } else {
                if (exp instanceof LongValue) {
                    value = ((LongValue) exp).getValue();
                } else {
                    if (exp instanceof Function) {
                        Function f = (Function) exp;
                        value = computeFunction(f, record);
                    } else {
                        throw new StatementExecutionException("unhandled select expression type " + exp.getClass() + ": " + exp);
                    }
                }
            }
        }
        return value;
    }

    public static Object computeFunction(Function f, Map<String, Object> record) throws StatementExecutionException {
        String name = f.getName().toLowerCase();
        switch (name) {
            case BuiltinFunctions.COUNT:
                // AGGREGATED FUNCTION
                return null;
            case BuiltinFunctions.LOWER: {
                Object computed = computeValue(f.getParameters().getExpressions().get(0), record);
                if (computed == null) {
                    return null;
                }
                return computed.toString().toLowerCase();
            }
            case BuiltinFunctions.UPPER: {
                Object computed = computeValue(f.getParameters().getExpressions().get(0), record);
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
