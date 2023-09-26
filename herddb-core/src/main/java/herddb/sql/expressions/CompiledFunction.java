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

import herddb.codec.RecordSerializer;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.DataAccessor;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.avatica.util.TimeUnitRange;

public class CompiledFunction implements CompiledSQLExpression {

    private final String name;
    private final List<CompiledSQLExpression> parameters;

    private final long roundMultiplier;
    private final double roundSign;

    public CompiledFunction(String name, List<CompiledSQLExpression> parameters) {
        this.name = name;
        this.parameters = parameters;

        if (name.equals(BuiltinFunctions.ROUND) && parameters.size() == 2) {
            if (parameters.size() == 2) {

                long precision;
                try {
                    precision = ((Number) parameters.get(1).evaluate(DataAccessor.NULL, null)).longValue();
                } catch (NullPointerException ex) {
                    throw new IllegalArgumentException("round second parameter must be a constant value");
                }
                long mult = 1L;
                for (int i = 0; i < Math.abs(precision); i++) {
                    mult *= 10;
                }
                roundMultiplier = mult;
                roundSign = Math.signum(precision);
            } else {
                roundMultiplier = 0;
                roundSign = 0;
            }
        } else {
            roundMultiplier = 0;
            roundSign = 0;
        }
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        switch (name) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
            case BuiltinFunctions.AVG:
            case BuiltinFunctions.MAX:
                // AGGREGATED FUNCTION
                return null;
            case BuiltinFunctions.LOWER: {
                Object parValue = parameters.get(0).evaluate(bean, context);
                return parValue.toString().toLowerCase();
            }
            case BuiltinFunctions.UPPER: {
                Object parValue = parameters.get(0).evaluate(bean, context);
                return parValue.toString().toUpperCase();
            }
            case BuiltinFunctions.ABS: {
                Object parValue = parameters.get(0).evaluate(bean, context);
                if (parValue instanceof Double) {
                    return Math.abs((double) parValue);
                } else if (parValue instanceof Long) {
                    return Math.abs((long) parValue);
                } else {
                    return Math.abs((int) parValue);
                }
            }
            case BuiltinFunctions.COSINE_SIMILARITY: {
                Object par1Value = RecordSerializer.convert(ColumnTypes.FLOATARRAY, parameters.get(0).evaluate(bean, context));
                Object par2Value = RecordSerializer.convert(ColumnTypes.FLOATARRAY, parameters.get(1).evaluate(bean, context));
                return cosineSimilarity((float[]) par1Value, (float[]) par2Value);
            }
            case BuiltinFunctions.DOT_PRODUCT: {
                Object par1Value = RecordSerializer.convert(ColumnTypes.FLOATARRAY, parameters.get(0).evaluate(bean, context));
                Object par2Value = RecordSerializer.convert(ColumnTypes.FLOATARRAY, parameters.get(1).evaluate(bean, context));
                return dotProduct((float[]) par1Value, (float[]) par2Value);
            }
            case BuiltinFunctions.EUCLIDEAN_DISTANCE: {
                Object par1Value = RecordSerializer.convert(ColumnTypes.FLOATARRAY, parameters.get(0).evaluate(bean, context));
                Object par2Value = RecordSerializer.convert(ColumnTypes.FLOATARRAY, parameters.get(1).evaluate(bean, context));
                return euclideanDistance((float[]) par1Value, (float[]) par2Value);
            }
            case BuiltinFunctions.CURRENT_TIMESTAMP:
                return context.getCurrentTimestamp();
            case BuiltinFunctions.NAME_CURRENT_DATE:
                return context.getCurrentTimestamp();
            case BuiltinFunctions.RAND:
                return ThreadLocalRandom.current().nextInt();
            case BuiltinFunctions.ROUND: {
                Object parValue = parameters.get(0).evaluate(bean, context);
                if (roundSign == 0) {
                    return (double) Math.round((double) parValue);
                } else if (roundSign > 0) {
                    return (double) Math.round((double) parValue * roundMultiplier) / roundMultiplier;
                } else {
                    return (double) Math.round((double) parValue / roundMultiplier) * roundMultiplier;
                }
            }
            case BuiltinFunctions.EXTRACT: {
                TimeUnitRange range = (TimeUnitRange) parameters.get(0).evaluate(bean, context);
                Object parValue = parameters.get(1).evaluate(bean, context);
                if (parValue == null) {
                    return null;
                }
                if (!(parValue instanceof java.sql.Timestamp)) {
                    throw new StatementExecutionException("Cannot EXTRACT " + range + " FROM a " + parValue.getClass() + " value is " + parValue);
                }
                ZonedDateTime i = ((java.sql.Timestamp) parValue).toInstant().atZone(context.getTimezone());
                switch (range) {
                    case YEAR:
                        return i.get(ChronoField.YEAR);
                    case MONTH:
                        return i.get(ChronoField.MONTH_OF_YEAR);
                    case DAY:
                        return i.get(ChronoField.DAY_OF_MONTH);
                    case DOW:
                        return i.get(ChronoField.DAY_OF_WEEK);
                    case HOUR:
                        return i.get(ChronoField.HOUR_OF_DAY);
                    case MINUTE:
                        return i.get(ChronoField.MINUTE_OF_HOUR);
                    case SECOND:
                        return i.get(ChronoField.SECOND_OF_MINUTE);
                    case MILLISECOND:
                        return i.get(ChronoField.MILLI_OF_SECOND);
                    default:
                        throw new StatementExecutionException("Unsupported EXTRACT " + range);
                }
            }
            case BuiltinFunctions.FLOOR: {
                // currently only supported FLOOR(value TO DAY) to get the midnight value
                Object parValue = parameters.get(0).evaluate(bean, context);
                if (parValue == null) {
                    return null;
                }
                TimeUnitRange range = (TimeUnitRange) parameters.get(1).evaluate(bean, context);
                if (!(parValue instanceof java.sql.Timestamp)) {
                    throw new StatementExecutionException("Cannot FLOOR " + range + " FROM a " + parValue.getClass() + " value is " + parValue);
                }
                ZonedDateTime i = ((java.sql.Timestamp) parValue).toInstant().atZone(context.getTimezone());
                switch (range) {
                    case DAY:
                        return new java.sql.Timestamp(i.toLocalDate().atStartOfDay(context.getTimezone()).toInstant().toEpochMilli());
                    default:
                        throw new StatementExecutionException("Unsupported FLOOR " + range);
                }
            }
            default:
                throw new StatementExecutionException("unhandled function " + name + " operands " + parameters);
        }
    }

    private static float dotProduct(float[] arr1, float[] arr2) {
        if (arr1.length != arr2.length) {
            throw new IllegalArgumentException("Arrays must have the same length");
        }

        float result = 0.0f;
        for (int i = 0; i < arr1.length; i++) {
            result += arr1[i] * arr2[i];
        }

        return result;
    }

    private static float euclideanDistance(float[] arr1, float[] arr2) {
        if (arr1.length != arr2.length) {
            throw new IllegalArgumentException("Arrays must have the same length");
        }

        float sumOfSquares = 0.0f;
        for (int i = 0; i < arr1.length; i++) {
            float diff = arr1[i] - arr2[i];
            sumOfSquares += diff * diff;
        }

        return (float) Math.sqrt(sumOfSquares);
    }

    private static float euclideanNorm(float[] arr) {
        float sumOfSquares = 0.0f;
        for (float value : arr) {
            sumOfSquares += value * value;
        }
        return (float) Math.sqrt(sumOfSquares);
    }

    public static float cosineSimilarity(float[] arr1, float[] arr2) {
        float norm1 = euclideanNorm(arr1);
        if (norm1 == 0) {
            return 0;
        }
        float norm2 = euclideanNorm(arr2);
        if (norm2 == 0) {
            return 0.0f;
        }
        float dotProduct = dotProduct(arr1, arr2);
        return dotProduct / (norm1 * norm2);
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        if (parameters != null) {
            parameters.forEach((expression) -> {
                expression.validate(context);
            });
        }
    }

    @Override
    public String toString() {
        if (roundMultiplier > 0) {
            return "CompiledFunction{" + "name=" + name + ", parameters=" + parameters + ", roundMultiplier=" + roundMultiplier + ", roundSign=" + roundSign + '}';
        } else {
            return "CompiledFunction{" + "name=" + name + ", parameters=" + parameters + '}';
        }
    }

}
