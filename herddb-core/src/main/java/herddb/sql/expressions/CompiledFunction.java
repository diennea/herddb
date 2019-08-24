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

import static herddb.sql.expressions.SQLExpressionCompiler.compileExpression;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.DataAccessor;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

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

    public static CompiledFunction create(Function f, String validatedTableAlias) {
        String name = f.getName();
        List<Expression> params = null;
        if (f.getParameters() != null) {
            params = f.getParameters().getExpressions();
        }

        switch (name) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
            case BuiltinFunctions.MAX:
                // AGGREGATED FUNCTION
                return new CompiledFunction(name, null);
            case BuiltinFunctions.LOWER: {
                if (params == null || params.size() != 1) {
                    throw new StatementExecutionException("function " + name + " must have one parameter");
                }
                break;
            }
            case BuiltinFunctions.UPPER: {
                if (params == null || params.size() != 1) {
                    throw new StatementExecutionException("function " + name + " must have one parameter");
                }
                break;
            }
            case BuiltinFunctions.ABS: {
                if (params == null || params.size() != 1) {
                    throw new StatementExecutionException("function " + name + " must have one parameter");
                }
                break;
            }
            case BuiltinFunctions.ROUND: {
                if (params == null || (params.size() != 1 && params.size() != 2)) {
                    throw new StatementExecutionException("function " + name + " must have one or two parameters");
                }
                break;
            }
            default:
                throw new StatementExecutionException("unhandled function " + name);
        }

        List<CompiledSQLExpression> compiledParams = new ArrayList<>();
        for (Expression exp : f.getParameters().getExpressions()) {
            compiledParams.add(compileExpression(validatedTableAlias, exp));
        }

        return new CompiledFunction(name, compiledParams);

    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        switch (name) {
            case BuiltinFunctions.COUNT:
            case BuiltinFunctions.SUM:
            case BuiltinFunctions.MIN:
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
                } else {
                    return Math.abs((long) parValue);
                }
            }
            case BuiltinFunctions.CURRENT_TIMESTAMP:
                return context.getCurrentTimestamp();
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
            default:
                throw new StatementExecutionException("unhandled function " + name);
        }
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        if (parameters != null) {
            parameters.forEach((expression) -> {
                expression.validate(context);
            });
        }
    }

}
