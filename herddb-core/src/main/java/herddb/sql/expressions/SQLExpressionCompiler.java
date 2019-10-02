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

import herddb.model.StatementExecutionException;
import herddb.sql.CalcitePlanner;
import herddb.sql.functions.BuiltinFunctions;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Created a pure Java implementation of the expression which represents the given jSQLParser Expression
 *
 * @author enrico.olivelli
 */
public class SQLExpressionCompiler {

    public static CompiledSQLExpression compileExpression(RexNode expression) {
        if (expression == null) {
            return null;
        }
        if (expression instanceof RexDynamicParam) {
            RexDynamicParam p = (RexDynamicParam) expression;
            return new TypedJdbcParameterExpression(p.getIndex(), CalcitePlanner.convertToHerdType(p.getType()));
        } else if (expression instanceof RexLiteral) {
            RexLiteral p = (RexLiteral) expression;
            if (p.isNull()) {
                return new ConstantExpression(null);
            } else {
                return new ConstantExpression(safeValue(p.getValue3(), p.getType(), p.getTypeName()));
            }
        } else if (expression instanceof RexInputRef) {
            RexInputRef p = (RexInputRef) expression;
            return new AccessCurrentRowExpression(p.getIndex());
        } else if (expression instanceof RexCall) {
            RexCall p = (RexCall) expression;
            SqlOperator op = p.op;
            String name = op.getName();
            CompiledSQLExpression[] operands = new CompiledSQLExpression[p.operands.size()];
//            System.out.println("operator '" + op + "' with " + p.operands.size() + " ops");
            int i = 0;
            for (RexNode operand : p.operands) {
//                System.out.println("operand: " + operand);
                operands[i++] = compileExpression(operand);
            }
            switch (name) {
                case "=":
                    return new CompiledEqualsExpression(false, operands[0], operands[1]);
                case "<>":
                    return new CompiledNotEqualsExpression(false, operands[0], operands[1]);
                case ">":
                    return new CompiledGreaterThenExpression(false, operands[0], operands[1]);
                case ">=":
                    return new CompiledGreaterThenEqualsExpression(false, operands[0], operands[1]);
                case "<":
                    return new CompiledMinorThenExpression(false, operands[0], operands[1]);
                case "<=":
                    return new CompiledMinorThenEqualsExpression(false, operands[0], operands[1]);
                case "+":
                    return new CompiledAddExpression(false, operands[0], operands[1]);
                case "-":
                    if (operands.length == 1) {
                        return new CompiledSignedExpression('-', operands[0]);
                    } else if (operands.length == 2) {
                        return new CompiledSubtractExpression(false, operands[0], operands[1]);
                    }
                    break;
                case "*":
                    return new CompiledMultiplyExpression(false, operands[0], operands[1]);
                case "/":
                    return new CompiledDivideExpression(false, operands[0], operands[1]);
                case "LIKE":
                    return new CompiledLikeExpression(false, operands[0], operands[1]);
                case "AND":
                    return new CompiledMultiAndExpression(operands);
                case "OR":
                    return new CompiledMultiOrExpression(operands);
                case "NOT":
                    return new CompiledParenthesisExpression(true, operands[0]);
                case "IS NOT NULL":
                    return new CompiledIsNullExpression(true, operands[0]);
                case "IS NOT TRUE":
                    return new CompiledIsNotTrueExpression(false, operands[0]);
                case "IS NULL":
                    return new CompiledIsNullExpression(false, operands[0]);
                case "CAST":
                    return operands[0].cast(CalcitePlanner.convertToHerdType(p.type));
                case "CASE":
                    List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases = new ArrayList<>(operands.length / 2);
                    boolean hasElse = operands.length % 2 == 1;
                    int numcases = hasElse ? ((operands.length - 1) / 2) : (operands.length / 2);
                    for (int j = 0; j < numcases; j++) {
                        cases.add(new AbstractMap.SimpleImmutableEntry<>(operands[j * 2], operands[j * 2 + 1]));
                    }
                    CompiledSQLExpression elseExp = hasElse ? operands[operands.length - 1] : null;
                    return new CompiledCaseExpression(cases, elseExp);
                case BuiltinFunctions.NAME_CURRENT_TIMESTAMP:
                    return new CompiledFunction(BuiltinFunctions.CURRENT_TIMESTAMP, Collections.emptyList());
                case BuiltinFunctions.NAME_LOWERCASE:
                    return new CompiledFunction(BuiltinFunctions.LOWER, Arrays.asList(operands));
                case BuiltinFunctions.NAME_UPPER:
                    return new CompiledFunction(BuiltinFunctions.UPPER, Arrays.asList(operands));
                case BuiltinFunctions.NAME_ABS:
                    return new CompiledFunction(BuiltinFunctions.ABS, Arrays.asList(operands));
                case BuiltinFunctions.NAME_ROUND:
                    return new CompiledFunction(BuiltinFunctions.ROUND, Arrays.asList(operands));
                default:
                    throw new StatementExecutionException("unsupported operator '" + name + "'");
            }
        } else if (expression instanceof RexFieldAccess) {
            RexFieldAccess p = (RexFieldAccess) expression;
            CompiledSQLExpression object = compileExpression(p.getReferenceExpr());
            return new AccessFieldExpression(object, p.getField().getName());
        } else if (expression instanceof RexCorrelVariable) {
            RexCorrelVariable p = (RexCorrelVariable) expression;
            return new AccessCorrelVariableExpression(p.id.getId(), p.id.getName());
        }
        throw new StatementExecutionException("not implemented expression type " + expression.getClass() + ": " + expression);
    }

    private static Object safeValue(Object value3, RelDataType relDataType, SqlTypeName sqlTypeName) {
        if (value3 instanceof BigDecimal) {
            if (relDataType instanceof BasicSqlType) {
                sqlTypeName = relDataType.getSqlTypeName();
            }
            if (sqlTypeName == SqlTypeName.DECIMAL) {
                return ((BigDecimal) value3).doubleValue();
            }
            return ((BigDecimal) value3).longValue();
        }
        return value3;
    }
}
