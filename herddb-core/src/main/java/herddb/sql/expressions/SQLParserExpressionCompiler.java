/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql.expressions;

import static herddb.sql.DDLSQLPlanner.checkSupported;
import herddb.model.Column;
import herddb.model.StatementExecutionException;
import herddb.sql.DDLSQLPlanner;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;

/**
 * Created a pure Java implementation of the expression which represents the given jSQLParser Expression
 *
 * @author enrico.olivelli
 */
public class SQLParserExpressionCompiler {

    public static CompiledSQLExpression compileExpression(Expression expression, Column[] tableSchema) {
        if (expression == null) {
            return null;
        }
        if (expression instanceof JdbcParameter) {
            JdbcParameter p = (JdbcParameter) expression;
            return new JdbcParameterExpression(p.getIndex() - 1);
        } else if (expression instanceof StringValue
                || expression instanceof LongValue
                || expression instanceof NullValue
                || expression instanceof TimestampValue) {
            return new ConstantExpression(DDLSQLPlanner.resolveValue(expression, false));
        } else if (expression instanceof net.sf.jsqlparser.schema.Column) {
            // mapping a reference to a Column to the index in the schema of the table
            net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) expression;
            checkSupported(col.getTable() == null);
            String columnName = col.getColumnName();
            int pos = 0;
            int indexInSchema = -1;
            Column found = null;
            for (Column colInSchema : tableSchema) {
                if (colInSchema.getName().equalsIgnoreCase(columnName)) {
                    indexInSchema = pos;
                    found = colInSchema;
                    break;
                }
                pos++;
            }
            if (indexInSchema == -1 || found == null) {
                throw new StatementExecutionException("Column " + columnName + " not found in target table");
            }
            return new AccessCurrentRowExpression(indexInSchema);
        } else if (expression instanceof BinaryExpression) {
            return compileBinaryExpression((BinaryExpression) expression, tableSchema);
        } else if (expression instanceof IsNullExpression) {
            IsNullExpression eq = (IsNullExpression) expression;
            CompiledSQLExpression left = compileExpression(eq.getLeftExpression(), tableSchema);
            return new CompiledIsNullExpression(eq.isNot(), left);
        } else if (expression instanceof NotExpression) {
            NotExpression eq = (NotExpression) expression;
            CompiledSQLExpression left = compileExpression(eq.getExpression(), tableSchema);
            return new CompiledNotExpression(left);
        }
//        else if (expression instanceof RexLiteral) {
//            RexLiteral p = (RexLiteral) expression;
//            if (p.isNull()) {
//                return new ConstantExpression(null);
//            } else {
//                return new ConstantExpression(safeValue(p.getValue3(), p.getType(), p.getTypeName()));
//            }
//        } else if (expression instanceof RexInputRef) {
//            RexInputRef p = (RexInputRef) expression;
//            return new AccessCurrentRowExpression(p.getIndex());
//        } else if (expression instanceof RexCall) {
//            RexCall p = (RexCall) expression;
//            SqlOperator op = p.op;
//            String name = op.getName();
//            CompiledSQLExpression[] operands = new CompiledSQLExpression[p.operands.size()];
//            int i = 0;
//            for (RexNode operand : p.operands) {
//                operands[i++] = compileExpression(operand);
//            }
//            switch (name) {
//                case "=":
//                    return new CompiledEqualsExpression(operands[0], operands[1]);
//                case "<>":
//                    return new CompiledNotEqualsExpression(operands[0], operands[1]);
//                case ">":
//                    return new CompiledGreaterThenExpression(operands[0], operands[1]);
//                case ">=":
//                    return new CompiledGreaterThenEqualsExpression(operands[0], operands[1]);
//                case "<":
//                    return new CompiledMinorThenExpression(operands[0], operands[1]);
//                case "<=":
//                    return new CompiledMinorThenEqualsExpression(operands[0], operands[1]);
//                case "+":
//                    return new CompiledAddExpression(operands[0], operands[1]);
//                case "MOD":
//                    return new CompiledModuloExpression(operands[0], operands[1]);
//                case "-":
//                    if (operands.length == 1) {
//                        return new CompiledSignedExpression('-', operands[0]);
//                    } else if (operands.length == 2) {
//                        return new CompiledSubtractExpression(operands[0], operands[1]);
//                    }
//                    break;
//                case "*":
//                    return new CompiledMultiplyExpression(operands[0], operands[1]);
//                case "/":
//                    return new CompiledDivideExpression(operands[0], operands[1]);
//                case "/INT":
//                    return new CompiledDivideIntExpression(operands[0], operands[1]);
//                case "LIKE":
//                    if (operands.length == 2) {
//                        return new CompiledLikeExpression(operands[0], operands[1]);
//                    } else {
//                        // ESCAPE
//                        return new CompiledLikeExpression(operands[0], operands[1], operands[2]);
//                    }
//                case "AND":
//                    return new CompiledMultiAndExpression(operands);
//                case "OR":
//                    return new CompiledMultiOrExpression(operands);
//                case "NOT":
//                    return new CompiledParenthesisExpression(true, operands[0]);
//                case "IS NOT NULL":
//                    return new CompiledIsNullExpression(true, operands[0]);
//                case "IS NOT TRUE":
//                    return new CompiledIsNotTrueExpression(false, operands[0]);
//                case "IS NULL":
//                    return new CompiledIsNullExpression(false, operands[0]);
//                case "CAST":
//                    return operands[0].cast(CalcitePlanner.convertToHerdType(p.type));
//                case "CASE":
//                    List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases = new ArrayList<>(operands.length / 2);
//                    boolean hasElse = operands.length % 2 == 1;
//                    int numcases = hasElse ? ((operands.length - 1) / 2) : (operands.length / 2);
//                    for (int j = 0; j < numcases; j++) {
//                        cases.add(new AbstractMap.SimpleImmutableEntry<>(operands[j * 2], operands[j * 2 + 1]));
//                    }
//                    CompiledSQLExpression elseExp = hasElse ? operands[operands.length - 1] : null;
//                    return new CompiledCaseExpression(cases, elseExp);
//                case BuiltinFunctions.NAME_CURRENT_TIMESTAMP:
//                    return new CompiledFunction(BuiltinFunctions.CURRENT_TIMESTAMP, Collections.emptyList());
//                case BuiltinFunctions.NAME_LOWERCASE:
//                    return new CompiledFunction(BuiltinFunctions.LOWER, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_UPPER:
//                    return new CompiledFunction(BuiltinFunctions.UPPER, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_ABS:
//                    return new CompiledFunction(BuiltinFunctions.ABS, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_ROUND:
//                    return new CompiledFunction(BuiltinFunctions.ROUND, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_EXTRACT:
//                    return new CompiledFunction(BuiltinFunctions.EXTRACT, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_FLOOR:
//                    return new CompiledFunction(BuiltinFunctions.FLOOR, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_RAND:
//                    return new CompiledFunction(BuiltinFunctions.RAND, Arrays.asList(operands));
//                case BuiltinFunctions.NAME_REINTERPRET:
//                    if (operands.length != 1) {
//                        throw new StatementExecutionException("unsupported use of Reinterpret with " + Arrays.toString(operands));
//                    }
//                    return (CompiledSQLExpression) operands[0];
//                default:
//                    throw new StatementExecutionException("unsupported operator '" + name + "'");
//            }
//        } else if (expression instanceof RexFieldAccess) {
//            RexFieldAccess p = (RexFieldAccess) expression;
//            CompiledSQLExpression object = compileExpression(p.getReferenceExpr());
//            return new AccessFieldExpression(object, p.getField().getName());
//        } else if (expression instanceof RexCorrelVariable) {
//            RexCorrelVariable p = (RexCorrelVariable) expression;
//            return new AccessCorrelVariableExpression(p.id.getId(), p.id.getName());
//        }
        throw new StatementExecutionException("not implemented expression type " + expression.getClass() + ": " + expression);
    }

    private static CompiledSQLExpression compileBinaryExpression(BinaryExpression expression, Column[] tableSchema) {
        CompiledSQLExpression left = compileExpression(expression.getLeftExpression(), tableSchema);
        CompiledSQLExpression right = compileExpression(expression.getRightExpression(), tableSchema);
        if (expression instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) expression;
            checkSupported(eq.getOldOracleJoinSyntax() == EqualsTo.NO_ORACLE_JOIN);
            checkSupported(eq.getOraclePriorPosition() == EqualsTo.NO_ORACLE_PRIOR);
            return new CompiledEqualsExpression(left, right);
        } else if (expression instanceof AndExpression) {
            return new CompiledAndExpression(left, right);
        } else if (expression instanceof OrExpression) {
            return new CompiledOrExpression(left, right);
        } else if (expression instanceof GreaterThan) {
            return new CompiledGreaterThanExpression(left, right);
        } else if (expression instanceof GreaterThanEquals) {
            return new CompiledGreaterThanEqualsExpression(left, right);
        } else if (expression instanceof MinorThan) {
            return new CompiledMinorThanExpression(left, right);
        } else if (expression instanceof MinorThanEquals) {
            return new CompiledMinorThanEqualsExpression(left, right);
        } else {
            return null;
        }
    }
}
