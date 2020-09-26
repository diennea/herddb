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

import static herddb.sql.JSQLParserPlanner.checkSupported;
import static herddb.sql.functions.BuiltinFunctions.AVG;
import static herddb.sql.functions.BuiltinFunctions.COUNT;
import static herddb.sql.functions.BuiltinFunctions.MAX;
import static herddb.sql.functions.BuiltinFunctions.MIN;
import static herddb.sql.functions.BuiltinFunctions.SUM;
import static herddb.sql.functions.BuiltinFunctions.SUM0;
import herddb.core.HerdDBInternalException;
import herddb.model.ColumnTypes;
import herddb.model.StatementExecutionException;
import herddb.sql.JSQLParserPlanner;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.IntHolder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Table;

/**
 * Created a pure Java implementation of the expression which represents the given jSQLParser Expression
 *
 * @author enrico.olivelli
 */
public class SQLParserExpressionCompiler {


    public static CompiledSQLExpression compileExpression(Expression expression, OpSchema tableSchema) {
        if (expression == null) {
            return null;
        }
        if (expression instanceof JdbcParameter) {
            JdbcParameter p = (JdbcParameter) expression;
            return new JdbcParameterExpression(p.getIndex() - 1);
        } else if (expression instanceof StringValue
                || expression instanceof LongValue
                || expression instanceof NullValue
                || expression instanceof DoubleValue
                || expression instanceof TimestampValue) {
            return new ConstantExpression(JSQLParserPlanner.resolveValue(expression, false));
        } else if (expression instanceof net.sf.jsqlparser.schema.Column) {
            // mapping a reference to a Column to the index in the schema of the table
            net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) expression;
            String tableAlias = extractTableName(col);
            String columnName = col.getColumnName();
            if (isBooleanLiteral(col)) {
                return new ConstantExpression(Boolean.parseBoolean(columnName.toLowerCase()));
            }
            IntHolder indexInSchema = new IntHolder(-1);
            ColumnRef found = findColumnInSchema(tableAlias, columnName, tableSchema, indexInSchema);
            if (indexInSchema.value == -1 || found == null) {
                checkSupported(false, "Column " + tableAlias + "." + columnName + " not found in target table " + tableSchema);
            }
            return new AccessCurrentRowExpression(indexInSchema.value);
        } else if (expression instanceof BinaryExpression) {
            return compileBinaryExpression((BinaryExpression) expression, tableSchema);
        } else if (expression instanceof IsNullExpression) {
            IsNullExpression eq = (IsNullExpression) expression;
            CompiledSQLExpression left = compileExpression(eq.getLeftExpression(), tableSchema);
            return new CompiledIsNullExpression(eq.isNot(), left);
        } else if (expression instanceof NotExpression) {
            NotExpression eq = (NotExpression) expression;
            CompiledSQLExpression left = compileExpression(eq.getExpression(), tableSchema);
            // trying to reproduce Calcite behaviour
            // NOT (a <= 5)  -> (a > 5)
            // this can be wrong while dealing with NULL values, but it is the current behaviour
            if (left instanceof CompiledBinarySQLExpression) {
                CompiledBinarySQLExpression binaryLeft = (CompiledBinarySQLExpression) left;
                if (binaryLeft.isNegateSupported()) {
                    return binaryLeft.negate();
                }
            }
            return new CompiledNotExpression(left);
        } else if (expression instanceof Parenthesis) {
            Parenthesis eq = (Parenthesis) expression;
            return compileExpression(eq.getExpression(), tableSchema);
        } else if (expression instanceof SignedExpression) {
            SignedExpression eq = (SignedExpression) expression;
            return new CompiledSignedExpression(eq.getSign(), compileExpression(eq.getExpression(), tableSchema));
        } else if (expression instanceof InExpression) {
            InExpression eq = (InExpression) expression;
            checkSupported(eq.getOldOracleJoinSyntax() == EqualsTo.NO_ORACLE_JOIN);
            checkSupported(eq.getOraclePriorPosition() == EqualsTo.NO_ORACLE_PRIOR);
            checkSupported(eq.getLeftItemsList() == null);
            checkSupported(eq.getMultiExpressionList() == null);
            checkSupported(eq.getRightExpression() == null);
            CompiledSQLExpression left = compileExpression(eq.getLeftExpression(), tableSchema);
            ItemsList rightItemsList = eq.getRightItemsList();
            checkSupported(rightItemsList instanceof ExpressionList);
            ExpressionList expressionList = (ExpressionList) rightItemsList;
            CompiledSQLExpression[] values = new CompiledSQLExpression[expressionList.getExpressions().size()];
            int i = 0;
            for (Expression exp : expressionList.getExpressions()) {
                values[i++] = compileExpression(exp, tableSchema);
            }
            return new CompiledInExpression(left, values);
        } else if (expression instanceof TimeKeyExpression) {
            TimeKeyExpression eq = (TimeKeyExpression) expression;
            if (eq.getStringValue().equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                return new CompiledFunction(BuiltinFunctions.CURRENT_TIMESTAMP, Collections.emptyList());
            }
            // fallthru
        } else if (expression instanceof Function) {
            Function eq = (Function) expression;
            checkSupported(eq.getKeep() == null);
            checkSupported(eq.getMultipartName() != null && eq.getMultipartName().size() == 1);
            checkSupported(eq.getNamedParameters() == null);
            checkSupported(eq.getAttribute() == null);
            checkSupported(eq.getAttributeName() == null);
            List<CompiledSQLExpression> operands = new ArrayList<>();
            if (eq.getParameters() != null) {
                for (Expression e : eq.getParameters().getExpressions()) {
                    operands.add(compileExpression(e, tableSchema));
                }
            }
            switch (eq.getName().toUpperCase()) {
                case BuiltinFunctions.NAME_LOWERCASE:
                    return new CompiledFunction(BuiltinFunctions.LOWER, operands);
                case BuiltinFunctions.NAME_UPPER:
                    return new CompiledFunction(BuiltinFunctions.UPPER, operands);
                case BuiltinFunctions.NAME_ABS:
                    return new CompiledFunction(BuiltinFunctions.ABS, operands);
                case BuiltinFunctions.NAME_AVG:
                    return new CompiledFunction(BuiltinFunctions.AVG, operands);
                case BuiltinFunctions.NAME_ROUND:
                    return new CompiledFunction(BuiltinFunctions.ROUND, operands);
                case BuiltinFunctions.NAME_EXTRACT:
                    return new CompiledFunction(BuiltinFunctions.EXTRACT, operands);
                case BuiltinFunctions.NAME_FLOOR:
                    return new CompiledFunction(BuiltinFunctions.FLOOR, operands);
                case BuiltinFunctions.NAME_RAND:
                    return new CompiledFunction(BuiltinFunctions.RAND, operands);
                default:
            }
            // fallthru
        } else if (expression instanceof CaseExpression) {
            CaseExpression eq = (CaseExpression) expression;
            checkSupported(eq.getSwitchExpression() == null);
            List<WhenClause> whenClauses = eq.getWhenClauses();
            List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases = new ArrayList<>(whenClauses.size());
            for (WhenClause c : whenClauses) {
                cases.add(new AbstractMap.SimpleImmutableEntry<>(
                        compileExpression(c.getWhenExpression(), tableSchema),
                        compileExpression(c.getThenExpression(), tableSchema)
                ));
            }
            CompiledSQLExpression elseExp = eq.getElseExpression() != null ? compileExpression(eq.getElseExpression(), tableSchema) : null;
            return new CompiledCaseExpression(cases, elseExp);
        } else if (expression instanceof Between) {
            Between b = (Between) expression;
            boolean not = b.isNot();
            CompiledSQLExpression baseValue = compileExpression(b.getLeftExpression(), tableSchema);
            CompiledSQLExpression start = compileExpression(b.getBetweenExpressionStart(), tableSchema);
            CompiledSQLExpression end = compileExpression(b.getBetweenExpressionEnd(), tableSchema);

            CompiledSQLExpression result = new CompiledAndExpression(new CompiledGreaterThanEqualsExpression(baseValue, start),
                    new CompiledMinorThanEqualsExpression(baseValue, end));
            if (not) {
                return new CompiledNotExpression(result);
            } else {
                return result;
            }
        } else if (expression instanceof net.sf.jsqlparser.expression.CastExpression) {
            net.sf.jsqlparser.expression.CastExpression b = (net.sf.jsqlparser.expression.CastExpression) expression;

            CompiledSQLExpression left = compileExpression(b.getLeftExpression(), tableSchema);
            int type = JSQLParserPlanner.sqlDataTypeToColumnType(b.getType());
            return new CastExpression(left, type);
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

    private static CompiledSQLExpression compileBinaryExpression(BinaryExpression expression, OpSchema tableSchema) {
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
        } else if (expression instanceof Addition) {
            return new CompiledAddExpression(left, right);
        } else if (expression instanceof Division) {
            return new CompiledDivideExpression(left, right);
        } else if (expression instanceof Multiplication) {
            return new CompiledMultiplyExpression(left, right);
        } else if (expression instanceof LikeExpression) {
            LikeExpression eq = (LikeExpression) expression;
            if (eq.isCaseInsensitive()) {
                // this is not supported by Calcite, do not support it with jSQLParser
                throw new StatementExecutionException("ILIKE is not supported");
            }
            CompiledSQLExpression res;
            if (eq.getEscape() != null) {
                CompiledSQLExpression escape = new ConstantExpression(eq.getEscape());
                res = new CompiledLikeExpression(left, right, escape);
            } else {
                res = new CompiledLikeExpression(left, right);
            }

            if (eq.isNot()) {
                res = new CompiledNotExpression(res);
            }
            return res;
        } else if (expression instanceof Modulo) {
            return new CompiledModuloExpression(left, right);
        } else if (expression instanceof Subtraction) {
            return new CompiledSubtractExpression(left, right);
        } else if (expression instanceof NotEqualsTo) {
            NotEqualsTo eq = (NotEqualsTo) expression;
            checkSupported(eq.getOldOracleJoinSyntax() == EqualsTo.NO_ORACLE_JOIN);
            checkSupported(eq.getOraclePriorPosition() == EqualsTo.NO_ORACLE_PRIOR);
            return new CompiledNotEqualsExpression(left, right);
        } else {
            throw new StatementExecutionException("not implemented expression type " + expression.getClass() + ": " + expression);
        }
    }

    public static boolean isBooleanLiteral(net.sf.jsqlparser.schema.Column col) {
        if (col.getTable() != null) {
            return false;
        }
        String columnName = col.getColumnName().toLowerCase();
        return (columnName.equals("false") // jSQLParser does not support boolean literals
                || columnName.equals("true"));
    }

    public static Function detectAggregatedFunction(Expression expression) {
        if (expression instanceof net.sf.jsqlparser.expression.CastExpression) {
            net.sf.jsqlparser.expression.CastExpression c = (net.sf.jsqlparser.expression.CastExpression) expression;
            return detectAggregatedFunction(c.getLeftExpression());
        }
        if (!(expression instanceof Function)) {
            return null;
        }
        Function fn = (Function) expression;
        if (BuiltinFunctions.isAggregatedFunction(fn.getName().toLowerCase())) {
            return fn;
        }
        return null;
    }

    public static int getAggregateFunctionType(Expression exp, OpSchema tableSchema) {
        if (exp instanceof net.sf.jsqlparser.expression.CastExpression) {
            // SELECT CAST(avg(n) as DOUBLE)
            net.sf.jsqlparser.expression.CastExpression c = (net.sf.jsqlparser.expression.CastExpression) exp;
            return JSQLParserPlanner.sqlDataTypeToColumnType(c.getType());
        }
        Function fn = (Function) exp;
        String functionNameLowercase = fn.getName().toLowerCase();
        switch (functionNameLowercase) {
            case COUNT:
                return ColumnTypes.LONG;
            case SUM:
            case SUM0:
            case AVG:
            case MIN:
            case MAX:
                checkSupported(fn.getParameters().getExpressions().size() == 1);
                final Expression first = fn.getParameters().getExpressions().get(0);
                if (first instanceof net.sf.jsqlparser.expression.CastExpression) {
                    // SELECT AVG(CAST(n) as DOUBLE))
                    net.sf.jsqlparser.expression.CastExpression c = (net.sf.jsqlparser.expression.CastExpression) first;
                    return JSQLParserPlanner.sqlDataTypeToColumnType(c.getType());
                }
                checkSupported(first instanceof net.sf.jsqlparser.schema.Column, first.getClass());
                net.sf.jsqlparser.schema.Column cName = (net.sf.jsqlparser.schema.Column) first;
                String tableAlias = extractTableName(cName);
                ColumnRef col = findColumnInSchema(tableAlias, cName.getColumnName(), tableSchema, new IntHolder());
                checkSupported(col != null);
                // SUM of INTEGERS is an INTEGER (this is what Calcite does, but it is smarter than this)
                return col.type;
            default:
                throw new HerdDBInternalException(functionNameLowercase);
        }
    }

    public static ColumnRef getAggregateFunctionArgument(Function fn, OpSchema tableSchema) {
        String functionNameLowercase = fn.getName().toLowerCase();
        switch (functionNameLowercase) {
            case COUNT:
                return null;
            case SUM:
            case SUM0:
            case AVG:
            case MIN:
            case MAX:
                checkSupported(fn.getParameters().getExpressions().size() == 1);
                Expression first = fn.getParameters().getExpressions().get(0);
                if (first instanceof net.sf.jsqlparser.expression.CastExpression) {
                    // SELECT AVG(CAST(n) as DOUBLE))
                    first = ((net.sf.jsqlparser.expression.CastExpression) first).getLeftExpression();
                }
                checkSupported(first instanceof net.sf.jsqlparser.schema.Column);
                net.sf.jsqlparser.schema.Column cName = (net.sf.jsqlparser.schema.Column) first;
                String tableAlias = extractTableName(cName);
                // validate that it is a valid column referece in the input schema
                ColumnRef col = findColumnInSchema(tableAlias, cName.getColumnName(), tableSchema, new IntHolder());
                checkSupported(col != null);
                return col;
            default:
                throw new HerdDBInternalException(functionNameLowercase);
        }
    }

    public static ColumnRef findColumnInSchema(String tableName, String columnName, OpSchema tableSchema, IntHolder pos) {
        pos.value = 0;
        if (tableName == null || tableName.equalsIgnoreCase(tableSchema.name)) {
            for (ColumnRef colInSchema : tableSchema.columns) {
                if (colInSchema.name.equalsIgnoreCase(columnName)
                        && (colInSchema.tableName == null || colInSchema.tableName.equalsIgnoreCase(tableSchema.name))) {
                    return colInSchema;
                }
                pos.value++;
            }
        } else {
            for (ColumnRef colInSchema : tableSchema.columns) {
                if (colInSchema.name.equalsIgnoreCase(columnName)
                        && colInSchema.tableName.equalsIgnoreCase(tableName)) {
                    return colInSchema;
                }
                pos.value++;
            }
        }
        pos.value = -1;
        return null;
    }

    public static String extractTableName(net.sf.jsqlparser.schema.Column col) {
        if (col.getTable() != null) {
            Table table = col.getTable();
            checkSupported(table.getAlias() == null);
            return fixMySqlBackTicks(table.getName());
        }
        return null;
    }


    public static String fixMySqlBackTicks(String s) {
        if (s == null || s.length() < 2) {
            return s;
        }
        if (s.startsWith("`") && s.endsWith("`")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

}
