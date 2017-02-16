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
import herddb.sql.functions.BuiltinFunctions;
import static herddb.sql.functions.BuiltinFunctions.CURRENT_TIMESTAMP;
import herddb.utils.RawString;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

/**
 * Created a pure Java implementation of the expression which represents the given jSQLParser Excpession
 *
 * @author enrico.olivelli
 */
public class SQLExpressionCompiler {

    public static CompiledSQLExpression compileExpression(String validatedTableAlias,
        Expression exp) {
        CompiledSQLExpression compiled = tryCompileExpression(validatedTableAlias, exp);
        if (compiled != null) {
            return compiled;
        }
        // TODO: make all kind of expression 'compilable'
        return new InterpretedSQLExpression(validatedTableAlias, exp);
    }

    private static CompiledSQLExpression tryCompileExpression(
        String validatedTableAlias,
        Expression exp) {
        if (exp instanceof BinaryExpression) {
            CompiledSQLExpression compiled = compileSpecialBinaryExpression(validatedTableAlias, exp);
            if (compiled != null) {
                return compiled;
            }
        } else if (exp instanceof net.sf.jsqlparser.schema.Column) {
            CompiledSQLExpression compiled = compileColumnExpression(validatedTableAlias, exp);
            if (compiled != null) {
                return compiled;
            }
        } else if (exp instanceof StringValue) {
            return new ConstantExpression(RawString.of(((StringValue) exp).getValue()));
        } else if (exp instanceof LongValue) {
            return new ConstantExpression(((LongValue) exp).getValue());
        } else if (exp instanceof TimestampValue) {
            return new ConstantExpression(((TimestampValue) exp).getValue());
        } else if (exp instanceof NullValue) {
            return new ConstantExpression(null);
        } else if (exp instanceof TimeKeyExpression) {
            TimeKeyExpression ext = (TimeKeyExpression) exp;
            if (CURRENT_TIMESTAMP.equalsIgnoreCase(ext.getStringValue())) {
                return new ConstantExpression(new java.sql.Timestamp(System.currentTimeMillis()));
            } else {
                throw new StatementExecutionException("unhandled expression " + exp);
            }
        } else if (exp instanceof JdbcParameter) {
            int index = ((JdbcParameter) exp).getIndex();
            return new JdbcParameterExpression(index);
        } else if (exp instanceof AndExpression) {
            AndExpression and = (AndExpression) exp;
            CompiledSQLExpression left = compileExpression(validatedTableAlias, and.getLeftExpression());
            if (left == null) {
                return null;
            }
            CompiledSQLExpression right = compileExpression(validatedTableAlias, and.getRightExpression());
            if (right == null) {
                return null;
            }
            return new CompiledAndExpresssion(and.isNot(), left, right);
        } else if (exp instanceof OrExpression) {
            OrExpression and = (OrExpression) exp;
            CompiledSQLExpression left = compileExpression(validatedTableAlias, and.getLeftExpression());
            if (left == null) {
                return null;
            }
            CompiledSQLExpression right = compileExpression(validatedTableAlias, and.getRightExpression());
            if (right == null) {
                return null;
            }
            return new CompiledOrExpresssion(and.isNot(), left, right);
        }
        return null;
    }

    private static CompiledSQLExpression compileColumnExpression(String validatedTableAlias, Expression exp) {
        net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
        if (validatedTableAlias != null) {
            if (c.getTable() != null && c.getTable().getName() != null
                && !c.getTable().getName().equals(validatedTableAlias)) {
                return null;
            }
        }
        return new ColumnExpression(c.getColumnName());
    }

    private static CompiledSQLExpression compileSpecialBinaryExpression(String validatedTableAlias, Expression exp) {
        BinaryExpression be = (BinaryExpression) exp;

        // MOST frequent expressions "TABLE.COLUMNNAME OPERATOR ?", we can hardcode the access to the column and to the JDBC parameter
        
        if (be.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) be.getLeftExpression();
            if (validatedTableAlias != null) {
                if (c.getTable() != null && c.getTable().getName() != null
                    && !c.getTable().getName().equals(validatedTableAlias)) {
                    return null;
                }
            }

            String columnName = c.getColumnName();
            switch (columnName) {
                case BuiltinFunctions.BOOLEAN_TRUE:
                    return null;
                case BuiltinFunctions.BOOLEAN_FALSE:
                    return null;
                default:
                    // OK !
                    break;
            }

            if (be.getRightExpression() instanceof JdbcParameter) {
                JdbcParameter jdbcParam = (JdbcParameter) be.getRightExpression();
                int jdbcIndex = jdbcParam.getIndex();
                if (be instanceof EqualsTo) {
                    return new ColumnEqualsJdbcParameter(be.isNot(), columnName, jdbcIndex);
                } else if (be instanceof NotEqualsTo) {
                    return new ColumnNotEqualsJdbcParameter(be.isNot(), columnName, jdbcIndex);
                } else if (be instanceof GreaterThanEquals) {
                    return new ColumnGreaterThanEqualsJdbcParameter(be.isNot(), columnName, jdbcIndex);
                } else if (be instanceof GreaterThan) {
                    return new ColumnGreaterThanJdbcParameter(be.isNot(), columnName, jdbcIndex);
                } else if (be instanceof MinorThan) {
                    return new ColumnMinorThanJdbcParameter(be.isNot(), columnName, jdbcIndex);
                } else if (be instanceof MinorThanEquals) {
                    return new ColumnMinorThanEqualsJdbcParameter(be.isNot(), columnName, jdbcIndex);
                }
            } // TODO handle "TABLE.COLUMNNAME OPERATOR CONSTANT"
        }
        return null;
    }
}
