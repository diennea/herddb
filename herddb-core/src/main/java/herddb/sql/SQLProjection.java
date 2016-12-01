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
package herddb.sql;

import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Projection;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.Tuple;
import herddb.sql.functions.BuiltinFunctions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Projection based on SQL
 *
 * @author enrico.olivelli
 */
public class SQLProjection implements Projection {

    private final Column[] columns;
    private final List<OutputColumn> output;
    private final String[] fieldNames;
    private final String tableAlias;

    private static final class OutputColumn {

        final Column column;
        final Expression expression;

        public OutputColumn(Column column, Expression expression) {
            this.column = column;
            this.expression = expression;
        }

    }

    public SQLProjection(Table table, String tableAlias, List<SelectItem> selectItems) throws StatementExecutionException {
        this.tableAlias = tableAlias;
        List<OutputColumn> raw_output = new ArrayList<>();
        int pos = 0;
        for (SelectItem item : selectItems) {
            pos++;
            if (item instanceof SelectExpressionItem) {
                SelectExpressionItem si = (SelectExpressionItem) item;
                Alias alias = si.getAlias();

                int columType;
                String fieldName = null;
                if (alias != null && alias.getName() != null) {
                    fieldName = alias.getName();
                }
                Expression exp = si.getExpression();
                if (exp instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
                    if (fieldName == null) {
                        fieldName = c.getColumnName();
                    }
                    Column column = table.getColumn(c.getColumnName());
                    if (column == null) {
                        throw new StatementExecutionException("invalid column name " + c.getColumnName());
                    }
                    if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equalsIgnoreCase(tableAlias)) {
                        throw new StatementExecutionException("invalid column name " + c.getColumnName() + " invalid table name " + c.getTable().getName() + ", expecting " + tableAlias);
                    }
                    columType = column.type;
                } else if (exp instanceof StringValue) {
                    columType = ColumnTypes.STRING;
                } else if (exp instanceof LongValue) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof TimestampValue) {
                    columType = ColumnTypes.TIMESTAMP;
                } else if (exp instanceof Function) {
                    Function f = (Function) exp;
                    columType = BuiltinFunctions.typeOfFunction(f.getName());
                } else if (exp instanceof Addition) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Subtraction) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof JdbcParameter) {
                    columType = ColumnTypes.ANYTYPE;
                } else {
                    throw new StatementExecutionException("unhandled select expression type " + exp.getClass() + ": " + exp);
                }
                if (fieldName == null) {
                    fieldName = "item" + pos;
                }
                Column col = Column.column(fieldName, columType);
                OutputColumn outputColumn = new OutputColumn(col, exp);
                raw_output.add(outputColumn);
            } else {
                throw new StatementExecutionException("unhandled select item type " + item.getClass() + ": " + item);
            }
        }
        List<OutputColumn> complete_output = new ArrayList<>(raw_output);
        for (OutputColumn col : raw_output) {
            if (col.expression instanceof Function) {
                addExpressionsForFunctionArguments((Function) col.expression, complete_output, table);
            }
        }
        this.output = complete_output;
        this.columns = new Column[output.size()];
        this.fieldNames = new String[output.size()];
        for (int i = 0; i < output.size(); i++) {
            Column c = output.get(i).column;
            this.columns[i] = c;
            this.fieldNames[i] = c.name;
        }
    }

    private static void addExpressionsForFunctionArguments(Function f, List<OutputColumn> output, Table table) throws StatementExecutionException {
        if (f.getParameters() != null && f.getParameters().getExpressions() != null) {
            for (Expression e : f.getParameters().getExpressions()) {
                if (e instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e;
                    String columnName = c.getColumnName();
                    boolean found = false;
                    for (OutputColumn outputColumn : output) {
                        if (columnName.equalsIgnoreCase(outputColumn.column.name)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        Column column = table.getColumn(c.getColumnName());
                        if (column == null) {
                            throw new StatementExecutionException("invalid column name " + c.getColumnName());
                        }
                        output.add(new OutputColumn(Column.column(columnName, column.type), c));
                    }
                }
            }
        }
    }

    @Override
    public Tuple map(Tuple tuple, StatementEvaluationContext context) throws StatementExecutionException {
        Map<String, Object> record = tuple.toMap();
        List<Object> values = new ArrayList<>(output.size());
        for (OutputColumn col : output) {
            Object value;
            value = BuiltinFunctions.computeValue(col.expression, record, context.getJdbcParameters());
            values.add(value);
        }
        return new Tuple(
            fieldNames,
            values.toArray()
        );
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

}
