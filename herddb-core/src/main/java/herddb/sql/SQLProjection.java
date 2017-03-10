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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Projection;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.Tuple;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.SQLExpressionCompiler;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.AllNullsDataAccessor;
import herddb.utils.DataAccessor;
import herddb.utils.ProjectedDataAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Projection based on SQL
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class SQLProjection implements Projection {

    private final Column[] columns;
    private final List<OutputColumn> output;
    private final String[] fieldNames;
    private final boolean onlyCountFunctions;
    private final boolean onlyColumnFunctions;
    private final String tableAlias;
    private final AllNullsDataAccessor allNulls;

    private static final class OutputColumn {

        final Column column;
        final Expression expression;
        final CompiledSQLExpression compiledExpression;
        final net.sf.jsqlparser.schema.Column directColumnReference;

        public OutputColumn(String tableAlias, Column column, Expression expression, net.sf.jsqlparser.schema.Column directColumnReference) {
            this.column = column;
            this.expression = expression;
            this.directColumnReference = directColumnReference;
            this.compiledExpression = SQLExpressionCompiler.compileExpression(tableAlias, expression);
        }

    }

    public SQLProjection(Table table, String tableAlias, List<SelectItem> selectItems) throws StatementExecutionException {
        this.tableAlias = tableAlias;
        List<OutputColumn> raw_output = new ArrayList<>();
        int pos = 0;
        int countSimpleFunctions = 0;
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
                net.sf.jsqlparser.schema.Column directColumnReference = null;
                if (exp instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
                    if (fieldName == null) {
                        fieldName = c.getColumnName();
                    }
                    if (BuiltinFunctions.BOOLEAN_TRUE.equalsIgnoreCase(c.getColumnName())
                        || BuiltinFunctions.BOOLEAN_FALSE.equalsIgnoreCase(c.getColumnName())) {
                        columType = ColumnTypes.BOOLEAN;
                    } else {
                        Column column = table.getColumn(c.getColumnName());
                        if (column == null) {
                            throw new StatementExecutionException("invalid column name " + c.getColumnName() + " in table " + table.name + ","
                                + "only " + Arrays.toString(table.getColumns()));
                        }
                        if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equals(tableAlias)) {
                            throw new StatementExecutionException("invalid column name " + c.getColumnName() + " invalid table name " + c.getTable().getName() + ", expecting " + tableAlias);
                        }
                        columType = column.type;
                        directColumnReference = c;
                    }
                } else if (exp instanceof StringValue) {
                    columType = ColumnTypes.STRING;
                } else if (exp instanceof LongValue) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof DoubleValue) {
                    columType = ColumnTypes.DOUBLE;
                } else if (exp instanceof TimestampValue) {
                    columType = ColumnTypes.TIMESTAMP;
                } else if (exp instanceof TimeKeyExpression) {
                    columType = ColumnTypes.TIMESTAMP;
                } else if (exp instanceof Function) {
                    Function f = (Function) exp;
                    String lcaseName = f.getName();
                    columType = BuiltinFunctions.typeOfFunction(lcaseName);
                    if (lcaseName.equals(BuiltinFunctions.COUNT)) {
                        countSimpleFunctions++;
                    }
                } else if (exp instanceof Addition) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Subtraction) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Multiplication) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Division) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Parenthesis) {
                    columType = ColumnTypes.ANYTYPE;
                } else if (exp instanceof JdbcParameter) {
                    columType = ColumnTypes.ANYTYPE;
                } else if (exp instanceof CaseExpression) {
                    columType = ColumnTypes.ANYTYPE;
                } else {
                    throw new StatementExecutionException("unhandled select expression type " + exp.getClass() + ": " + exp);
                }
                if (fieldName == null) {
                    fieldName = "item" + pos;
                }
                Column col = Column.column(fieldName, columType);
                OutputColumn outputColumn = new OutputColumn(tableAlias, col, exp, directColumnReference);
                raw_output.add(outputColumn);
            } else {
                throw new StatementExecutionException("unhandled select item type " + item.getClass() + ": " + item);
            }
        }
        List<OutputColumn> complete_output = new ArrayList<>(raw_output);
        for (OutputColumn col : raw_output) {
            ColumnReferencesDiscovery discovery = new ColumnReferencesDiscovery(col.expression, tableAlias);
            col.expression.accept(discovery);
            addExpressionsForFunctionArguments(discovery, complete_output, table);
        }
        this.output = complete_output;
        this.columns = new Column[output.size()];
        this.fieldNames = new String[output.size()];

        int i = 0;
        boolean _onlyColumnFunctions = true;
        for (OutputColumn col : complete_output) {
            Column c = col.column;
            this.columns[i] = c;
            this.fieldNames[i] = c.name;
            if (col.expression instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column exp = (net.sf.jsqlparser.schema.Column) col.expression;
                String columnName = exp.getColumnName();
                if (!exp.getColumnName().equals(c.name)) {
                    // aliased column name
                    _onlyColumnFunctions = false;
                } else if (BuiltinFunctions.BOOLEAN_TRUE.equalsIgnoreCase(columnName)) {
                    _onlyColumnFunctions = false;
                } else if (BuiltinFunctions.BOOLEAN_FALSE.equalsIgnoreCase(columnName)) {
                    _onlyColumnFunctions = false;
                } else if (BuiltinFunctions.CURRENT_TIMESTAMP.equalsIgnoreCase(columnName)) {
                    _onlyColumnFunctions = false;
                }
            } else {
                _onlyColumnFunctions = false;
            }

            i++;
        }
        this.onlyColumnFunctions = _onlyColumnFunctions;
        this.onlyCountFunctions = countSimpleFunctions == fieldNames.length;
        this.allNulls = onlyCountFunctions ? new AllNullsDataAccessor(fieldNames) : null;
    }

    public SQLProjection(String defaultTableSpace, Map<String, Table> tables, List<SelectItem> selectItems) throws StatementExecutionException {
        this.tableAlias = null;
        this.onlyColumnFunctions = false;
        List<OutputColumn> raw_output = new ArrayList<>();
        int pos = 0;
        int countSimpleFunctions = 0;
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
                net.sf.jsqlparser.schema.Column directColumnReference = null;
                if (exp instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) exp;
                    if (fieldName == null) {
                        fieldName = c.getColumnName();
                    }
                    TableRef tableRef = TableRef.buildFrom(c.getTable(), defaultTableSpace);
                    String aliasName = tableRef.tableAlias;
                    Table table = tables.get(aliasName);
                    if (table == null) {
                        throw new StatementExecutionException("invalid alias name " + aliasName);
                    }
                    Column column = table.getColumn(c.getColumnName());
                    if (column == null) {
                        throw new StatementExecutionException("invalid column name " + c.getColumnName());
                    }
                    columType = column.type;
                    directColumnReference = c;
                } else if (exp instanceof StringValue) {
                    columType = ColumnTypes.STRING;
                } else if (exp instanceof LongValue) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof DoubleValue) {
                    columType = ColumnTypes.DOUBLE;
                } else if (exp instanceof TimestampValue) {
                    columType = ColumnTypes.TIMESTAMP;
                } else if (exp instanceof TimeKeyExpression) {
                    columType = ColumnTypes.TIMESTAMP;
                } else if (exp instanceof Function) {
                    Function f = (Function) exp;
                    String lcaseName = f.getName();
                    columType = BuiltinFunctions.typeOfFunction(lcaseName);
                    if (lcaseName.equals(BuiltinFunctions.COUNT)) {
                        countSimpleFunctions++;
                    }
                } else if (exp instanceof Addition) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Subtraction) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Multiplication) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Division) {
                    columType = ColumnTypes.LONG;
                } else if (exp instanceof Parenthesis) {
                    columType = ColumnTypes.ANYTYPE;
                } else if (exp instanceof JdbcParameter) {
                    columType = ColumnTypes.ANYTYPE;
                } else {
                    throw new StatementExecutionException("unhandled select expression type " + exp.getClass() + ": " + exp);
                }
                if (fieldName == null) {
                    fieldName = "item" + pos;
                }
                Column col = Column.column(fieldName, columType);
                OutputColumn outputColumn = new OutputColumn(tableAlias, col, exp, directColumnReference);
                raw_output.add(outputColumn);
            } else if (item instanceof AllTableColumns) {
                AllTableColumns c = (AllTableColumns) item;
                TableRef tableRef = TableRef.buildFrom(c.getTable(), defaultTableSpace);
                String aliasName = tableRef.tableAlias;
                Table table = tables.get(aliasName);
                if (table == null) {
                    throw new StatementExecutionException("invalid alias name " + aliasName);
                }
                for (herddb.model.Column tablecol : table.columns) {
                    net.sf.jsqlparser.schema.Column fakeCol = new net.sf.jsqlparser.schema.Column(c.getTable(), tablecol.name);
                    OutputColumn outputColumn = new OutputColumn(tableAlias, tablecol, fakeCol, null);
                    raw_output.add(outputColumn);
                }
            } else {
                throw new StatementExecutionException("unhandled select item type " + item.getClass() + ": " + item);
            }
        }
        List<OutputColumn> complete_output = new ArrayList<>(raw_output);
        for (OutputColumn col : raw_output) {
            ColumnReferencesDiscovery discovery = new ColumnReferencesDiscovery(col.expression);
            col.expression.accept(discovery);
            String alias = discovery.getMainTableAlias();
            if (alias == null) {
                throw new StatementExecutionException("unhandled select item with function " + col.expression);
            }
            Table table = tables.get(alias);
            if (table == null) {
                throw new StatementExecutionException("bad select item with table alias  " + alias + " -> " + col.expression);
            }
            addExpressionsForFunctionArguments(discovery, complete_output, table);

        }

        this.output = complete_output;

        this.columns = new Column[output.size()];

        this.fieldNames = new String[output.size()];
        final int size = output.size();
        for (int i = 0; i < size; i++) {
            Column c = output.get(i).column;
            this.columns[i] = c;
            this.fieldNames[i] = c.name;
        }

        this.onlyCountFunctions = countSimpleFunctions == fieldNames.length;
        this.allNulls = onlyCountFunctions ? new AllNullsDataAccessor(fieldNames) : null;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    private static void addExpressionsForFunctionArguments(ColumnReferencesDiscovery discovery, List<OutputColumn> output, Table table) throws StatementExecutionException {
        List<net.sf.jsqlparser.schema.Column> columns = discovery.getColumnsByTable().get(table.name);
        if (columns != null) {
            for (Expression e : columns) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e;
                String columnName = c.getColumnName();
                boolean found = false;
                for (OutputColumn outputColumn : output) {
                    if (columnName.equalsIgnoreCase(outputColumn.column.name)) {
                        found = true;
                        break;
                    } else if (outputColumn.directColumnReference != null
                        && outputColumn.directColumnReference.getColumnName().equalsIgnoreCase(columnName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    Column column = table.getColumn(c.getColumnName());
                    if (column == null) {
                        throw new StatementExecutionException("invalid column name " + c.getColumnName());
                    }
                    output.add(new OutputColumn(null, Column.column(columnName, column.type), c, null));
                }
            }
        }
    }

    @Override
    public DataAccessor map(DataAccessor tuple, StatementEvaluationContext context) throws StatementExecutionException {
        if (onlyCountFunctions) {
            return allNulls;
        }
        if (onlyColumnFunctions) {
            return new ProjectedDataAccessor(fieldNames, tuple);
        }
        List<Object> values = new ArrayList<>(output.size());
        for (OutputColumn col : output) {
            Object value = col.compiledExpression.evaluate(tuple, context);
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
