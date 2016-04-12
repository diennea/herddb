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

import herddb.codec.RecordSerializer;
import herddb.core.DBManager;
import herddb.core.TableManager;
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Predicate;
import herddb.model.RecordFunction;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class SQLTranslator {

    private final DBManager manager;

    public SQLTranslator(DBManager manager) {
        this.manager = manager;
    }

    public Statement translate(String query, List<Object> parameters) throws StatementExecutionException {
        try {
            net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(query);
            if (stmt instanceof CreateTable) {
                return buildCreateTableStatement((CreateTable) stmt);
            }
            if (stmt instanceof Insert) {
                return buildInsertStatement((Insert) stmt, parameters);
            }
            if (stmt instanceof Delete) {
                return buildDeleteStatement((Delete) stmt, parameters);
            }
            if (stmt instanceof Update) {
                return buildUpdateStatement((Update) stmt, parameters);
            }
            if (stmt instanceof Select) {
                return buildSelectStatement((Select) stmt, parameters);
            }
            if (stmt instanceof Execute) {
                return buildExecuteStatement((Execute) stmt, parameters);
            }
            throw new StatementExecutionException("unable to parse query " + query + ", type " + stmt.getClass());
        } catch (JSQLParserException err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }

    }

    private Statement buildCreateTableStatement(CreateTable s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        Table.Builder tablebuilder = Table.builder().name(tableName).tablespace(tableSpace);
        for (ColumnDefinition cf : s.getColumnDefinitions()) {
            int type;
            switch (cf.getColDataType().getDataType()) {
                case "string":
                    type = ColumnTypes.STRING;
                    break;
                case "long":
                    type = ColumnTypes.LONG;
                    break;
                case "int":
                    type = ColumnTypes.INTEGER;
                    break;
                case "bytea":
                    type = ColumnTypes.BYTEARRAY;
                    break;
                default:
                    throw new StatementExecutionException("bad type " + cf.getColDataType().getDataType());
            }
            tablebuilder.column(cf.getColumnName(), type);
            if (cf.getColumnSpecStrings() != null && cf.getColumnSpecStrings().contains("primary")) {
                tablebuilder.primaryKey(cf.getColumnName());
            }

        }
        try {
            CreateTableStatement statement = new CreateTableStatement(tablebuilder.build());
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition", err);
        }
    }

    private Statement buildInsertStatement(Insert s, List<Object> parameters) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();
        Map<String, Object> record = new HashMap<>();
        int index = 0;
        AtomicInteger jdbcParameterPos = new AtomicInteger();
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
            }
            ExpressionList list = (ExpressionList) s.getItemsList();
            Expression expression = list.getExpressions().get(index);
            Object _value = resolveValue(expression, parameters, jdbcParameterPos);
            record.put(column.name, _value);
            index++;
        }

        try {
            return new InsertStatement(tableSpace, tableName, RecordSerializer.toRecord(record, table));
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private Statement buildDeleteStatement(Delete s, List<Object> parameters) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) s.getTable();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();

        Bytes key = findPrimaryKeyEqualsTo(s.getWhere(), table, parameters, new AtomicInteger());
        if (key == null) {
            // DELETE FROM TABLE WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }
        Predicate where = buildPredicate(s.getWhere(), table, parameters, 0);
        try {
            return new DeleteStatement(tableSpace, tableName, key, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private Statement buildUpdateStatement(Update s, List<Object> parameters) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) s.getTables().get(0);
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();
        Map<String, Object> record = new HashMap<>();

        int index = 0;
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
            }
        }
        RecordFunction function = buildRecordFunction(s.getColumns(), s.getExpressions(), parameters, table);
        int setClauseParamters = (int) s.getExpressions().stream().filter(e -> e instanceof JdbcParameter).count();
        Bytes key = findPrimaryKeyEqualsTo(s.getWhere(), table, parameters, new AtomicInteger(setClauseParamters));
        if (key == null) {
            // UPDATE TABLE SET XXX WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }

        Predicate where = buildPredicate(s.getWhere(), table, parameters, setClauseParamters);

        try {
            return new UpdateStatement(tableSpace, tableName, key, function, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private RecordFunction buildRecordFunction(List<net.sf.jsqlparser.schema.Column> columns, List<Expression> expressions, List<Object> parameters, Table table) {
        return new SQLRecordFunction(table, columns, expressions, parameters);
    }

    private Predicate buildPredicate(Expression where, Table table, List<Object> parameters, int parameterPos) {
        if (where instanceof EqualsTo) {
            // surely this is the only predicate on the PK, we can skip it
            return null;
        }
        return new SQLRecordPredicate(table, where, parameters, parameterPos);

    }

    private int countJdbcParametersUsedByExpression(Expression e) {
        if (e instanceof Column) {
            return 0;
        }
        if (e instanceof BinaryExpression) {
            BinaryExpression bi = (BinaryExpression) e;
            return countJdbcParametersUsedByExpression(bi.getLeftExpression()) + countJdbcParametersUsedByExpression(bi.getRightExpression());
        }
        if (e instanceof JdbcParameter) {
            return 1;
        }
        throw new UnsupportedOperationException("unsupported expression type " + e.getClass() + " (" + e + ")");
    }

    private Bytes findPrimaryKeyEqualsTo(Expression where, Table table, List<Object> parameters, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            Bytes keyOnLeft = validatePrimaryKeyEqualsToExpression(and.getLeftExpression(), table, parameters, jdbcParameterPos);
            if (keyOnLeft != null) {
                return keyOnLeft;
            }
            int countJdbcParametersUsedByLeft = countJdbcParametersUsedByExpression(and.getLeftExpression());

            Bytes keyOnRight = validatePrimaryKeyEqualsToExpression(and.getRightExpression(), table, parameters, new AtomicInteger(jdbcParameterPos.get() + countJdbcParametersUsedByLeft));
            if (keyOnRight != null) {
                return keyOnRight;
            }
        } else if (where instanceof EqualsTo) {
            Bytes keyDirect = validatePrimaryKeyEqualsToExpression(where, table, parameters, jdbcParameterPos);
            if (keyDirect != null) {
                return keyDirect;
            }
        }

        return null;
    }

    private Object resolveValue(Expression expression, List<Object> parameters, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            return parameters.get(jdbcParameterPos.getAndIncrement());
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else {
            throw new StatementExecutionException("unsupported value type " + expression.getClass());
        }
    }

    private Bytes validatePrimaryKeyEqualsToExpression(Expression testExpression, Table table1, List<Object> parameters, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        Bytes result = null;
        if (testExpression instanceof EqualsTo) {
            EqualsTo e = (EqualsTo) testExpression;
            if (e.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
                if (column.getColumnName().equals(table1.primaryKeyColumn)) {
                    Object value = resolveValue(e.getRightExpression(), parameters, jdbcParameterPos);
                    result = new Bytes(RecordSerializer.serialize(value, table1.getColumn(table1.primaryKeyColumn).type));
                }
            } else if (e.getLeftExpression() instanceof AndExpression) {
                result = findPrimaryKeyEqualsTo((AndExpression) e.getLeftExpression(), table1, parameters, jdbcParameterPos);
            }
        }
        return result;
    }

    private Statement buildSelectStatement(Select s, List<Object> parameters) throws StatementExecutionException {
        PlainSelect selectBody = (PlainSelect) s.getSelectBody();
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) selectBody.getFromItem();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();
        Map<String, Object> record = new HashMap<>();

        for (SelectItem c : selectBody.getSelectItems()) {
            if (!(c instanceof AllColumns)) {
                throw new StatementExecutionException("unsupported select " + c.getClass() + " " + c);
            }
        }
        if (selectBody.getWhere() == null) {
            throw new StatementExecutionException("unsupported SELECT without WHERE");
        }

        // SELECT * FROM WHERE KEY=? AND ....
        Bytes key = findPrimaryKeyEqualsTo(selectBody.getWhere(), table, parameters, new AtomicInteger());

        if (key == null) {
            throw new StatementExecutionException("unsupported where " + selectBody.getWhere() + " " + selectBody.getWhere().getClass());
        }

        Predicate where = buildPredicate(selectBody.getWhere(), table, parameters, 0);

        try {
            return new GetStatement(tableSpace, tableName, key, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private Statement buildExecuteStatement(Execute execute, List<Object> parameters) throws StatementExecutionException {
        switch (execute.getName()) {
            case "BEGINTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), parameters, new AtomicInteger());
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName");
                }
                return new BeginTransactionStatement(tableSpaceName.toString());
            }
            case "COMMITTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                AtomicInteger pos = new AtomicInteger();
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), parameters, pos);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), parameters, pos);
                if (transactionId == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new CommitTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }

            }
            case "ROLLBACKTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                AtomicInteger pos = new AtomicInteger();
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), parameters, pos);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), parameters, pos);
                if (transactionId == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new RollbackTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
            }
            default:
                throw new StatementExecutionException("Unsupported command " + execute.getName());
        }
    }
}
