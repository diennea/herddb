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
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
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
        } catch (JSQLParserException err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }
        throw new StatementExecutionException("unable to parse query " + query);
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
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
            }
            Object _value = parameters.get(index++);
            record.put(column.name, _value);
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

        long whereParamters = 0;
        Predicate where = null;
        Bytes key = null;
        if (s.getWhere() instanceof EqualsTo) {
            // DELETE FROM TABLE WHERE KEY=?
            EqualsTo e = (EqualsTo) s.getWhere();
            net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
            if (!column.getColumnName().equals(table.primaryKeyColumn)) {
                throw new StatementExecutionException("unsupported where, only on primary key field " + table.primaryKeyColumn);
            }
            JdbcParameter jdbcparam = (JdbcParameter) e.getRightExpression();
            key = new Bytes(
                    RecordSerializer.serialize(
                            parameters.get((int) whereParamters),
                            table.getColumn(table.primaryKeyColumn).type));
        } else {
            // DELETE FROM TABLE WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }

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
        long whereParamters = s.getExpressions().stream().filter(e -> e instanceof JdbcParameter).count();
        Predicate where = null;
        Bytes key = null;
        if (s.getWhere() instanceof EqualsTo) {
            // UPDATE TABLE SET XXX WHERE KEY=?
            EqualsTo e = (EqualsTo) s.getWhere();
            net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
            if (!column.getColumnName().equals(table.primaryKeyColumn)) {
                throw new StatementExecutionException("unsupported where, only on primary key field " + table.primaryKeyColumn);
            }
            JdbcParameter jdbcparam = (JdbcParameter) e.getRightExpression();
            key = new Bytes(
                    RecordSerializer.serialize(
                            parameters.get((int) whereParamters),
                            table.getColumn(table.primaryKeyColumn).type));
        } else {
            // UPDATE TABLE SET XXX WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }

        try {
            return new UpdateStatement(tableSpace, tableName, key, function, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private RecordFunction buildRecordFunction(List<net.sf.jsqlparser.schema.Column> columns, List<Expression> expressions, List<Object> parameters, Table table) {
        return new SQLRecordFunction(table, columns, expressions, parameters);
    }

    private Predicate buildPredicate(Expression where, Table table) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

        int index = 0;
        for (SelectItem c : selectBody.getSelectItems()) {
            if (!(c instanceof AllColumns)) {
                throw new StatementExecutionException("unsupported select " + c.getClass() + " " + c);
            }
        }
        long whereParamters = 0;
        Predicate where = null;
        Bytes key = null;
        if (selectBody.getWhere() instanceof EqualsTo) {
            // SELECT * FROM WHERE KEY=?
            EqualsTo e = (EqualsTo) selectBody.getWhere();
            net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
            if (!column.getColumnName().equals(table.primaryKeyColumn)) {
                throw new StatementExecutionException("unsupported where, only on primary key field " + table.primaryKeyColumn);
            }
            JdbcParameter jdbcparam = (JdbcParameter) e.getRightExpression();
            key = new Bytes(
                    RecordSerializer.serialize(
                            parameters.get((int) whereParamters),
                            table.getColumn(table.primaryKeyColumn).type));
        } else {
            // SELECT * FROM WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + selectBody.getWhere());
        }

        try {
            return new GetStatement(tableSpace, tableName, key, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }
}
