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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexSeek;
import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexRangeScan;
import herddb.index.SecondaryIndexSeek;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Aggregator;
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.ColumnsList;
import herddb.model.DMLStatement;
import herddb.model.ExecutionPlan;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.RecordFunction;
import herddb.model.ScanLimits;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TupleComparator;
import herddb.model.TuplePredicate;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.DropIndexStatement;
import herddb.model.commands.DropTableSpaceStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.IntHolder;
import herddb.utils.SQLUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.logging.Logger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class SQLPlanner {

    private final DBManager manager;
    private final PlansCache cache;

    public void clearCache() {
        cache.clear();
    }

    public SQLPlanner(DBManager manager) {
        this.manager = manager;
        this.cache = new PlansCache();
    }

    private static String rewriteExecuteSyntax(String query) {
        int idx = SQLUtils.findQueryStart(query);

        //* No match at all. Only ignorable charaters and comments */
        if (idx == -1) {
            return query;
        }

        char ch = query.charAt(idx);

        /* "empty" data skipped now we must recognize instructions to rewrite */
        switch (ch) {
            /* ALTER */
            case 'A':
                if (query.regionMatches(idx, "ALTER TABLESPACE ", 0, 17)) {
                    return "EXECUTE altertablespace " + query.substring(idx + 17);
                }

                return query;

            /* BEGIN */
            case 'B':
                if (query.regionMatches(idx, "BEGIN TRANSACTION", 0, 17)) {
                    return "EXECUTE begintransaction" + query.substring(idx + 17);
                }

                return query;

            /* COMMIT / CREATE */
            case 'C':
                ch = query.charAt(idx + 1);
                switch (ch) {
                    case 'O':
                        if (query.regionMatches(idx, "COMMIT TRANSACTION", 0, 18)) {
                            return "EXECUTE committransaction" + query.substring(idx + 18);
                        }

                        break;

                    case 'R':
                        if (query.regionMatches(idx, "CREATE TABLESPACE ", 0, 18)) {
                            return "EXECUTE createtablespace " + query.substring(idx + 18);
                        }

                        break;
                }

                return query;

            /* DROP */
            case 'D':
                if (query.regionMatches(idx, "DROP TABLESPACE ", 0, 16)) {
                    return "EXECUTE droptablespace " + query.substring(idx + 16);
                }

                return query;

            /* ROLLBACK */
            case 'R':
                if (query.regionMatches(idx, "ROLLBACK TRANSACTION", 0, 20)) {
                    return "EXECUTE rollbacktransaction" + query.substring(idx + 20);
                }

                return query;

            default:

                return query;
        }
    }

    public TranslatedQuery translate(String defaultTableSpace, String query, List<Object> parameters,
        boolean scan, boolean allowCache, boolean returnValues) throws StatementExecutionException {
        if (parameters == null) {
            parameters = Collections.emptyList();
        }
        query = rewriteExecuteSyntax(query);
        String cacheKey = "scan:" + scan
            + ",defaultTableSpace:" + defaultTableSpace
            + ", query:" + query
            + ",returnValues:" + returnValues;
        if (allowCache) {
            ExecutionPlan cached = cache.get(cacheKey);
            if (cached != null) {
                return new TranslatedQuery(cached, new SQLStatementEvaluationContext(query, parameters));
            }
        }
        try {
            ExecutionPlan result;
            net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(query);
            assignJdbcParametersIndexes(stmt);
            if (stmt instanceof CreateTable) {
                result = ExecutionPlan.simple(buildCreateTableStatement(defaultTableSpace, (CreateTable) stmt));
            } else if (stmt instanceof CreateIndex) {
                result = ExecutionPlan.simple(buildCreateIndexStatement(defaultTableSpace, (CreateIndex) stmt));
            } else if (stmt instanceof Insert) {
                result = buildInsertStatement(defaultTableSpace, (Insert) stmt, returnValues);
            } else if (stmt instanceof Delete) {
                result = buildDeleteStatement(defaultTableSpace, (Delete) stmt, returnValues);
            } else if (stmt instanceof Update) {
                result = buildUpdateStatement(defaultTableSpace, (Update) stmt, returnValues);
            } else if (stmt instanceof Select) {
                result = buildSelectStatement(defaultTableSpace, (Select) stmt, scan);
            } else if (stmt instanceof Execute) {
                result = ExecutionPlan.simple(buildExecuteStatement(defaultTableSpace, (Execute) stmt));
                allowCache = false;
            } else if (stmt instanceof Alter) {
                result = ExecutionPlan.simple(buildAlterStatement(defaultTableSpace, (Alter) stmt));
                allowCache = false;
            } else if (stmt instanceof Drop) {
                result = ExecutionPlan.simple(buildDropStatement(defaultTableSpace, (Drop) stmt));
                allowCache = false;
            } else {
                throw new StatementExecutionException("unable to execute query " + query + ", type " + stmt.getClass());
            }
            if (allowCache) {
                cache.put(cacheKey, result);
            }
            return new TranslatedQuery(result, new SQLStatementEvaluationContext(query, parameters));
        } catch (JSQLParserException | net.sf.jsqlparser.parser.TokenMgrError err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }

    }

    private Statement buildCreateTableStatement(String defaultTableSpace, CreateTable s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        try {
            boolean foundPk = false;
            Table.Builder tablebuilder = Table.builder().name(tableName).tablespace(tableSpace);
            Set<String> primaryKey = new HashSet<>();

            if (s.getIndexes() != null) {
                for (Index index : s.getIndexes()) {
                    if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                        for (String n : index.getColumnsNames()) {
                            n = n.toLowerCase();
                            tablebuilder.primaryKey(n);
                            primaryKey.add(n);
                            foundPk = true;
                        }
                    }
                }
            }

            int position = 0;
            for (ColumnDefinition cf : s.getColumnDefinitions()) {
                String columnName = cf.getColumnName().toLowerCase();
                int type;
                String dataType = cf.getColDataType().getDataType();
                type = sqlDataTypeToColumnType(dataType);
                tablebuilder.column(columnName, type, position++);

                if (cf.getColumnSpecStrings() != null) {
                    List<String> columnSpecs = cf.getColumnSpecStrings().stream().map(String::toUpperCase).collect(Collectors.toList());
                    boolean auto_increment = columnSpecs.contains("AUTO_INCREMENT");
                    if (columnSpecs.contains("PRIMARY")) {
                        foundPk = true;
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                    if (auto_increment && primaryKey.contains(cf.getColumnName())) {
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                }
            }

            if (!foundPk) {
                tablebuilder.column("_pk", ColumnTypes.LONG, position++);
                tablebuilder.primaryKey("_pk", true);
            }

            Table table = tablebuilder.build();
            List<herddb.model.Index> otherIndexes = new ArrayList<>();
            if (s.getIndexes() != null) {
                for (Index index : s.getIndexes()) {
                    if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {

                    } else if (index.getType().equalsIgnoreCase("INDEX")) {
                        String indexName = index.getName().toLowerCase();
                        String indexType = convertIndexType(null);

                        herddb.model.Index.Builder builder = herddb.model.Index
                            .builder()
                            .name(indexName)
                            .type(indexType)
                            .table(tableName)
                            .tablespace(tableSpace);

                        for (String columnName : index.getColumnsNames()) {
                            columnName = columnName.toLowerCase();
                            Column column = table.getColumn(columnName);
                            if (column == null) {
                                throw new StatementExecutionException("no such column " + columnName + " on table " + tableName + " in tablespace " + tableSpace);
                            }
                            builder.column(column.name, column.type);
                        }

                        otherIndexes.add(builder.build());
                    }
                }
            }

            CreateTableStatement statement = new CreateTableStatement(table, otherIndexes);
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition: " + err.getMessage(), err);
        }
    }

    private Statement buildCreateIndexStatement(String defaultTableSpace, CreateIndex s) throws StatementExecutionException {
        try {
            String tableSpace = s.getTable().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String tableName = s.getTable().getName().toLowerCase();

            String indexName = s.getIndex().getName().toLowerCase();
            String indexType = convertIndexType(s.getIndex().getType());

            herddb.model.Index.Builder builder = herddb.model.Index
                .builder()
                .name(indexName)
                .type(indexType)
                .table(tableName)
                .tablespace(tableSpace);

            AbstractTableManager tableDefinition = manager.getTableSpaceManager(tableSpace).getTableManager(tableName);
            if (tableDefinition == null) {
                throw new TableDoesNotExistException("no such table " + tableName + " in tablespace " + tableSpace);
            }
            for (String columnName : s.getIndex().getColumnsNames()) {
                columnName = columnName.toLowerCase();
                Column column = tableDefinition.getTable().getColumn(columnName);
                if (column == null) {
                    throw new StatementExecutionException("no such column " + columnName + " on table " + tableName + " in tablespace " + tableSpace);
                }
                builder.column(column.name, column.type);
            }

            CreateIndexStatement statement = new CreateIndexStatement(builder.build());
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad index definition: " + err.getMessage(), err);
        }
    }

    private String convertIndexType(String indexType) throws StatementExecutionException {
        if (indexType == null) {
            indexType = herddb.model.Index.TYPE_BRIN;
        } else {
            indexType = indexType.toLowerCase();
        }
        switch (indexType) {
            case herddb.model.Index.TYPE_HASH:
            case herddb.model.Index.TYPE_BRIN:
                break;
            default:
                throw new StatementExecutionException("Invalid index type " + indexType);
        }
        return indexType;
    }

    private int sqlDataTypeToColumnType(String dataType) throws StatementExecutionException {
        int type;
        switch (dataType.toLowerCase()) {
            case "string":
            case "varchar":
            case "nvarchar":
            case "nvarchar2":
            case "nclob":
            case "clob":
                type = ColumnTypes.STRING;
                break;
            case "long":
            case "bigint":
                type = ColumnTypes.LONG;
                break;
            case "int":
            case "integer":
            case "tinyint":
            case "smallint":
                type = ColumnTypes.INTEGER;
                break;
            case "bytea":
            case "blob":
            case "image":
                type = ColumnTypes.BYTEARRAY;
                break;
            case "timestamp":
            case "timestamptz":
            case "datetime":
                type = ColumnTypes.TIMESTAMP;
                break;
            default:
                throw new StatementExecutionException("bad type " + dataType);
        }
        return type;
    }

    private ExecutionPlan buildInsertStatement(String defaultTableSpace, Insert s, boolean returnValues) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablespace " + tableSpace);
        }
        Table table = tableManager.getTable();

        List<Expression> keyValueExpression = new ArrayList<>();
        List<String> keyExpressionToColumn = new ArrayList<>();

        List<Expression> valuesExpressions = new ArrayList<>();
        List<net.sf.jsqlparser.schema.Column> valuesColumns = new ArrayList<>();

        int index = 0;

        ItemsList itemlist = (ItemsList) s.getItemsList();
        if (itemlist != null) {
            if (itemlist instanceof MultiExpressionList) {
                throw new StatementExecutionException("multi values insert is not supported yet");
            }
            ExpressionList list = (ExpressionList) itemlist;
            if (s.getColumns() != null) {
                for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
                    Column column = table.getColumn(c.getColumnName());
                    if (column == null) {
                        throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
                    }
                    Expression expression;
                    try {
                        expression = list.getExpressions().get(index++);
                    } catch (IndexOutOfBoundsException badQuery) {
                        throw new StatementExecutionException("bad number of VALUES in INSERT clause");
                    }

                    if (table.isPrimaryKeyColumn(column.name)) {
                        keyExpressionToColumn.add(column.name);
                        keyValueExpression.add(expression);

                    }
                    valuesColumns.add(c);
                    valuesExpressions.add(expression);
                }
            } else {
                for (Column column : table.columns) {

                    Expression expression = list.getExpressions().get(index++);
                    if (table.isPrimaryKeyColumn(column.name)) {
                        keyExpressionToColumn.add(column.name);
                        keyValueExpression.add(expression);
                    }
                    valuesColumns.add(new net.sf.jsqlparser.schema.Column(column.name));
                    valuesExpressions.add(expression);
                }
            }

            RecordFunction keyfunction;
            if (keyValueExpression.isEmpty() && table.auto_increment) {
                keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
            } else {
                if (keyValueExpression.size() != table.primaryKey.length) {
                    throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                }
                keyfunction = new SQLRecordKeyFunction(table, keyExpressionToColumn, keyValueExpression);
            }
            RecordFunction valuesfunction = new SQLRecordFunction(table, valuesColumns, valuesExpressions, 0);

            try {
                return ExecutionPlan.simple(new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction).setReturnValues(returnValues));
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        } else {
            Select select = s.getSelect();
            ExecutionPlan datasource = buildSelectStatement(defaultTableSpace, select, true);
            if (s.getColumns() == null) {
                throw new StatementExecutionException("for INSERT ... SELECT you have to declare the columns to be filled in (use INSERT INTO TABLE(c,c,c,) SELECT .....)");
            }
            IntHolder holder = new IntHolder(0);
            for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
                Column column = table.getColumn(c.getColumnName());
                if (column == null) {
                    throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
                }
                JdbcParameter readFromResultSetAsJdbcParameter = new JdbcParameter();
                readFromResultSetAsJdbcParameter.setIndex(holder.value++);

                if (table.isPrimaryKeyColumn(column.name)) {
                    keyExpressionToColumn.add(column.name);
                    keyValueExpression.add(readFromResultSetAsJdbcParameter);
                }
                valuesColumns.add(c);
                valuesExpressions.add(readFromResultSetAsJdbcParameter);
            }

            RecordFunction keyfunction;
            if (keyValueExpression.isEmpty() && table.auto_increment) {
                keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
            } else {
                if (keyValueExpression.size() != table.primaryKey.length) {
                    throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                }
                keyfunction = new SQLRecordKeyFunction(table, keyExpressionToColumn, keyValueExpression);
            }
            RecordFunction valuesfunction = new SQLRecordFunction(table, valuesColumns, valuesExpressions, 0);

            try {
                return ExecutionPlan.dataManipulationFromSelect(
                    new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction)
                        .setReturnValues(returnValues),
                    datasource);
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        }
    }

    private ExecutionPlan buildDeleteStatement(String defaultTableSpace, Delete s, boolean returnValues) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) s.getTable();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablespace " + tableSpace);
        }
        Table table = tableManager.getTable();

        // Perform a scan and then delete each row
        Predicate where = s.getWhere() != null ? new SQLRecordPredicate(table, table.name, s.getWhere()) : null;
        if (where != null) {
            SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(s.getWhere(), table, table.name);
            if (keyFunction != null) {
                if (keyFunction.isFullPrimaryKey()) {
                    where.setIndexOperation(new PrimaryIndexSeek(keyFunction));
                } else {
                    where.setIndexOperation(new PrimaryIndexPrefixScan(keyFunction));
                }
            } else {
                Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
                if (indexes != null) {
                    // TODO: use some kink of statistics, maybe using an index is more expensive than a full table scan
                    for (AbstractIndexManager index : indexes.values()) {
                        if (!index.isAvailable()) {
                            continue;
                        }
                        String[] columnsToMatch = index.getColumnNames();
                        SQLRecordKeyFunction indexSeekFunction = findIndexAccess(s.getWhere(), columnsToMatch,
                            index.getIndex(),
                            table.name,
                            EqualsTo.class
                        );
                        if (indexSeekFunction != null) {
                            if (indexSeekFunction.isFullPrimaryKey()) {
                                where.setIndexOperation(new SecondaryIndexSeek(index.getIndexName(), columnsToMatch, indexSeekFunction));
                            } else {
                                where.setIndexOperation(new SecondaryIndexPrefixScan(index.getIndexName(), columnsToMatch, indexSeekFunction));
                            }
                            break;

                        } else {
                            SQLRecordKeyFunction rangeMin = findIndexAccess(s.getWhere(), columnsToMatch,
                                index.getIndex(),
                                table.name, GreaterThanEquals.class
                            );
                            if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                                rangeMin = null;

                            }
                            if (rangeMin == null) {
                                rangeMin = findIndexAccess(s.getWhere(), columnsToMatch,
                                    index.getIndex(),
                                    table.name, GreaterThan.class
                                );
                                if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                                    rangeMin = null;

                                }
                            }

                            SQLRecordKeyFunction rangeMax = findIndexAccess(s.getWhere(), columnsToMatch,
                                index.getIndex(),
                                table.name, MinorThanEquals.class
                            );
                            if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                                rangeMax = null;

                            }
                            if (rangeMax == null) {
                                rangeMax = findIndexAccess(s.getWhere(), columnsToMatch,
                                    index.getIndex(),
                                    table.name, MinorThan.class
                                );
                                if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                                    rangeMax = null;
                                }
                            }

                            if (rangeMin != null || rangeMax != null) {
                                where.setIndexOperation(new SecondaryIndexRangeScan(index.getIndexName(), columnsToMatch, rangeMin, rangeMax));
                                break;
                            }

                        }
                    }
                }
            }
        }

        DMLStatement st = new DeleteStatement(tableSpace, tableName, null, where).setReturnValues(returnValues);
        return ExecutionPlan.simple(st);

    }

    private ExecutionPlan buildUpdateStatement(String defaultTableSpace, Update s,
        boolean returnValues) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) s.getTables().get(0);
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablespace " + tableSpace);
        }
        Table table = tableManager.getTable();
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
            }
        }

        RecordFunction function = new SQLRecordFunction(table, s.getColumns(), s.getExpressions(), 0);

        // Perform a scan and then update each row
        Predicate where = s.getWhere() != null ? new SQLRecordPredicate(table, table.name, s.getWhere()) : null;
        if (where != null) {
            SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(s.getWhere(), table, table.name);
            if (keyFunction != null) {
                if (keyFunction.isFullPrimaryKey()) {
                    where.setIndexOperation(new PrimaryIndexSeek(keyFunction));
                } else {
                    where.setIndexOperation(new PrimaryIndexPrefixScan(keyFunction));
                }
            } else {
                Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
                if (indexes != null) {
                    // TODO: use some kink of statistics, maybe using an index is more expensive than a full table scan
                    for (AbstractIndexManager index : indexes.values()) {
                        if (!index.isAvailable()) {
                            continue;
                        }
                        String[] columnsToMatch = index.getColumnNames();
                        SQLRecordKeyFunction indexSeekFunction = findIndexAccess(s.getWhere(), columnsToMatch,
                            index.getIndex(),
                            table.name,
                            EqualsTo.class
                        );
                        if (indexSeekFunction != null) {
                            if (indexSeekFunction.isFullPrimaryKey()) {
                                where.setIndexOperation(new SecondaryIndexSeek(index.getIndexName(), columnsToMatch, indexSeekFunction));
                            } else {
                                where.setIndexOperation(new SecondaryIndexPrefixScan(index.getIndexName(), columnsToMatch, indexSeekFunction));
                            }
                            break;

                        } else {
                            SQLRecordKeyFunction rangeMin = findIndexAccess(s.getWhere(), columnsToMatch,
                                index.getIndex(),
                                table.name, GreaterThanEquals.class
                            );
                            if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                                rangeMin = null;

                            }
                            if (rangeMin == null) {
                                rangeMin = findIndexAccess(s.getWhere(), columnsToMatch,
                                    index.getIndex(),
                                    table.name, GreaterThan.class
                                );
                                if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                                    rangeMin = null;

                                }
                            }

                            SQLRecordKeyFunction rangeMax = findIndexAccess(s.getWhere(), columnsToMatch,
                                index.getIndex(),
                                table.name, MinorThanEquals.class
                            );
                            if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                                rangeMax = null;

                            }
                            if (rangeMax == null) {
                                rangeMax = findIndexAccess(s.getWhere(), columnsToMatch,
                                    index.getIndex(),
                                    table.name, MinorThan.class
                                );
                                if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                                    rangeMax = null;
                                }
                            }

                            if (rangeMin != null || rangeMax != null) {
                                where.setIndexOperation(new SecondaryIndexRangeScan(index.getIndexName(), columnsToMatch, rangeMin, rangeMax));
                                break;
                            }

                        }
                    }
                }
            }
        }
        DMLStatement st = new UpdateStatement(tableSpace, tableName, null, function, where)
            .setReturnValues(returnValues);
        return ExecutionPlan.simple(st);

    }

    private Predicate buildSimplePredicate(Expression where, Table table, String tableAlias) {
        if (where instanceof EqualsTo || where == null) {
            // surely this is the only predicate on the PK, we can skip it
            return null;
        }
        return new SQLRecordPredicate(table, tableAlias, where);

    }

    private Expression findConstraintOnColumn(Expression where, String columnName, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            Expression keyOnLeft = findConstraintOnColumn(and.getLeftExpression(), columnName, tableAlias, expressionType);
            if (keyOnLeft != null) {
                return keyOnLeft;
            }

            Expression keyOnRight = findConstraintOnColumn(and.getRightExpression(), columnName, tableAlias,
                expressionType);
            if (keyOnRight != null) {
                return keyOnRight;
            }
        } else if (expressionType.equals(where.getClass())) {
            Expression keyDirect = validateColumnConstaintToExpression(where, columnName, tableAlias, expressionType);
            if (keyDirect != null) {
                return keyDirect;
            }
        }

        return null;

    }

    private SQLRecordKeyFunction findPrimaryKeyIndexSeek(Expression where, Table table, String tableAlias) throws StatementExecutionException {
        return findIndexAccess(where, table.primaryKey, table, tableAlias, EqualsTo.class
        );
    }

    private SQLRecordKeyFunction findIndexAccess(Expression where, String[] columnsToMatch, ColumnsList table, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        List<Expression> expressions = new ArrayList<>();
        List<String> columns = new ArrayList<>();

        for (String pk : columnsToMatch) {
            Expression condition = findConstraintOnColumn(where, pk, tableAlias, expressionType);
            if (condition == null) {
                break;
            }
            columns.add(pk);
            expressions.add(condition);

        }
        if (expressions.isEmpty()) {
            // no match at all, there is no direct constraint on PK
            return null;
        }
        return new SQLRecordKeyFunction(table, columns, expressions);
    }

    private Object resolveValue(Expression expression) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            throw new StatementExecutionException("jdbcparameter expression not usable in this query");
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else if (expression instanceof TimestampValue) {
            return ((TimestampValue) expression).getValue();
        } else if (expression instanceof SignedExpression) {
            SignedExpression se = (SignedExpression) expression;
            switch (se.getSign()) {
                case '+': {
                    return resolveValue(se.getExpression());
                }
                case '-': {
                    Object value = resolveValue(se.getExpression());
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof Integer) {
                        return -1L * ((Integer) value);
                    } else if (value instanceof Long) {
                        return -1L * ((Long) value);
                    } else {
                        throw new StatementExecutionException("unsupported value type " + expression.getClass() + " with sign " + se.getSign() + " on value " + value + " of type " + value.getClass());
                    }
                }
                default:
                    throw new StatementExecutionException("unsupported value type " + expression.getClass() + " with sign " + se.getSign());
            }

        } else {
            throw new StatementExecutionException("unsupported value type " + expression.getClass());
        }
    }

    private Expression validateColumnConstaintToExpression(Expression testExpression, String columnName, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        Expression result = null;
        if (expressionType.equals(testExpression.getClass())) {
            BinaryExpression e = (BinaryExpression) testExpression;
            if (e.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
                boolean okAlias = true;
                if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equalsIgnoreCase(tableAlias)) {
                    okAlias = false;
                }
                if (okAlias && columnName.equalsIgnoreCase(c.getColumnName()) && SQLRecordPredicate.isConstant(e.getRightExpression())) {
                    return e.getRightExpression();
                }
            } else if (e.getLeftExpression() instanceof AndExpression) {
                result = findConstraintOnColumn((AndExpression) e.getLeftExpression(), columnName, tableAlias, expressionType);
                if (result != null) {
                    return result;
                }
            } else if (e.getRightExpression() instanceof AndExpression) {
                result = findConstraintOnColumn((AndExpression) e.getRightExpression(), columnName, tableAlias, expressionType);
                if (result != null) {
                    return result;
                }
            }
        }
        return result;
    }

    private TableAliasDiscovery discoverMainTableAlias(Expression expression) throws StatementExecutionException {
        TableAliasDiscovery discovery = new TableAliasDiscovery();
        expression.accept(discovery);
        return discovery;
    }

    private Expression collectConditionsForAlias(String alias, Expression expression, List<Expression> conditionsOnJoinedResult) throws StatementExecutionException {
        if (expression == null) {
            // no constraint on table
            return null;
        }
        TableAliasDiscovery discoveredMainAlias = discoverMainTableAlias(expression);
        String mainAlias = discoveredMainAlias.getMainTableAlias();
        if (!discoveredMainAlias.isContainsMixedAliases() && alias.equals(mainAlias)) {
            return expression;
        } else if (expression instanceof AndExpression) {
            AndExpression be = (AndExpression) expression;
            TableAliasDiscovery discoveredMainAliasLeft = discoverMainTableAlias(be.getLeftExpression());
            String mainAliasLeft = discoveredMainAliasLeft.isContainsMixedAliases()
                ? null : discoveredMainAliasLeft.getMainTableAlias();

            TableAliasDiscovery discoveredMainAliasright = discoverMainTableAlias(be.getRightExpression());
            String mainAliasRight = discoveredMainAliasright.isContainsMixedAliases()
                ? null : discoveredMainAliasright.getMainTableAlias();
            if (alias.equals(mainAliasLeft)) {
                if (alias.equals(mainAliasRight)) {
                    return expression;
                } else {
                    return be.getLeftExpression();
                }
            } else if (alias.equals(mainAliasRight)) {
                return be.getRightExpression();
            } else {
                // no constraint on table
                return null;
            }
        } else {
            conditionsOnJoinedResult.add(expression);
            return null;
        }
    }

    private Expression composeAndExpression(List<Expression> conditionsOnJoinedResult) {
        if (conditionsOnJoinedResult.size() == 1) {
            return conditionsOnJoinedResult.get(0);
        }
        AndExpression result = result = new AndExpression(conditionsOnJoinedResult.get(0), conditionsOnJoinedResult.get(1));
        for (int i = 2; i < conditionsOnJoinedResult.size(); i++) {
            result = new AndExpression(result, conditionsOnJoinedResult.get(i));
        }
        return result;
    }

    private static class JoinSupport {

        final TableRef tableRef;
        final AbstractTableManager tableManager;
        final Table table;
        Projection projection;
        List<SelectItem> selectItems = new ArrayList<>();
        boolean allColumns;

        public JoinSupport(TableRef tableRef, AbstractTableManager tableManager) {
            this.tableRef = tableRef;
            this.tableManager = tableManager;
            this.table = tableManager.getTable();
        }

    }

    private ExecutionPlan buildSelectStatement(String defaultTableSpace, Select s, boolean scan) throws StatementExecutionException {
        PlainSelect selectBody = (PlainSelect) s.getSelectBody();
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) selectBody.getFromItem();

        TableRef mainTable = TableRef.buildFrom(fromTable, defaultTableSpace);
        String mainTableAlias = mainTable.tableAlias;
        String tableSpace = mainTable.tableSpace;
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new TableSpaceDoesNotExistException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(mainTable.tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no such table " + mainTable.tableName + " in tablespace " + tableSpace);
        }

        // linked hash map retains the order of insertions
        LinkedHashMap<String, JoinSupport> joins = new LinkedHashMap<>();
        boolean joinPresent = false;
        joins.put(mainTable.tableAlias, new JoinSupport(mainTable, tableManager));

        if (selectBody.getJoins() != null) {
            for (Join join : selectBody.getJoins()) {
                joinPresent = true;
                if (join.isLeft()
                    || join.isCross()
                    || join.isNatural()
                    || join.isRight()
                    || join.isOuter()
                    || join.isSimple()) {
                    throw new StatementExecutionException("unsupported JOIN type: " + join);
                }                
                net.sf.jsqlparser.schema.Table joinedTable = (net.sf.jsqlparser.schema.Table) join.getRightItem();
                TableRef joinedTableRef = TableRef.buildFrom(joinedTable, defaultTableSpace);
                if (!joinedTableRef.tableSpace.equalsIgnoreCase(mainTable.tableSpace)) {
                    throw new TableDoesNotExistException("unsupported cross-tablespace JOIN "
                        + "between" + mainTable.tableSpace + "." + mainTable.tableName
                        + " and " + joinedTableRef.tableSpace + "." + joinedTableRef.tableName);
                }
                AbstractTableManager joinedTableManager = tableSpaceManager.getTableManager(joinedTableRef.tableName);
                if (joinedTableManager == null) {
                    throw new TableDoesNotExistException("no such table " + joinedTableRef.tableName + " in tablespace " + tableSpace);
                }
                JoinSupport joinSupport = new JoinSupport(joinedTableRef, joinedTableManager);
                joins.put(joinedTableRef.tableAlias, joinSupport);
            }
        }

        Projection mainTableProjection;
        Table table = tableManager.getTable();
        boolean allColumns = false;
        boolean containsAggregateFunctions = false;
        for (SelectItem c : selectBody.getSelectItems()) {
            if (c instanceof AllColumns) {
                allColumns = true;
                break;
            } else if (c instanceof AllTableColumns) {
                AllTableColumns allTableColumns = (AllTableColumns) c;
                TableRef ref = TableRef.buildFrom(allTableColumns.getTable(), defaultTableSpace);
                if (ref.tableAlias.equals(mainTable.tableAlias) && !joinPresent) {
                    // select a.*  FROM table a
                    allColumns = true;
                } else {
                    // select a.*, b.* FROM table a JOIN table b
                    joins.get(ref.tableAlias).allColumns = true;
                }
            } else if (c instanceof SelectExpressionItem) {
                SelectExpressionItem se = (SelectExpressionItem) c;
                if (isAggregateFunction(se.getExpression())) {
                    containsAggregateFunctions = true;
                }
                if (!joinPresent) {
                    joins.get(mainTable.tableAlias).selectItems.add(c);
                } else {
                    TableAliasDiscovery discoverMainTableAlias = discoverMainTableAlias(se.getExpression());
                    String mainTableAliasForItem = discoverMainTableAlias.getMainTableAlias();
                    if (discoverMainTableAlias.isContainsMixedAliases()) {
                        throw new StatementExecutionException("unsupported single SELECT ITEM with mixed aliases: " + c);
                    }
                    if (mainTableAliasForItem == null) {
                        mainTableAliasForItem = mainTable.tableAlias;
                    }
                    joins.get(mainTableAliasForItem).selectItems.add(c);
                }
            } else {
                throw new StatementExecutionException("unsupported SELECT ITEM type: " + c);
            }
        }

        if (allColumns) {
            mainTableProjection = Projection.IDENTITY(table.columns);
            for (Map.Entry<String, JoinSupport> join : joins.entrySet()) {
                JoinSupport support = join.getValue();
                support.projection = Projection.IDENTITY(support.table.columns);
            }
        } else {
            if (!joinPresent) {
                mainTableProjection = new SQLProjection(table, mainTableAlias, selectBody.getSelectItems());
            } else {
                for (JoinSupport support : joins.values()) {
                    if (support.allColumns) {
                        support.projection = Projection.IDENTITY(support.table.columns);
                    } else {
                        support.projection = new SQLProjection(support.table, support.tableRef.tableAlias, support.selectItems);
                    }
                }
                mainTableProjection = joins.get(mainTableAlias).projection;
            }
        }
        if (scan) {
            if (!joinPresent) {
                Predicate where = selectBody.getWhere() != null ? new SQLRecordPredicate(table, mainTableAlias, selectBody.getWhere()) : null;
                if (where != null) {
                    SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(selectBody.getWhere(), table, mainTableAlias);
                    if (keyFunction != null) {
                        if (keyFunction.isFullPrimaryKey()) {
                            where.setIndexOperation(new PrimaryIndexSeek(keyFunction));
                        } else {
                            where.setIndexOperation(new PrimaryIndexPrefixScan(keyFunction));
                        }
                    } else {
                        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
                        if (indexes != null) {
                            // TODO: use some kink of statistics, maybe using an index is more expensive than a full table scan
                            for (AbstractIndexManager index : indexes.values()) {
                                if (!index.isAvailable()) {
                                    continue;
                                }
                                String[] columnsToMatch = index.getColumnNames();
                                SQLRecordKeyFunction indexSeekFunction = findIndexAccess(selectBody.getWhere(), columnsToMatch,
                                    index.getIndex(),
                                    table.name,
                                    EqualsTo.class
                                );
                                if (indexSeekFunction != null) {
                                    if (indexSeekFunction.isFullPrimaryKey()) {
                                        where.setIndexOperation(new SecondaryIndexSeek(index.getIndexName(), columnsToMatch, indexSeekFunction));
                                    } else {
                                        where.setIndexOperation(new SecondaryIndexPrefixScan(index.getIndexName(), columnsToMatch, indexSeekFunction));
                                    }
                                    break;

                                } else {
                                    SQLRecordKeyFunction rangeMin = findIndexAccess(selectBody.getWhere(), columnsToMatch,
                                        index.getIndex(),
                                        table.name, GreaterThanEquals.class
                                    );
                                    if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                                        rangeMin = null;

                                    }
                                    if (rangeMin == null) {
                                        rangeMin = findIndexAccess(selectBody.getWhere(), columnsToMatch,
                                            index.getIndex(),
                                            table.name, GreaterThan.class
                                        );
                                        if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                                            rangeMin = null;

                                        }
                                    }

                                    SQLRecordKeyFunction rangeMax = findIndexAccess(selectBody.getWhere(), columnsToMatch,
                                        index.getIndex(),
                                        table.name, MinorThanEquals.class
                                    );
                                    if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                                        rangeMax = null;

                                    }
                                    if (rangeMax == null) {
                                        rangeMax = findIndexAccess(selectBody.getWhere(), columnsToMatch,
                                            index.getIndex(),
                                            table.name, MinorThan.class
                                        );
                                        if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                                            rangeMax = null;
                                        }
                                    }

                                    if (rangeMin != null || rangeMax != null) {
                                        where.setIndexOperation(new SecondaryIndexRangeScan(index.getIndexName(), columnsToMatch, rangeMin, rangeMax));
                                        break;
                                    }

                                }
                            }
                        }
                    }
                }

                Aggregator aggregator = null;
                ScanLimits limitsOnScan = null;
                ScanLimits limitsOnPlan = null;
                if (containsAggregateFunctions || (selectBody.getGroupByColumnReferences() != null && !selectBody.getGroupByColumnReferences().isEmpty())) {
                    aggregator = new SQLAggregator(selectBody.getSelectItems(), selectBody.getGroupByColumnReferences(), manager.getRecordSetFactory());
                }

                TupleComparator comparatorOnScan = null;
                TupleComparator comparatorOnPlan = null;
                if (selectBody.getOrderByElements() != null && !selectBody.getOrderByElements().isEmpty()) {
                    if (aggregator != null) {
                        comparatorOnPlan = new SQLTupleComparator(mainTableAlias, selectBody.getOrderByElements());;
                    } else {
                        comparatorOnScan = new SQLTupleComparator(mainTableAlias, selectBody.getOrderByElements());
                    }
                }

                Limit limit = selectBody.getLimit();
                Top top = selectBody.getTop();
                if (limit != null && top != null) {
                    throw new StatementExecutionException("LIMIT and TOP cannot be used on the same query");
                }
                if (limit != null) {
                    if (limit.isLimitAll() || limit.isLimitNull() || limit.isOffsetJdbcParameter() || limit.isRowCountJdbcParameter()) {
                        throw new StatementExecutionException("Invalid LIMIT clause");
                    }
                    if (aggregator != null) {
                        limitsOnPlan = new ScanLimits((int) limit.getRowCount(), (int) limit.getOffset());
                    } else {
                        limitsOnScan = new ScanLimits((int) limit.getRowCount(), (int) limit.getOffset());
                    }
                } else if (top != null) {
                    if (top.isPercentage() || top.getExpression() == null) {
                        throw new StatementExecutionException("Invalid TOP clause");
                    }
                    try {
                        int rowCount = Integer.parseInt(resolveValue(top.getExpression()) + "");
                        if (aggregator != null) {
                            limitsOnPlan = new ScanLimits((int) rowCount, 0);
                        } else {
                            limitsOnScan = new ScanLimits((int) rowCount, 0);
                        }
                    } catch (NumberFormatException error) {
                        throw new StatementExecutionException("Invalid TOP clause: " + error, error);
                    }
                }

                try {
                    ScanStatement statement = new ScanStatement(tableSpace, mainTable.tableName, mainTableProjection, where, comparatorOnScan, limitsOnScan);
                    return ExecutionPlan.make(statement, aggregator, limitsOnPlan, comparatorOnPlan);
                } catch (IllegalArgumentException err) {
                    throw new StatementExecutionException(err);
                }
            } else {
                if (containsAggregateFunctions || (selectBody.getGroupByColumnReferences() != null && !selectBody.getGroupByColumnReferences().isEmpty())) {
                    throw new StatementExecutionException("AGGREGATEs are not yet supported with JOIN");
                }
                Limit limit = selectBody.getLimit();
                Top top = selectBody.getTop();
                if (limit != null && top != null) {
                    throw new StatementExecutionException("LIMIT and TOP cannot be used on the same query");
                }
                ScanLimits limitsOnPlan = null;
                if (limit != null) {
                    if (limit.isLimitAll() || limit.isLimitNull() || limit.isOffsetJdbcParameter() || limit.isRowCountJdbcParameter()) {
                        throw new StatementExecutionException("Invalid LIMIT clause");
                    }

                    limitsOnPlan = new ScanLimits((int) limit.getRowCount(), (int) limit.getOffset());
                } else if (top != null) {
                    if (top.isPercentage() || top.getExpression() == null) {
                        throw new StatementExecutionException("Invalid TOP clause");
                    }
                    try {
                        int rowCount = Integer.parseInt(resolveValue(top.getExpression()) + "");
                        limitsOnPlan = new ScanLimits((int) rowCount, 0);
                    } catch (NumberFormatException error) {
                        throw new StatementExecutionException("Invalid TOP clause: " + error, error);
                    }
                }

                List<Expression> conditionsOnJoinedResult = new ArrayList<>();
                List<ScanStatement> scans = new ArrayList<>();
                for (Map.Entry<String, JoinSupport> join : joins.entrySet()) {
                    String alias = join.getKey();
                    JoinSupport joinSupport = join.getValue();
                    Expression collectedConditionsForAlias = collectConditionsForAlias(alias, selectBody.getWhere(), conditionsOnJoinedResult);
                    LOG.severe("Collected WHERE for alias " + alias + ": " + collectedConditionsForAlias);
                    Predicate predicate;
                    if (collectedConditionsForAlias == null) {
                        predicate = null;
                    } else {
                        predicate = new SQLRecordPredicate(
                            join.getValue().table, alias, collectedConditionsForAlias);
                    }
                    ScanStatement statement = new ScanStatement(tableSpace,
                        joinSupport.table.name,
                        joinSupport.projection, predicate, null, null);
                    scans.add(statement);
                }
                for (Join join : selectBody.getJoins()) {
                    if (join.getOnExpression() != null) {
                        conditionsOnJoinedResult.add(join.getOnExpression());
                    }
                }
                for (Expression e : conditionsOnJoinedResult) {
                    LOG.severe("Collected WHERE on final JOIN result: " + e);
                }
                TuplePredicate joinedDataResult = null;
                if (!conditionsOnJoinedResult.isEmpty()) {
                    joinedDataResult = new SQLRecordPredicate(null, null, composeAndExpression(conditionsOnJoinedResult));
                }

                try {
                    return ExecutionPlan.joinedScan(scans, joinedDataResult, limitsOnPlan, null);
                } catch (IllegalArgumentException err) {
                    throw new StatementExecutionException(err);
                }

            }
        } else {
            if (selectBody.getWhere() == null) {
                throw new StatementExecutionException("unsupported GET without WHERE");
            }
            if (joinPresent) {
                throw new StatementExecutionException("unsupported GET with JOIN");
            }

            // SELECT * FROM WHERE KEY=? AND ....
            SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(selectBody.getWhere(), table, mainTableAlias);
            if (keyFunction == null || !keyFunction.isFullPrimaryKey()) {
                throw new StatementExecutionException("unsupported GET not on PK, bad where clause: " + selectBody.getWhere() + " (" + selectBody.getWhere().getClass() + ")");
            }
            Predicate where = buildSimplePredicate(selectBody.getWhere(), table, mainTableAlias);
            try {
                return ExecutionPlan.simple(new GetStatement(tableSpace, mainTable.tableName, keyFunction, where));
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        }
    }
    private static final Logger LOG = Logger.getLogger(SQLPlanner.class.getName());

    private Statement buildExecuteStatement(String defaultTableSpace, Execute execute) throws StatementExecutionException {
        switch (execute.getName().toUpperCase()) {
            case "BEGINTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName");
                }
                return new BeginTransactionStatement(tableSpaceName.toString());
            }
            case "COMMITTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1));
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
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1));
                if (transactionId == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new RollbackTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
            }
            case "CREATETABLESPACE": {
                if (execute.getExprList().getExpressions().size() < 1) {
                    throw new StatementExecutionException("CREATETABLESPACE syntax (EXECUTE CREATETABLESPACE tableSpaceName ['leader:LEADERID'],['wait:TIMEOUT'] )");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                String leader = null;
                Set<String> replica = new HashSet<>();
                int expectedreplicacount = 1;
                long maxleaderinactivitytime = 0;
                int wait = 0;
                for (int i = 1; i < execute.getExprList().getExpressions().size(); i++) {
                    String property = (String) resolveValue(execute.getExprList().getExpressions().get(i));
                    int colon = property.indexOf(':');
                    if (colon <= 0) {
                        throw new StatementExecutionException("bad property " + property + " in " + execute + " statement");
                    }
                    String pName = property.substring(0, colon);
                    String value = property.substring(colon + 1);
                    switch (pName.toLowerCase()) {
                        case "leader":
                            leader = value;
                            break;
                        case "replica":
                            replica = Arrays.asList(value.split(",")).stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
                            break;
                        case "wait":
                            wait = Integer.parseInt(value);
                            break;
                        case "expectedreplicacount":
                            try {
                                expectedreplicacount = Integer.parseInt(value.trim());
                                if (expectedreplicacount <= 0) {
                                    throw new StatementExecutionException("invalid expectedreplicacount " + value + " must be positive");
                                }
                            } catch (NumberFormatException err) {
                                throw new StatementExecutionException("invalid expectedreplicacount " + value + ": " + err);
                            }
                            break;
                        case "maxleaderinactivitytime":
                            try {
                                maxleaderinactivitytime = Long.parseLong(value.trim());
                                if (maxleaderinactivitytime < 0) {
                                    throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + " must be positive or zero");
                                }
                            } catch (NumberFormatException err) {
                                throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + ": " + err);
                            }
                            break;
                        default:
                            throw new StatementExecutionException("bad property " + pName);
                    }
                }
                if (leader == null) {
                    leader = this.manager.getNodeId();
                }
                if (replica.isEmpty()) {
                    replica.add(leader);
                }
                return new CreateTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount, wait, maxleaderinactivitytime);
            }
            case "ALTERTABLESPACE": {
                if (execute.getExprList().getExpressions().size() < 2) {
                    throw new StatementExecutionException("ALTERTABLESPACE syntax (EXECUTE ALTERTABLESPACE tableSpaceName,'property:value','property2:value2')");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0));
                try {
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if (tableSpace == null) {
                        throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    Set<String> replica = tableSpace.replicas;
                    String leader = tableSpace.leaderId;
                    int expectedreplicacount = tableSpace.expectedReplicaCount;
                    long maxleaderinactivitytime = tableSpace.maxLeaderInactivityTime;
                    for (int i = 1; i < execute.getExprList().getExpressions().size(); i++) {
                        String property = (String) resolveValue(execute.getExprList().getExpressions().get(i));
                        int colon = property.indexOf(':');
                        if (colon <= 0) {
                            throw new StatementExecutionException("bad property " + property + " in " + execute + " statement");
                        }
                        String pName = property.substring(0, colon);
                        String value = property.substring(colon + 1);
                        switch (pName.toLowerCase()) {
                            case "leader":
                                leader = value;
                                break;
                            case "replica":
                                replica = Arrays.asList(value.split(",")).stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
                                break;
                            case "expectedreplicacount":
                                try {
                                    expectedreplicacount = Integer.parseInt(value.trim());
                                    if (expectedreplicacount <= 0) {
                                        throw new StatementExecutionException("invalid expectedreplicacount " + value + " must be positive");
                                    }
                                } catch (NumberFormatException err) {
                                    throw new StatementExecutionException("invalid expectedreplicacount " + value + ": " + err);
                                }
                                break;
                            case "maxleaderinactivitytime":
                                try {
                                    maxleaderinactivitytime = Long.parseLong(value.trim());
                                    if (maxleaderinactivitytime <= 0) {
                                        throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + " must be positive");
                                    }
                                } catch (NumberFormatException err) {
                                    throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + ": " + err);
                                }
                                break;
                            default:
                                throw new StatementExecutionException("bad property " + pName);
                        }
                    }
                    return new AlterTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount, maxleaderinactivitytime);
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            case "DROPTABLESPACE": {
                if (execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("DROPTABLESPACE syntax (EXECUTE DROPTABLESPACE tableSpaceName)");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0));
                try {
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if (tableSpace == null) {
                        throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    return new DropTableSpaceStatement(tableSpaceName + "");
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            default:
                throw new StatementExecutionException("Unsupported command " + execute.getName());
        }
    }

    private Statement buildAlterStatement(String defaultTableSpace, Alter alter) throws StatementExecutionException {
        if (alter.getTable() == null) {
            throw new StatementExecutionException("missing table name");
        }
        String tableSpace = alter.getTable().getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        List<Column> addColumns = new ArrayList<>();
        List<String> dropColumns = new ArrayList<>();
        String tableName = alter.getTable().getName().toLowerCase();
        if (alter.getAlterExpressions() == null || alter.getAlterExpressions().size() != 1) {
            throw new StatementExecutionException("supported multi-alter operation '" + alter + "'");
        }
        AlterExpression alterExpression = alter.getAlterExpressions().get(0);
        AlterOperation operation = alterExpression.getOperation();
        switch (operation) {
            case ADD:
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                for (AlterExpression.ColumnDataType cl : cols) {
                    Column newColumn = Column.column(cl.getColumnName(), sqlDataTypeToColumnType(cl.getColDataType().getDataType()));
                    addColumns.add(newColumn);
                }
                break;
            case DROP:
                dropColumns.add(alterExpression.getColumnName());
                break;
            default:
                throw new StatementExecutionException("supported alter operation '" + alter + "'");
        }
        return new AlterTableStatement(addColumns, dropColumns, tableName, tableSpace);
    }

    private Statement buildDropStatement(String defaultTableSpace, Drop drop) throws StatementExecutionException {
        if (drop.getType().equalsIgnoreCase("table")) {
            if (drop.getName() == null) {
                throw new StatementExecutionException("missing table name");
            }

            String tableSpace = drop.getName().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String tableName = drop.getName().getName();
            return new DropTableStatement(tableSpace, tableName);
        }
        if (drop.getType().equalsIgnoreCase("index")) {
            if (drop.getName() == null) {
                throw new StatementExecutionException("missing index name");
            }
            String tableSpace = drop.getName().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String indexName = drop.getName().getName();
            return new DropIndexStatement(tableSpace, indexName);
        }

        throw new StatementExecutionException("only DROP TABLE and TABLESPACE is supported, drop type=" + drop.getType() + " is not implemented");
    }

    private boolean isAggregateFunction(Expression expression) throws StatementExecutionException {
        if (!(expression instanceof Function)) {
            return false;
        }
        Function function = (Function) expression;
        String name = function.getName().toLowerCase();
        if (BuiltinFunctions.isAggregateFunction(function.getName())) {
            return true;
        }
        if (BuiltinFunctions.isScalarFunction(function.getName())) {
            return false;
        }
        throw new StatementExecutionException("unsupported function " + name);
    }

    private void assignJdbcParametersIndexes(net.sf.jsqlparser.statement.Statement stmt) {
        stmt.accept(new JdbcParameterIndexAssigner());
    }

}
