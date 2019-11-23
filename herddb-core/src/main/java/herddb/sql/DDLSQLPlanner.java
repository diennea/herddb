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

import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.ExecutionPlan;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropIndexStatement;
import herddb.model.commands.DropTableSpaceStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.TableIntegrityCheckStatement;
import herddb.model.commands.TruncateTableStatement;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.SQLUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.truncate.Truncate;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class DDLSQLPlanner implements AbstractSQLPlanner {

    private final DBManager manager;
    private final PlansCache cache;

    @Override
    public long getCacheSize() {
        return cache.getCacheSize();
    }

    @Override
    public long getCacheHits() {
        return cache.getCacheHits();
    }

    @Override
    public long getCacheMisses() {
        return cache.getCacheMisses();
    }

    @Override
    public void clearCache() {
        cache.clear();
    }

    public DDLSQLPlanner(DBManager manager, long maxPlanCacheSize) {
        this.manager = manager;
        this.cache = new PlansCache(maxPlanCacheSize);
    }

    public static String rewriteExecuteSyntax(String query) {
        char ch = query.charAt(0);

        /* "empty" data skipped now we must recognize instructions to rewrite */
        switch (ch) {
            /* ALTER */
            case 'A':
            case 'a':
                if (query.regionMatches(true, 0, "ALTER TABLESPACE ", 0, 17)) {
                    return "EXECUTE altertablespace " + query.substring(17);
                }

                return query;

            /* BEGIN */
            case 'B':
            case 'b':
                if (query.regionMatches(true, 0, "BEGIN TRANSACTION", 0, 17)) {
                    return "EXECUTE begintransaction" + query.substring(17);
                }

                return query;

            /* COMMIT / CREATE */
            case 'C':
            case 'c':
                ch = query.charAt(1);
                switch (ch) {
                    case 'O':
                    case 'o':
                        if (query.regionMatches(true, 0, "COMMIT TRANSACTION", 0, 18)) {
                            return "EXECUTE committransaction" + query.substring(18);
                        }

                        break;

                    case 'R':
                    case 'r':
                        if (query.regionMatches(true, 0, "CREATE TABLESPACE ", 0, 18)) {
                            return "EXECUTE createtablespace " + query.substring(18);
                        }

                        break;
                }

                return query;

            /* DROP */
            case 'D':
            case 'd':
                if (query.regionMatches(true, 0, "DROP TABLESPACE ", 0, 16)) {
                    return "EXECUTE droptablespace " + query.substring(16);
                }

                return query;

            /* ROLLBACK */
            case 'R':
            case 'r':
                if (query.regionMatches(true, 0, "ROLLBACK TRANSACTION", 0, 20)) {
                    return "EXECUTE rollbacktransaction" + query.substring(20);
                }
                return query;

            /* TRUNCATE */
            case 'T':
            case 't':
                if (query.regionMatches(true, 0, "TRUNCATE", 0, 8)) {
                    return "TRUNCATE" + query.substring(8);
                }
                return query;
            /* TABLE INTEGRITY CHECK */    
            case 'I':
            case 'i':
                if(query.regionMatches(true, 0, "CHECKTABLEINTEGRITY", 0,19)){
                    return "EXECUTE checktableintegrity " + query.substring(19);
                }
                return query;
            default:
                return query;
        }
    }

    @Override
    public TranslatedQuery translate(
            String defaultTableSpace, String query, List<Object> parameters,
            boolean scan, boolean allowCache, boolean returnValues, int maxRows
    ) throws StatementExecutionException {
        if (parameters == null) {
            parameters = Collections.emptyList();
        }

        /* Strips out leading comments */
        int idx = SQLUtils.findQueryStart(query);
        if (idx != -1) {
            query = query.substring(idx);
        }

        query = rewriteExecuteSyntax(query);
        String cacheKey = "scan:" + scan
                + ",defaultTableSpace:" + defaultTableSpace
                + ",query:" + query
                + ",returnValues:" + returnValues
                + ",maxRows:" + maxRows;
        if (allowCache) {
            ExecutionPlan cached = cache.get(cacheKey);
            if (cached != null) {
                return new TranslatedQuery(cached, new SQLStatementEvaluationContext(query, parameters));
            }
        }
        net.sf.jsqlparser.statement.Statement stmt = parseStatement(query);
        if (!isCachable(stmt)) {
            allowCache = false;
        }
        ExecutionPlan executionPlan = plan(defaultTableSpace, stmt, scan, returnValues, maxRows);
        if (allowCache) {
            cache.put(cacheKey, executionPlan);
        }
        return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters));

    }

    private net.sf.jsqlparser.statement.Statement parseStatement(String query) throws StatementExecutionException {
        net.sf.jsqlparser.statement.Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException | net.sf.jsqlparser.parser.TokenMgrError err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }
        return stmt;
    }

    private ExecutionPlan plan(
            String defaultTableSpace, net.sf.jsqlparser.statement.Statement stmt,
            boolean scan, boolean returnValues, int maxRows
    ) {
        verifyJdbcParametersIndexes(stmt);
        ExecutionPlan result;
        if (stmt instanceof CreateTable) {
            result = ExecutionPlan.simple(buildCreateTableStatement(defaultTableSpace, (CreateTable) stmt));
        } else if (stmt instanceof CreateIndex) {
            result = ExecutionPlan.simple(buildCreateIndexStatement(defaultTableSpace, (CreateIndex) stmt));
        } else if (stmt instanceof Execute) {
            result = ExecutionPlan.simple(buildExecuteStatement(defaultTableSpace, (Execute) stmt));
        } else if (stmt instanceof Alter) {
            result = ExecutionPlan.simple(buildAlterStatement(defaultTableSpace, (Alter) stmt));
        } else if (stmt instanceof Drop) {
            result = ExecutionPlan.simple(buildDropStatement(defaultTableSpace, (Drop) stmt));
        } else if (stmt instanceof Truncate) {
            result = ExecutionPlan.simple(buildTruncateStatement(defaultTableSpace, (Truncate) stmt));
        } else {
            return null;
        }
        return result;
    }

    private static boolean isCachable(net.sf.jsqlparser.statement.Statement stmt) {
        if (stmt instanceof Execute) {
            return false;
        } else if (stmt instanceof Alter) {
            return false;
        } else if (stmt instanceof Drop) {
            return false;
        } else {
            return !(stmt instanceof Truncate);
        }
    }

    private Statement buildCreateTableStatement(String defaultTableSpace, CreateTable s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        final boolean isNotExsists = s.isIfNotExists();
        try {
            boolean foundPk = false;
            Table.Builder tablebuilder = Table.builder()
                    .uuid(UUID.randomUUID().toString())
                    .name(tableName)
                    .tablespace(tableSpace);
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
                type = sqlDataTypeToColumnType(dataType, cf.getColDataType().getArgumentsStringList());

                if (cf.getColumnSpecStrings() != null) {
                    List<String> columnSpecs = decodeColumnSpecs(cf.getColumnSpecStrings());
                    boolean auto_increment = decodeAutoIncrement(columnSpecs);
                    if (columnSpecs.contains("PRIMARY")) {
                        foundPk = true;
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                    if (auto_increment && primaryKey.contains(columnName)) {
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }

                    if (String.join("_", columnSpecs).equals("NOT_NULL")) {
                        type = ColumnTypes.getNonNullTypeForPrimitiveType(type);
                    }
                }

                tablebuilder.column(columnName, type, position++);

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
                                .uuid(UUID.randomUUID().toString())
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

            CreateTableStatement statement = new CreateTableStatement(table, otherIndexes, isNotExsists);
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition: " + err.getMessage(), err);
        }
    }

    private boolean decodeAutoIncrement(List<String> columnSpecs) {
        boolean auto_increment = columnSpecs.contains("AUTO_INCREMENT");
        return auto_increment;
    }

    private String decodeRenameTo(List<String> columnSpecs) {
        if (columnSpecs.size() != 3) {
            return null;
        }
        if (!"RENAME".equals(columnSpecs.get(0))) {
            return null;
        }
        if (!"TO".equals(columnSpecs.get(1))) {
            return null;
        }
        return columnSpecs.get(2).toLowerCase();

    }

    private List<String> decodeColumnSpecs(List<String> columnSpecs) {
        if (columnSpecs == null || columnSpecs.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> columnSpecsDecoded = columnSpecs.stream().map(String::toUpperCase).collect(Collectors.toList());
        return columnSpecsDecoded;
    }

    private Statement buildCreateIndexStatement(String defaultTableSpace, CreateIndex s) throws StatementExecutionException {
        try {
            String tableSpace = s.getTable().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String tableName = s.getTable().getName();

            String indexName = s.getIndex().getName().toLowerCase();
            String indexType = convertIndexType(s.getIndex().getType());

            herddb.model.Index.Builder builder = herddb.model.Index
                    .builder()
                    .name(indexName)
                    .uuid(UUID.randomUUID().toString())
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

    private int sqlDataTypeToColumnType(String dataType, List<String> arguments) throws StatementExecutionException {
        int type;
        switch (dataType.toLowerCase()) {
            case "string":
            case "varchar":
            case "nvarchar":
            case "nvarchar2":
            case "nclob":
            case "text":
            case "longtext":
            case "clob":
            case "char":
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
            case "boolean":
            case "bool":
            case "bit":
                type = ColumnTypes.BOOLEAN;
                break;
            case "double":
            case "float":
                type = ColumnTypes.DOUBLE;
                break;
            case "numeric":
            case "decimal":
                if (arguments == null || arguments.isEmpty()) {
                    type = ColumnTypes.DOUBLE;
                } else if (arguments.size() == 2) {
                    int precision = Integer.parseInt(arguments.get(0));
                    int scale = Integer.parseInt(arguments.get(1));
                    if (scale == 0) {
                        if (precision > 0) {
                            type = ColumnTypes.INTEGER;
                        } else {
                            type = ColumnTypes.LONG;
                        }
                    } else {
                        type = ColumnTypes.DOUBLE;
                    }
                } else {
                    throw new StatementExecutionException("bad type " + dataType + " with arguments " + arguments);
                }

                break;

            default:
                throw new StatementExecutionException("bad type " + dataType);
        }
        return type;
    }

    private static Object resolveValue(Expression expression, boolean allowColumn) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            throw new StatementExecutionException("jdbcparameter expression not usable in this query");
        } else if (allowColumn && expression instanceof net.sf.jsqlparser.schema.Column) {
            // this is only for supporting back ticks in DDL
            return ((net.sf.jsqlparser.schema.Column) expression).getColumnName();
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
                    return resolveValue(se.getExpression(), allowColumn);
                }
                case '-': {
                    Object value = resolveValue(se.getExpression(), allowColumn);
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

    private ColumnReferencesDiscovery discoverMainTableAlias(Expression expression) throws StatementExecutionException {
        ColumnReferencesDiscovery discovery = new ColumnReferencesDiscovery(expression);
        expression.accept(discovery);
        return discovery;
    }

    private Expression collectConditionsForAlias(
            String alias, Expression expression,
            List<ColumnReferencesDiscovery> conditionsOnJoinedResult, String mainTableName
    ) throws StatementExecutionException {
        if (expression == null) {
            // no constraint on table
            return null;
        }
        ColumnReferencesDiscovery discoveredMainAlias = discoverMainTableAlias(expression);
        String mainAlias = discoveredMainAlias.getMainTableAlias();
        if (!discoveredMainAlias.isContainsMixedAliases() && alias.equals(mainAlias)) {
            return expression;
        } else if (expression instanceof AndExpression) {
            AndExpression be = (AndExpression) expression;
            ColumnReferencesDiscovery discoveredMainAliasLeft = discoverMainTableAlias(be.getLeftExpression());
            String mainAliasLeft = discoveredMainAliasLeft.isContainsMixedAliases()
                    ? null : discoveredMainAliasLeft.getMainTableAlias();

            ColumnReferencesDiscovery discoveredMainAliasright = discoverMainTableAlias(be.getRightExpression());
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
            conditionsOnJoinedResult.add(discoveredMainAlias);
            return null;
        }
    }

    private Expression composeAndExpression(List<ColumnReferencesDiscovery> conditionsOnJoinedResult) {
        if (conditionsOnJoinedResult.size() == 1) {
            return conditionsOnJoinedResult.get(0).getExpression();
        }
        AndExpression result = new AndExpression(conditionsOnJoinedResult.get(0).getExpression(),
                conditionsOnJoinedResult.get(1).getExpression());
        for (int i = 2; i < conditionsOnJoinedResult.size(); i++) {
            result = new AndExpression(result, conditionsOnJoinedResult.get(i).getExpression());
        }
        return result;
    }

    private Expression composeSimpleAndExpressions(List<Expression> expressions) {
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        AndExpression result = new AndExpression(expressions.get(0),
                expressions.get(1));
        for (int i = 2; i < expressions.size(); i++) {
            result = new AndExpression(result, expressions.get(i));
        }
        return result;
    }

    private static final Logger LOG = Logger.getLogger(DDLSQLPlanner.class.getName());

    private Statement buildExecuteStatement(String defaultTableSpace, Execute execute) throws StatementExecutionException {
        switch (execute.getName().toUpperCase()) {
            case "BEGINTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName)");
                }
                return new BeginTransactionStatement(tableSpaceName.toString());
            }
            case "COMMITTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), true);
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
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), true);
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
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() < 1) {
                    throw new StatementExecutionException("CREATETABLESPACE syntax (EXECUTE CREATETABLESPACE tableSpaceName ['leader:LEADERID'],['wait:TIMEOUT'] )");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                String leader = null;
                Set<String> replica = new HashSet<>();
                int expectedreplicacount = 1;
                long maxleaderinactivitytime = 0;
                int wait = 0;
                for (int i = 1; i < execute.getExprList().getExpressions().size(); i++) {
                    String property = (String) resolveValue(execute.getExprList().getExpressions().get(i), true);
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
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() < 2) {
                    throw new StatementExecutionException("ALTERTABLESPACE syntax (EXECUTE ALTERTABLESPACE tableSpaceName,'property:value','property2:value2')");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0), true);
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
                        String property = (String) resolveValue(execute.getExprList().getExpressions().get(i), true);
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
                    return new AlterTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount, maxleaderinactivitytime);
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            case "DROPTABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("DROPTABLESPACE syntax (EXECUTE DROPTABLESPACE tableSpaceName)");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0), true);
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
            
            case "RENAMETABLE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 3) {
                    throw new StatementExecutionException("RENAMETABLE syntax (EXECUTE RENAMETABLE 'tableSpaceName','tablename','nametablename')");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0), true);
                String oldTableName = (String) resolveValue(execute.getExprList().getExpressions().get(1), true);
                String newTableName = (String) resolveValue(execute.getExprList().getExpressions().get(2), true);
                try {
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if (tableSpace == null) {
                        throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    return new AlterTableStatement(Collections.emptyList(),
                            Collections.emptyList(), Collections.emptyList(),
                            null, oldTableName, tableSpaceName, newTableName);
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            case "CHECKTABLEINTEGRITY": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 3) {
                    throw new StatementExecutionException(" CHECKTABLEINTEGRITY syntax (EXECUTE CHECKTABLEINTEGRITY 'tableSpaceName','tablename')");
                }
                String tableSpaceName =(String) resolveValue(execute.getExprList().getExpressions().get(0), true);
                String tableName = (String) resolveValue(execute.getExprList().getExpressions().get(1), true);
                try{
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if(tableSpace == null){
                         throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    
                    return new TableIntegrityCheckStatement(tableSpaceName, tableName);
                    
                }catch(MetadataStorageManagerException err){
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
        List<Column> modifyColumns = new ArrayList<>();
        List<String> dropColumns = new ArrayList<>();
        String tableName = alter.getTable().getName();
        if (alter.getAlterExpressions() == null || alter.getAlterExpressions().size() != 1) {
            throw new StatementExecutionException("supported multi-alter operation '" + alter + "'");
        }
        AlterExpression alterExpression = alter.getAlterExpressions().get(0);
        AlterOperation operation = alterExpression.getOperation();
        Boolean changeAutoIncrement = null;
        switch (operation) {
            case ADD: {
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                for (AlterExpression.ColumnDataType cl : cols) {
                    Column newColumn = Column.column(cl.getColumnName(), sqlDataTypeToColumnType(
                            cl.getColDataType().getDataType(),
                            cl.getColDataType().getArgumentsStringList()
                    ));
                    addColumns.add(newColumn);
                }
            }
            break;
            case DROP:
                dropColumns.add(alterExpression.getColumnName());
                break;
            case MODIFY: {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
                if (tableSpaceManager == null) {
                    throw new StatementExecutionException("bad tablespace '" + tableSpace + "'");
                }
                AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
                if (tableManager == null) {
                    throw new StatementExecutionException("bad table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                Table table = tableManager.getTable();
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                for (AlterExpression.ColumnDataType cl : cols) {
                    String columnName = cl.getColumnName().toLowerCase();
                    Column oldColumn = table.getColumn(columnName);
                    if (oldColumn == null) {
                        throw new StatementExecutionException("bad column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                    }
                    Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(tableName);
                    if (indexes != null) {
                        for (AbstractIndexManager am : indexes.values()) {
                            for (String indexedColumn : am.getColumnNames()) {
                                if (indexedColumn.equalsIgnoreCase(oldColumn.name)) {
                                    throw new StatementExecutionException(
                                            "cannot alter indexed " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "',"
                                                    + "index name is " + am.getIndexName());
                                }
                            }
                        }
                    }
                    int newType = sqlDataTypeToColumnType(
                            cl.getColDataType().getDataType(),
                            cl.getColDataType().getArgumentsStringList()
                    );

                    if (oldColumn.type != newType) {
                        throw new StatementExecutionException("cannot change datatype to " + cl.getColDataType().getDataType()
                                + " for column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                    }
                    List<String> columnSpecs = decodeColumnSpecs(cl.getColumnSpecs());
                    if (table.isPrimaryKeyColumn(columnName)) {
                        boolean new_auto_increment = decodeAutoIncrement(columnSpecs);
                        if (new_auto_increment && table.primaryKey.length > 1) {
                            throw new StatementExecutionException("cannot add auto_increment flag to " + cl.getColDataType().getDataType()
                                    + " for column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                        }
                        if (table.auto_increment != new_auto_increment) {
                            changeAutoIncrement = new_auto_increment;
                        }
                    }
                    String renameTo = decodeRenameTo(columnSpecs);
                    if (renameTo != null) {
                        columnName = renameTo;
                    }
                    Column newColumnDef = Column.column(columnName, newType, oldColumn.serialPosition);
                    modifyColumns.add(newColumnDef);
                }
            }
            break;
            default:
                throw new StatementExecutionException("supported alter operation '" + alter + "'");
        }
        return new AlterTableStatement(addColumns, modifyColumns, dropColumns,
                changeAutoIncrement, tableName, tableSpace, null);
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
            return new DropTableStatement(tableSpace, tableName, drop.isIfExists());
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
            return new DropIndexStatement(tableSpace, indexName, drop.isIfExists());
        }
        throw new StatementExecutionException("only DROP TABLE and TABLESPACE is supported, drop type=" + drop.getType() + " is not implemented");
    }

    private Statement buildTruncateStatement(String defaultTableSpace, Truncate truncate) throws StatementExecutionException {

        if (truncate.getTable() == null) {
            throw new StatementExecutionException("missing table name");
        }

        String tableSpace = truncate.getTable().getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        String tableName = truncate.getTable().getName();
        return new TruncateTableStatement(tableSpace, tableName);
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

    private void verifyJdbcParametersIndexes(net.sf.jsqlparser.statement.Statement stmt) {
        JdbcQueryRewriter assigner = new JdbcQueryRewriter();
        stmt.accept(assigner);

    }

}


