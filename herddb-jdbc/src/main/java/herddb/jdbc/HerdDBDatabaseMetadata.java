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
package herddb.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.client.ScanResultSetMetadata;
import herddb.client.impl.EmptyScanResultSet;
import herddb.client.impl.IteratorScanResultSet;
import herddb.model.TransactionContext;
import herddb.utils.SQLUtils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Database Metadata Implementation
 *
 * @author enrico.olivelli
 */
public class HerdDBDatabaseMetadata implements DatabaseMetaData {

    private final HerdDBConnection con;
    private final String tableSpace;

    HerdDBDatabaseMetadata(HerdDBConnection con, String tableSpace) {
        this.con = con;
        this.tableSpace = tableSpace;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return "";
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "HerdDB";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "HerdDB";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "0.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "'";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "tablespace";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        switch (level) {
            case Connection.TRANSACTION_READ_COMMITTED:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    private static final String[] GET_TABLES_SCHEMA = new String[]{
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "TABLE_TYPE",
        "REMARKS",
        "TYPE_CAT",
        "TYPE_SCHEM",
        "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION"
    };
    private static final String[] GET_INDEXES_SCHEMA = new String[]{
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "NON_UNIQUE",
        "INDEX_QUALIFIER",
        "INDEX_NAME",
        "TYPE",
        "ORDINAL_POSITION",
        "COLUMN_NAME",
        "ASC_OR_DESC",
        "CARDINALITY",
        "FILTER_CONDITION"
    };

    @Override
    /**
     * Retrieves a description of the tables available in the given catalog.
     * Only table descriptions matching the catalog, schema, table name and type
     * criteria are returned. They are ordered by <code>TABLE_TYPE</code>,
     * <code>TABLE_CAT</code>, <code>TABLE_SCHEM</code> and
     * <code>TABLE_NAME</code>.
     * <P>
     * Each table description has the following columns:
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be
     * <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be
     * <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>TABLE_TYPE</B> String {@code =>} table type. Typical types are
     * "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
     * "ALIAS", "SYNONYM".
     * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table
     * <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be
     * <code>null</code>)
     * <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be
     * <code>null</code>)
     * <LI><B>TYPE_NAME</B> String {@code =>} type name (may be
     * <code>null</code>)
     * <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the
     * designated "identifier" column of a typed table (may be
     * <code>null</code>)
     * <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in
     * SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER",
     * "DERIVED". (may be <code>null</code>)
     * </OL>
     *
     * <P>
     * <B>Note:</B> Some databases may not return information for all tables.
     *
     * @param catalog a catalog name; must match the catalog name as it is
     * stored in the database; "" retrieves those without a catalog;
     * <code>null</code> means that the catalog name should not be used to
     * narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as
     * it is stored in the database; "" retrieves those without a schema;
     * <code>null</code> means that the schema name should not be used to narrow
     * the search
     * @param tableNamePattern a table name pattern; must match the table name
     * as it is stored in the database
     * @param types a list of table types, which must be from the list of table
     * types returned from {@link #getTableTypes},to include; <code>null</code>
     * returns all types
     * @return <code>ResultSet</code> - each row is a table description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        String query = "SELECT table_name FROM SYSTABLES";
        if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
            query = query + " WHERE table_name LIKE '" + SQLUtils.escape(tableNamePattern) + "'";
        }
        try (Statement statement = con.createStatement();
                ResultSet rs = statement.executeQuery(query)) {

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                String table_name = rs.getString("table_name");

                Map<String, Object> data = new HashMap<>();
                data.put("TABLE_CAT", null);
                data.put("TABLE_SCHEM", tableSpace);
                data.put("TABLE_NAME", table_name);
                data.put("TABLE_TYPE", "TABLE");
                data.put("REMARKS", "");
                data.put("TYPE_CAT", null);
                data.put("TYPE_SCHEM", null);
                data.put("TYPE_NAME", null);
                data.put("SELF_REFERENCING_COL_NAME", null);
                data.put("REF_GENERATION", null);
                results.add(data);

            }
            ScanResultSetMetadata metadata = new ScanResultSetMetadata(GET_TABLES_SCHEMA);
            return new HerdDBResultSet(new IteratorScanResultSet(TransactionContext.NOTRANSACTION_ID, metadata, results.iterator()));
        }
    }

    private final static String[] GET_SCHEMAS_SCHEMA = new String[]{"TABLE_SCHEM", "TABLE_CATALOG"};

    @Override
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String query = "SELECT tablespace_name FROM SYSTABLESPACES";
        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            query = query + " WHERE tablespace_name LIKE '" + SQLUtils.escape(schemaPattern) + "'";
        }

        try (Statement statement = con.createStatement();
                ResultSet rs = statement.executeQuery(query)) {

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                String tablespace_name = rs.getString("tablespace_name");

                Map<String, Object> data = new HashMap<>();

                data.put("TABLE_SCHEM", tablespace_name);
                data.put("TABLE_CATALOG", null);

                results.add(data);

            }
            ScanResultSetMetadata metadata = new ScanResultSetMetadata(GET_SCHEMAS_SCHEMA);
            return new HerdDBResultSet(new IteratorScanResultSet(TransactionContext.NOTRANSACTION_ID, metadata, results.iterator()));
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    private static final String[] GET_COLUMNS_SCHEMA = new String[]{
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "COLUMN_NAME",
        "DATA_TYPE",
        "COLUMN_SIZE",
        "BUFFER_LENGTH",
        "DECIMAL_DIGITS",
        "NUM_PREC_RADIX",
        "NULLABLE",
        "REMARKS",
        "COLUMN_DEF",
        "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH",
        "ORDINAL_POSITION",
        "IS_NULLABLE",
        "SCOPE_CATALOG",
        "SCOPE_SCHEMA",
        "SCOPE_TABLE",
        "SOURCE_DATA_TYPE",
        "IS_AUTOINCREMENT",
        "IS_GENERATEDCOLUMN"
    };

    /**
     * Retrieves a description of table columns available in the specified
     * catalog.
     *
     * <P>
     * Only column descriptions matching the catalog, schema, table and column
     * name criteria are returned. They are ordered by
     * <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>, <code>TABLE_NAME</code>,
     * and <code>ORDINAL_POSITION</code>.
     *
     * <P>
     * Each column description has the following columns:
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be
     * <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be
     * <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
     * for a UDT the type name is fully qualified
     * <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     * <LI><B>BUFFER_LENGTH</B> is not used.
     * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits.
     * Null is returned for data types where DECIMAL_DIGITS is not applicable.
     * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     * <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
     * <UL>
     * <LI> columnNoNulls - might not allow <code>NULL</code> values
     * <LI> columnNullable - definitely allows <code>NULL</code> values
     * <LI> columnNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be
     * <code>null</code>)
     * <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column,
     * which should be interpreted as a string when the value is enclosed in
     * single quotes (may be <code>null</code>)
     * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum
     * number of bytes in the column
     * <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
     * (starting at 1)
     * <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine
     * the nullability for a column.
     * <UL>
     * <LI> YES --- if the column can include NULLs
     * <LI> NO --- if the column cannot include NULLs
     * <LI> empty string --- if the nullability for the column is unknown
     * </UL>
     * <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the
     * scope of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     * <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the
     * scope of a reference attribute (<code>null</code> if the DATA_TYPE isn't
     * REF)
     * <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
     * of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     * <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct
     * type or user-generated Ref type, SQL type from java.sql.Types
     * (<code>null</code> if DATA_TYPE isn't DISTINCT or user-generated REF)
     * <LI><B>IS_AUTOINCREMENT</B> String {@code =>} Indicates whether this
     * column is auto incremented
     * <UL>
     * <LI> YES --- if the column is auto incremented
     * <LI> NO --- if the column is not auto incremented
     * <LI> empty string --- if it cannot be determined whether the column is
     * auto incremented
     * </UL>
     * <LI><B>IS_GENERATEDCOLUMN</B> String {@code =>} Indicates whether this is
     * a generated column
     * <UL>
     * <LI> YES --- if this a generated column
     * <LI> NO --- if this not a generated column
     * <LI> empty string --- if it cannot be determined whether this is a
     * generated column
     * </UL>
     * </OL>
     *
     * <p>
     * The COLUMN_SIZE column specifies the column size for the given column.
     * For numeric data, this is the maximum precision. For character data, this
     * is the length in characters. For datetime datatypes, this is the length
     * in characters of the String representation (assuming the maximum allowed
     * precision of the fractional seconds component). For binary data, this is
     * the length in bytes. For the ROWID datatype, this is the length in bytes.
     * Null is returned for data types where the column size is not applicable.
     *
     * @param catalog a catalog name; must match the catalog name as it is
     * stored in the database; "" retrieves those without a catalog;
     * <code>null</code> means that the catalog name should not be used to
     * narrow the search
     * @param schemaPattern a schema name pattern; must match the schema name as
     * it is stored in the database; "" retrieves those without a schema;
     * <code>null</code> means that the schema name should not be used to narrow
     * the search
     * @param tableNamePattern a table name pattern; must match the table name
     * as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column
     * name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a column description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    @Override
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {

        String query = "SELECT table_name,column_name,data_type,auto_increment,is_nullable,ordinal_position FROM SYSCOLUMNS WHERE 1=1 ";
        if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
            query = query + " AND table_name LIKE '" + SQLUtils.escape(tableNamePattern) + "'";
        }
        if (columnNamePattern != null && !columnNamePattern.isEmpty()) {
            query = query + " AND column_name LIKE '" + SQLUtils.escape(columnNamePattern) + "'";
        }
        try (Statement statement = con.createStatement();
                ResultSet rs = statement.executeQuery(query)) {

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                String table_name = rs.getString("table_name");
                String column_name = rs.getString("column_name");
                String data_type = rs.getString("data_type");
                int auto_increment = rs.getInt("auto_increment");
                int is_nullable = rs.getInt("is_nullable");
                int ordinal_position = rs.getInt("ordinal_position");

                Map<String, Object> data = new HashMap<>();
                data.put("TABLE_CAT", null);
                data.put("TABLE_SCHEM", tableSpace);
                data.put("TABLE_NAME", table_name);
                data.put("COLUMN_NAME", column_name);
                data.put("DATA_TYPE", data_type);
                data.put("COLUMN_SIZE", 0);
                data.put("BUFFER_LENGTH", 0);
                data.put("DECIMAL_DIGITS", null);
                data.put("NUM_PREC_RADIX", null);
                data.put("NULLABLE", is_nullable > 0 ? "YES" : "NO");
                data.put("REMARKS", "");
                data.put("COLUMN_DEF", "");
                data.put("SQL_DATA_TYPE", "");
                data.put("SQL_DATETIME_SUB", "");
                data.put("CHAR_OCTET_LENGTH", "");
                data.put("ORDINAL_POSITION", ordinal_position);
                data.put("IS_NULLABLE", is_nullable > 0 ? "YES" : "NO");
                data.put("SCOPE_CATALOG", null);
                data.put("SCOPE_SCHEMA", null);
                data.put("SCOPE_TABLE", null);
                data.put("SOURCE_DATA_TYPE", null);
                data.put("IS_AUTOINCREMENT", auto_increment > 0 ? "YES" : "NO");
                data.put("IS_GENERATEDCOLUMN", auto_increment > 0 ? "YES" : "NO");

                results.add(data);

            }
            ScanResultSetMetadata metadata = new ScanResultSetMetadata(GET_COLUMNS_SCHEMA);
            return new HerdDBResultSet(new IteratorScanResultSet(TransactionContext.NOTRANSACTION_ID, metadata, results.iterator()));
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    /**
     * Retrieves a description of the given table's indices and statistics. They
     * are ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <P>
     * Each index column description has the following columns:
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be
     * <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be
     * <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>NON_UNIQUE</B> boolean {@code =>} Can index values be non-unique.
     * false when TYPE is tableIndexStatistic
     * <LI><B>INDEX_QUALIFIER</B> String {@code =>} index catalog (may be
     * <code>null</code>); <code>null</code> when TYPE is tableIndexStatistic
     * <LI><B>INDEX_NAME</B> String {@code =>} index name; <code>null</code>
     * when TYPE is tableIndexStatistic
     * <LI><B>TYPE</B> short {@code =>} index type:
     * <UL>
     * <LI> tableIndexStatistic - this identifies table statistics that are
     * returned in conjunction with a table's index descriptions
     * <LI> tableIndexClustered - this is a clustered index
     * <LI> tableIndexHashed - this is a hashed index
     * <LI> tableIndexOther - this is some other style of index
     * </UL>
     * <LI><B>ORDINAL_POSITION</B> short {@code =>} column sequence number
     * within index; zero when TYPE is tableIndexStatistic
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name; <code>null</code>
     * when TYPE is tableIndexStatistic
     * <LI><B>ASC_OR_DESC</B> String {@code =>} column sort sequence, "A"
     * {@code =>} ascending, "D" {@code =>} descending, may be <code>null</code>
     * if sort sequence is not supported; <code>null</code> when TYPE is
     * tableIndexStatistic
     * <LI><B>CARDINALITY</B> long {@code =>} When TYPE is tableIndexStatistic,
     * then this is the number of rows in the table; otherwise, it is the number
     * of unique values in the index.
     * <LI><B>PAGES</B> long {@code =>} When TYPE is tableIndexStatistic then
     * this is the number of pages used for the table, otherwise it is the
     * number of pages used for the current index.
     * <LI><B>FILTER_CONDITION</B> String {@code =>} Filter condition, if any.
     * (may be <code>null</code>)
     * </OL>
     *
     * @param catalog a catalog name; must match the catalog name as it is
     * stored in this database; "" retrieves those without a catalog;
     * <code>null</code> means that the catalog name should not be used to
     * narrow the search
     * @param schema a schema name; must match the schema name as it is stored
     * in this database; "" retrieves those without a schema; <code>null</code>
     * means that the schema name should not be used to narrow the search
     * @param tableNamePattern a table name; must match the table name as it is stored in
     * this database
     * @param onlyUnique when true, return only indices for unique values; when
     * false, return indices regardless of whether unique or not
     * @param approximate when true, result is allowed to reflect approximate or
     * out of data values; when false, results are requested to be accurate
     * @return <code>ResultSet</code> - each row is an index column description
     * @exception SQLException if a database access error occurs
     */
    @Override
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public ResultSet getIndexInfo(String catalog, String schema, String tableNamePattern, boolean onlyUnique, boolean approximate) throws SQLException {
        String query = "SELECT * FROM SYSINDEXCOLUMNS";
        if (tableNamePattern != null && !tableNamePattern.isEmpty()) {
            query = query + " WHERE table_name LIKE '" + SQLUtils.escape(tableNamePattern) + "'";
        }        
        try (Statement statement = con.createStatement();
                ResultSet rs = statement.executeQuery(query)) {

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                String table_name = rs.getString("table_name");
                String index_name = rs.getString("index_name");
                String column_name = rs.getString("column_name");
                int ordinal_position = rs.getInt("ordinal_position");
                boolean clustered = rs.getInt("clustered") == 1;
                boolean uniqueValues = rs.getInt("unique") == 1;

                if (onlyUnique && !uniqueValues) {                    
                    continue;
                }

                Map<String, Object> data = new HashMap<>();
                data.put("TABLE_CAT", null);
                data.put("TABLE_SCHEM", tableSpace);
                data.put("TABLE_NAME", table_name);
                data.put("NON_UNIQUE", !uniqueValues);
                data.put("INDEX_QUALIFIER", null);

                data.put("INDEX_NAME", index_name);
                data.put("TYPE", clustered ? DatabaseMetaData.tableIndexClustered : DatabaseMetaData.tableIndexOther);

                data.put("ORDINAL_POSITION", ordinal_position);

                data.put("COLUMN_NAME", column_name);
                data.put("ASC_OR_DESC", null);
                data.put("CARDINALITY", null);
                data.put("FILTER_CONDITION", null);
                results.add(data);

            }
            ScanResultSetMetadata metadata = new ScanResultSetMetadata(GET_INDEXES_SCHEMA);
            return new HerdDBResultSet(new IteratorScanResultSet(TransactionContext.NOTRANSACTION_ID, metadata, results.iterator()));
        }
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return true;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.con;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return true;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new HerdDBResultSet(new EmptyScanResultSet(TransactionContext.NOTRANSACTION_ID));
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) this;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
