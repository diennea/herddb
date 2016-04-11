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
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.commands.CreateTableStatement;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class SQLTranslator {

    public Statement translate(String query, List<Object> parameters) throws StatementExecutionException {
        try {
            net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(query);
            if (stmt instanceof CreateTable) {
                return buildCreateTableStatement((CreateTable) stmt);
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
}
