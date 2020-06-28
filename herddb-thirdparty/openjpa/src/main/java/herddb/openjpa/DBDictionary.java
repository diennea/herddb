/*
 * Copyright 2020 eolivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.openjpa;

/**
 * OpenJPA DBDictionary
 */
public class DBDictionary extends org.apache.openjpa.jdbc.sql.DBDictionary {

    public DBDictionary() {
        platform = "HerdDB";

//         It looks like this stuff does not work, it still does create tables with 'user' name
//        reservedWordSet.addAll(Arrays.asList(new String[]{
//            "AUTO_INCREMENT", "USER", "LANGUAGE", "PERIOD", "VALUE"
//        }));
//
//        invalidColumnWordSet.addAll(Arrays.asList(new String[]{
//            "ADD", "ALL", "ALTER", "AND", "AS", "ASC", "BETWEEN", "BINARY",
//            "BLOB", "BOTH", "BY", "CASCADE", "CASE", "CHANGE", "CHAR",
//            "CHARACTER", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CONTINUE",
//            "CONVERT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
//            "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DEC", "DECIMAL",
//            "DECLARE", "DEFAULT", "DELETE", "DESC", "DESCRIBE", "DISTINCT",
//            "DOUBLE", "DROP", "ELSE", "END-EXEC", "EXISTS", "FALSE", "FETCH",
//            "FLOAT", "FLOAT4", "FOR", "FOREIGN", "FROM", "GRANT", "GROUP",
//            "HAVING", "IN", "INFILE", "INNER", "INSENSITIVE", "INSERT", "INT",
//            "INT1", "INT2", "INT4", "INTEGER", "INTERVAL", "INTO", "IS", "JOIN",
//            "KEY", "LEADING", "LEFT", "LIKE", "LOAD", "MATCH", "MEDIUMINT",
//            "NATURAL", "NOT", "NULL", "NUMERIC", "ON", "OPTION", "OR", "ORDER",
//            "OUTER", "OUTFILE", "PRECISION", "PRIMARY", "PROCEDURE", "READ",
//            "REAL", "REFERENCES", "REPLACE", "RESTRICT", "REVOKE", "RIGHT",
//            "SCHEMA", "SELECT", "SET", "SMALLINT", "SQL", "SQLSTATE",
//            "STARTING", "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION",
//            "UNIQUE", "UNSIGNED", "UPDATE", "USAGE", "USING", "VALUES",
//            "VARCHAR", "VARYING", "WHEN", "WHERE", "WITH", "WRITE", "ZEROFILL",
//            "INDEX", "AUTO_INCREMENT", "USER", "LANGUAGE", "PERIOD", "VALUE", "VALUES"
//        }));

        forUpdateClause = "";

    }
//
//    @Override
//    public void closeDataSource(DataSource ds) {
//        super.closeDataSource(ds);
//        if (ds instanceof DelegatingDataSource) {
//            ds = ((DelegatingDataSource) ds).getInnermostDelegate();
//        }
//        if (ds instanceof BasicDataSource) {
//            try {
//                BasicDataSource sa = (BasicDataSource) ds;
//                sa.close();
//            } catch (SQLException ex) {
//                throw new RuntimeException(ex);
//            }
//        }
//        if (ds instanceof AutoCloseable) {
//            try {
//                AutoCloseable ac = (AutoCloseable) ds;
//                ac.close();
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            }
//        }
//    }


}
