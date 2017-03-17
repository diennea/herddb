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
package herddb.cli;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Maps tables to tablespaces
 *
 * @author enrico.olivelli
 */
public class TableSpaceMapper {

    ScriptEngine engine;
    String groovyScript;

    public TableSpaceMapper(String groovyScript) {
        this.groovyScript = groovyScript;
        if (this.groovyScript != null && !groovyScript.isEmpty()) {
            engine = new ScriptEngineManager().getEngineByName("groovy");
        }
    }

    public String getTableSpace(Statement statement) throws ScriptException {
        String tableName = null;
        if (statement instanceof Insert) {
            Insert st = (Insert) statement;
            tableName = st.getTable().getFullyQualifiedName();
        } else if (statement instanceof Update) {
            Update st = (Update) statement;
            tableName = st.getTables().get(0).getFullyQualifiedName();
        } else if (statement instanceof Delete) {
            Delete st = (Delete) statement;
            tableName = st.getTables().get(0).getFullyQualifiedName();
        } else if (statement instanceof Truncate) {
            Truncate st = (Truncate) statement;
            tableName = st.getTable().getFullyQualifiedName();
        } else if (statement instanceof Drop) {
            Drop st = (Drop) statement;
            tableName = st.getName().getFullyQualifiedName();
        } else if (statement instanceof CreateTable) {
            CreateTable st = (CreateTable) statement;
            tableName = st.getTable().getFullyQualifiedName();
        } else if (statement instanceof Alter) {
            Alter st = (Alter) statement;
            tableName = st.getTable().getFullyQualifiedName();
        } else {
            return null;
        }
        tableName = tableName.replace("`", "");
        return mapTableNameToSchema(tableName);
    }

    public String getTableSpace(String tableName) throws ScriptException {
        return mapTableNameToSchema(tableName);
    }

    protected String mapTableNameToSchema(String tableName) throws ScriptException {
        if (engine == null) {
            return null;
        }
        engine.put("tableName", tableName);
        Object res = engine.eval(groovyScript);
        if (res != null) {
            return res.toString();
        } else {
            return null;
        }
    }

}
