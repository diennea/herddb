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
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.Table;
import java.util.List;
import java.util.Map;
import static javax.management.Query.value;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.schema.Column;

/**
 * Record mutator using SQL
 *
 * @author enrico.olivelli
 */
public class SQLRecordFunction extends RecordFunction {

    private final Table table;
    private final List<Column> columns;
    private final List<Expression> expressions;
    private final List<Object> parameters;

    public SQLRecordFunction(Table table, List<Column> columns, List<Expression> expressions, List<Object> parameters) {
        this.table = table;
        this.columns = columns;
        this.expressions = expressions;
        this.parameters = parameters;
    }

    @Override
    public byte[] computeNewValue(Record previous) {
        Map<String, Object> bean = RecordSerializer.toBean(previous, table);
        int paramIndex = 0;
        for (int i = 0; i < columns.size(); i++) {
            Expression e = expressions.get(i);
            if (e instanceof JdbcParameter) {
                Object param = parameters.get(paramIndex++);
                bean.put(columns.get(i).getColumnName(), param);
            } else {
                throw new RuntimeException("unsupported type " + e.getClass() + " " + e);
            }
        }
        return RecordSerializer.toRecord(bean, table).value.data;
    }

}
