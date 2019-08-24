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

package herddb.model;

import herddb.model.planner.ProjectOp.IdentityProjection;
import herddb.utils.DataAccessor;

/**
 * Function which maps the result of a scan to a Tuple
 *
 * @author enrico.olivelli
 */
public interface Projection {

    // CHECKSTYLE.OFF: MethodName
    static Projection IDENTITY(String[] fieldNames, Column[] columns) {
        return new IdentityProjection(fieldNames, columns);
    }
    // CHECKSTYLE.ON: MethodName

    // CHECKSTYLE.OFF: MethodName
    static Projection PRIMARY_KEY(Table table) {
        final Column[] columns = new Column[table.primaryKey.length];

        int i = 0;
        for (String pk : table.primaryKey) {
            columns[i++] = table.getColumn(pk);
        }
        return new Projection() {
            @Override
            public Column[] getColumns() {
                return table.columns;
            }

            @Override
            public String[] getFieldNames() {
                return table.primaryKey;
            }

            @Override
            public Tuple map(DataAccessor tuple, StatementEvaluationContext context) throws StatementExecutionException {
                Object[] values = new Object[columns.length];
                for (int i = 0; i < values.length; i++) {
                    Object v = tuple.get(columns[i].name);
                    values[i] = v;
                }
                return new Tuple(table.primaryKey, values);
            }
        };
    }
    // CHECKSTYLE.ON: MethodName

    Column[] getColumns();

    String[] getFieldNames();

    DataAccessor map(DataAccessor tuple, StatementEvaluationContext context) throws StatementExecutionException;

}
