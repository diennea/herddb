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
package herddb.sql.predicates;

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.SQLRecordPredicate;
import java.util.Map;

public class ColumnMinorThanEqualsJdbcParameter implements HardcodedPredicateMatcher {

    private final String columnName;
    private final int index;

    public ColumnMinorThanEqualsJdbcParameter(String columnName, int index) {
        this.columnName = columnName;
        this.index = index;
    }

    @Override
    public boolean matches(Map<String, Object> bean, StatementEvaluationContext context) throws StatementExecutionException {
        Object left = bean.get(columnName);
        Object value = context.getJdbcParameters().get(index);
        return SQLRecordPredicate.compare(left, value) <= 0;
    }

}
