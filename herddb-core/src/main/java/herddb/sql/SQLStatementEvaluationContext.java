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

import herddb.model.StatementEvaluationContext;
import herddb.utils.RawString;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Instance of StatementEvaluationContext for SQL/JDBC
 *
 * @author enrico.olivelli
 */
public class SQLStatementEvaluationContext extends StatementEvaluationContext {

    public final String query;
    public final List<Object> jdbcParameters;
    public final Map<Object, Object> constants = new HashMap<>();

    @Override
    public List<Object> getJdbcParameters() {
        return jdbcParameters;
    }

    public SQLStatementEvaluationContext(String query, List<Object> jdbcParameters, boolean forceAcquireWriteLock) {
        super(forceAcquireWriteLock);
        this.query = query;
        this.jdbcParameters = jdbcParameters;
        final int len = jdbcParameters.size();
        for (int i = 0; i < len; i++) {
            Object o = jdbcParameters.get(i);
            if (o instanceof String) {
                jdbcParameters.set(i, RawString.of((String) o));
            }
        }

    }

    public <T> T getConstant(Object key) {
        T result = (T) constants.get(key);
        return result;
    }

    public void cacheConstant(Object key, Object value) {
        constants.put(key, value);
    }

    @Override
    public String toString() {
        return "SQLStatementEvaluationContext{" + "query=" + query + ", jdbcParameters=" + jdbcParameters + '}';
    }

}
