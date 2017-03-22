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

import java.util.HashMap;
import java.util.Map;
import net.sf.jsqlparser.expression.JdbcParameter;

/**
 * Cache to store "immutable" Expressions, this reduces a lot the impact of large execution plans which include a lot of
 * replicated expressions, like JdbcParameter.
 * This in general boost INSERT expressions
 *
 * @author enrico.olivelli
 */
public class ImmutableExpressionsCache {

    private static final class ImmutableJdbcParameter extends JdbcParameter {

        public ImmutableJdbcParameter(Integer index, boolean useFixedIndex) {
            super(index, useFixedIndex);
        }

        @Override
        public void setUseFixedIndex(boolean useFixedIndex) {
            throw new IllegalStateException("this value is immutable");
        }

        @Override
        public void setIndex(Integer index) {
            throw new IllegalStateException("this value is immutable");
        }

    }

    private static final Map<Integer, ImmutableJdbcParameter> IMMUTABLE_JDBC_PARAMETERS = new HashMap<>();

    static {
        for (int i = 0; i < 2000; i++) {
            IMMUTABLE_JDBC_PARAMETERS.put(i, new ImmutableJdbcParameter(i, false));
        }
    }

    public static JdbcParameter internJdbcParameterExpression(JdbcParameter param) {
        ImmutableJdbcParameter res = IMMUTABLE_JDBC_PARAMETERS.get(param.getIndex());
        return res != null ? res : param;
    }
}
