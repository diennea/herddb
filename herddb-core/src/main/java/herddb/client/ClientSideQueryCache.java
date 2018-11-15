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
package herddb.client;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks client side prepared statements.
 *
 * @author enrico.olivelli
 */
public class ClientSideQueryCache {

    private final ConcurrentHashMap<String, Long> preparedStatements = new ConcurrentHashMap<>();

    public long getQueryId(String tableSpace, String query) {
        return preparedStatements.getOrDefault(tableSpace + "#" + query, 0L);
    }

    public void registerQueryId(String tableSpace, String text, long id) {
        preparedStatements.put(tableSpace + "#" + text, id);
    }

    void invalidate(long statementId) {
        String query = null;
        for (Map.Entry<String, Long> next : preparedStatements.entrySet()) {
            Long value = next.getValue();
            if (value != null && value == statementId) {
                query = next.getKey();
            }
        }
        if (query != null) {
            preparedStatements.remove(query, statementId);
        }
    }
}
