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
package herddb.server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Simple cache of prepared statements on the server side
 *
 * @author enrico.olivelli
 */
public class ServerSidePreparedStatementCache {

    private final AtomicLong idGenerator = new AtomicLong();
    private final ConcurrentHashMap<String, Long> preparedStatements = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> tableSpaces = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> queries = new ConcurrentHashMap<>();

    public void registerQueryId(String tableSpace, String text, long id) {
        preparedStatements.put(tableSpace + "#" + text, id);
    }

    long prepare(String tableSpace, String text) {
        return preparedStatements.computeIfAbsent(tableSpace + "#" + text, (k) -> {
            long newId = idGenerator.incrementAndGet();
            queries.put(newId, text);
            tableSpaces.put(newId, tableSpace);
            return newId;
        });
    }
    private static final Logger LOG = Logger.getLogger(ServerSidePreparedStatementCache.class.getName());

    public int getSize() {
        return preparedStatements.size();
    }

    String resolveQuery(String tableSpace, long statementId) {
        String ts = tableSpaces.get(statementId);
        if (!tableSpace.equals(ts)) {
            return null;
        }
        String query = queries.get(statementId);
        if (query == null) {
            return null;
        }
        return query;
    }

}
