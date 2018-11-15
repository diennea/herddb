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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import herddb.core.HerdDBInternalException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple cache of prepared statements on the server side
 *
 * @author enrico.olivelli
 */
public class ServerSidePreparedStatementCache {

    private final AtomicLong idGenerator = new AtomicLong();
    private final Cache<String, Long> preparedStatements;
    private final ConcurrentHashMap<Long, PreparedStatementInfo> statementsInfo = new ConcurrentHashMap<>();

    private static final class PreparedStatementInfo {

        private final String query;
        private final String tablespace;
        private final int memory;

        public PreparedStatementInfo(String query, String tablespace) {
            this.query = query;
            this.tablespace = tablespace;
            // impossible overflow
            this.memory = query.length() + tablespace.length();
        }

    }

    public ServerSidePreparedStatementCache(long maxMemory) {
        this.preparedStatements = CacheBuilder
                .<String, Long>newBuilder()
                .maximumWeight(maxMemory)
                .initialCapacity(200)
                .weigher((String query, Long statementid) -> {
                    PreparedStatementInfo info = statementsInfo.get(statementid);
                    if (info == null) {
                        // quite impossible
                        return Integer.MAX_VALUE;
                    } else {
                        return info.memory;
                    }
                })
                .removalListener((RemovalNotification<String, Long> notification) -> {
                    PreparedStatementInfo info = statementsInfo.remove(notification.getValue());
                    LOG.log(Level.INFO, "unpreparing {0} {1}", new Object[]{notification.getValue(), notification.getKey()});
                })
                .build();
    }

    public void registerQueryId(String tableSpace, String text, long id) {
        preparedStatements.put(tableSpace + "#" + text, id);
    }

    long prepare(String tableSpace, String text) {
        try {
            return preparedStatements.get(tableSpace + "#" + text, () -> {
                long newId = idGenerator.incrementAndGet();
                PreparedStatementInfo info = new PreparedStatementInfo(text, tableSpace);
                statementsInfo.put(newId, info);
                return newId;
            });
        } catch (ExecutionException err) {
            throw new HerdDBInternalException(err.getCause());
        }
    }
    private static final Logger LOG = Logger.getLogger(ServerSidePreparedStatementCache.class.getName());

    public long getSize() {
        return preparedStatements.size();
    }

    String resolveQuery(String tableSpace, long statementId) {
        PreparedStatementInfo info = statementsInfo.get(statementId);
        if (info == null) {
            return null;
        }
        if (!tableSpace.equals(info.tablespace)) {
            return null;
        }
        return info.query;
    }

    @VisibleForTesting
    public void clear() {
        preparedStatements.invalidateAll();
        statementsInfo.clear();
    }
}
