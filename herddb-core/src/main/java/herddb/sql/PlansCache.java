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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import herddb.model.ExecutionPlan;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LRU Cache of Execution plans
 *
 * @author enrico.olivelli
 */
public class PlansCache {

    private static final Logger LOG = Logger.getLogger(PlansCache.class.getName());

    private final Cache<String, ExecutionPlan> cache;

    public PlansCache(long maxBytes) {

        LOG.log(Level.INFO, "Max query plan cache size: {0} bytes", maxBytes + "");

        this.cache = CacheBuilder
            .newBuilder()
            .recordStats()
            .softValues()
            .weigher((String sql, ExecutionPlan plan) -> {
                return sql.length() * 10;
            })
            .maximumWeight(maxBytes)
            .removalListener((RemovalNotification<String, ExecutionPlan> notification) -> {
                LOG.log(Level.FINE, "Removed query " + notification.getCause() + " -> " + notification.getKey());
            })
            .build();

    }

    public long getCacheSize() {
        return cache.size();
    }

    public long getCacheHits() {
        return cache.stats().hitCount();
    }

    public long getCacheMisses() {
        return cache.stats().missCount();
    }

    public ExecutionPlan get(String sql) {
        return this.cache.getIfPresent(sql);
    }

    public void put(String sql, ExecutionPlan statement) {
        this.cache.put(sql, statement);
    }

    public void clear() {
        this.cache.invalidateAll();
    }

}
