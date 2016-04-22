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
import herddb.model.Statement;

/**
 * LRU Cache of statements
 *
 * @author enrico.olivelli
 */
public class SQLStatementsCache {

    private final Cache<String, Statement> cache;

    public SQLStatementsCache() {
        this.cache = CacheBuilder
                .newBuilder()
                .initialCapacity(100)
                .maximumSize(100)
                .build();

    }

    public Statement get(String sql) {
        return this.cache.getIfPresent(sql);
    }

    public void put(String sql, Statement statement) {
        this.cache.put(sql, statement);
    }

    public void clear() {
        this.cache.invalidateAll();
    }

}
