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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import herddb.model.ExecutionPlan;
import herddb.utils.IntHolder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LRU Cache of Execution plans
 *
 * @author enrico.olivelli
 */
public class PlansCache {

    private static final Logger LOG = Logger.getLogger(PlansCache.class.getName());

    private final Cache<String, ExecutionPlanContainer> cache;

    private static final boolean KRYO_AVAILABLE;

    static {
        boolean _KRYO_AVAILABLE;
        try {
            new Kryo().writeObject(new Output(new ByteArrayOutputStream()), "");
            _KRYO_AVAILABLE = true;
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Kryo is not available", t);
            _KRYO_AVAILABLE = false;
        }
        KRYO_AVAILABLE = _KRYO_AVAILABLE;
    }

    private static class ExecutionPlanContainer {

        private final ExecutionPlan plan;
        private final int weight;

        public ExecutionPlanContainer(ExecutionPlan plan) {
            this.plan = plan;
            this.weight = computeWeigth(plan);
        }

        private int computeWeigth(ExecutionPlan plan) {

            if (!KRYO_AVAILABLE) {
                return 1;
            }

            final Kryo kryo = new Kryo();
            IntHolder res = new IntHolder();
            try (Output oo = new Output(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    res.value++;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    res.value += len;
                }

                @Override
                public void write(byte[] b) throws IOException {
                    res.value += b.length;
                }

            })) {
                kryo.writeObject(oo, plan);
            }
            return res.value;
        }

    }

    public PlansCache(long maxBytes) {

        LOG.log(Level.INFO, "Max query plan cache size: {0} bytes", maxBytes + "");

        this.cache = CacheBuilder
                .newBuilder()
                .recordStats()
                .weigher((String sql, ExecutionPlanContainer plan) -> {
                    return plan.weight;
                })
                .maximumWeight(maxBytes)
                .removalListener((RemovalNotification<String, ExecutionPlanContainer> notification) -> {
                    LOG.log(Level.FINE, "Removed query {0} -> {1} size {2} bytes", new Object[]{notification.getCause(),
                            notification.getKey(), notification.getValue().weight});
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
        ExecutionPlanContainer res = this.cache.getIfPresent(sql);
        return res != null ? res.plan : null;
    }

    public void put(String sql, ExecutionPlan statement) {
        this.cache.put(sql, new ExecutionPlanContainer(statement));
    }

    public void clear() {
        this.cache.invalidateAll();
    }

}
