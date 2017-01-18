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
package herddb.index.brin;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple in-memory index data storage manager
 *
 * @author enrico.olivelli
 */
public class MemoryIndexDataStorage<K, V> implements IndexDataStorage<K, V> {

    AtomicLong newPageId = new AtomicLong();

    private final ConcurrentHashMap<Long, List<Map.Entry<K, V>>> pages = new ConcurrentHashMap<>();

    @Override
    public List<Map.Entry<K, V>> loadDataPage(long pageId) throws IOException {
        return pages.get(pageId);
    }

    @Override
    public long createDataPage(List<Map.Entry<K, V>> values) throws IOException {
        long newid = newPageId.incrementAndGet();
        pages.put(newid, values);
        return newid;
    }

}
