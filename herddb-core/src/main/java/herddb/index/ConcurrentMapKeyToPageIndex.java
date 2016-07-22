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
package herddb.index;

import herddb.model.StatementEvaluationContext;
import herddb.model.TableContext;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of KeyToPageIndex which uses any ConcurrentMap
 *
 * @author enrico.olivelli
 */
public class ConcurrentMapKeyToPageIndex implements KeyToPageIndex {

    private final ConcurrentMap<Bytes, Long> map;

    public ConcurrentMapKeyToPageIndex(ConcurrentMap<Bytes, Long> map) {
        this.map = map;
    }

    public ConcurrentMap<Bytes, Long> getMap() {
        return map;
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public Long put(Bytes key, long currentPage) {
        return map.put(key, currentPage);
    }

    @Override
    public List<Bytes> getKeysMappedToPage(long pageId) {
        return map.entrySet().stream().filter(entry -> pageId == entry.getValue()).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    @Override
    public boolean containsKey(Bytes key) {
        return map.containsKey(key);
    }

    @Override
    public Long get(Bytes key) {
        return map.get(key);
    }

    @Override
    public Long remove(Bytes key) {
        return map.remove(key);
    }

    @Override
    public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) {
        Stream<Map.Entry<Bytes, Long>> baseStream = map.entrySet().stream();
        if (operation == null) {
            return baseStream;
        } else if (operation instanceof PrimaryIndexPrefixScan) {
            return baseStream.filter(operation.toStreamPredicate(context, tableContext));
        } else {
            throw new IllegalArgumentException("operation " + operation + " not implemented on " + this.getClass());
        }
    }

    @Override
    public void close() {
        map.clear();
    }

}
