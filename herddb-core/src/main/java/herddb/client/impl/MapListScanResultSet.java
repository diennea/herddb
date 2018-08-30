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
package herddb.client.impl;

import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.client.ScanResultSetMetadata;
import herddb.utils.DataAccessor;
import herddb.utils.MapDataAccessor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simple wrapper for static data
 *
 * @author enrico.olivelli
 */
public class MapListScanResultSet extends ScanResultSet {

    private final Iterator<DataAccessor> iterator;
    private final ScanResultSetMetadata metadata;

    public MapListScanResultSet(long transactionId, ScanResultSetMetadata metadata, String[] columns, List<Map<String, Object>> list) {
        super(transactionId);
        this.iterator = list.stream().map(m -> {
            return (DataAccessor) new MapDataAccessor(m, columns);
        }).collect(Collectors.toList()).iterator();
        this.metadata = metadata;
    }

    @Override
    public ScanResultSetMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean hasNext() throws HDBException {
        return iterator.hasNext();
    }

    @Override
    public DataAccessor next() throws HDBException {
        return iterator.next();
    }
}
