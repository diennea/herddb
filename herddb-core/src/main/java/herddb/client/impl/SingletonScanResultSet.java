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
import java.util.HashMap;

/**
 * @author enrico.olivelli
 */
public class SingletonScanResultSet extends ScanResultSet {

    boolean read = false;
    final Object key;

    public SingletonScanResultSet(long transactionId, Object key) {
        super(transactionId);
        this.key = key;
    }

    private static final String[] HEADER = {"key"};

    @Override
    public ScanResultSetMetadata getMetadata() {
        return new ScanResultSetMetadata(HEADER);
    }

    @Override
    public boolean hasNext() throws HDBException {
        return !read;
    }

    @Override
    public DataAccessor next() throws HDBException {
        read = true;
        HashMap<String, Object> result = new HashMap<>();
        result.put("key", key);
        return new MapDataAccessor(result, HEADER);
    }

}
