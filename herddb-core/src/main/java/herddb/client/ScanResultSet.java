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

import herddb.utils.DataAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Receives records from a Scan
 *
 * @author enrico.olivelli
 */
public abstract class ScanResultSet implements AutoCloseable {

    public abstract ScanResultSetMetadata getMetadata();

    public abstract boolean hasNext() throws HDBException;

    public abstract DataAccessor next() throws HDBException;

    public final long transactionId;

    public ScanResultSet(long transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public void close() {
    }
    
    public List<Map<String, Object>> consume() throws HDBException {
        List<Map<String, Object>> result = new ArrayList<>();
        while (hasNext()) {
            Map<String, Object> record = next().toMap();
            result.add(record);
        }
        this.close();
        return result;
    }
    
    /**
     * Used only by JDBC API
     * @return some id of this ResultSet
     */
    public String getCursorName() {
        return "<unnamed-" + System.identityHashCode(this) + "@tx" + transactionId + ">";
    }
}
