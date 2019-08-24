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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.sql.SQLRecordKeyFunction;

/**
 * Scan on a secondary index, between a given range of values (inclusive)
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class SecondaryIndexRangeScan implements IndexOperation {

    public final String indexName;
    public final String[] columnsToMatch;
    public final SQLRecordKeyFunction minValue;
    public final SQLRecordKeyFunction maxValue;

    public SecondaryIndexRangeScan(String indexName, String[] columnsToMatch, SQLRecordKeyFunction minValue, SQLRecordKeyFunction maxValue) {
        this.indexName = indexName;
        this.columnsToMatch = columnsToMatch;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

}
