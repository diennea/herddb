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
import herddb.sql.SQLRecordKeyFunction;
import herddb.utils.Bytes;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Seek on secondary index
 *
 * @author enrico.olivelli
 */
public class SecondaryIndexSeek implements IndexOperation {

    public final String indexName;
    public final String[] columnsToMatch;
    public final SQLRecordKeyFunction value;

    public SecondaryIndexSeek(String indexName, String[] columnsToMatch, SQLRecordKeyFunction value) {
        this.indexName = indexName;
        this.columnsToMatch = columnsToMatch;
        this.value = value;
    }

    @Override
    public Predicate<? super Map.Entry<Bytes, Long>> toStreamPredicate(StatementEvaluationContext ctx, TableContext tableContext) {
        throw new UnsupportedOperationException("Not supported"); //To change body of generated methods, choose Tools | Templates.
    }

}
