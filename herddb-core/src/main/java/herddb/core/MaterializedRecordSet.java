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
package herddb.core;

import herddb.model.Projection;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import java.util.ArrayList;
import java.util.List;

/**
 * a Simple MaterializedRecordSet held in memory
 *
 * @author enrico.olivelli
 */
public class MaterializedRecordSet {

    final public List<Tuple> records;

    public MaterializedRecordSet() {
        records = new ArrayList<>();
    }

    public MaterializedRecordSet(int size) {
        records = new ArrayList<>(size);
    }

    void sort(TupleComparator comparator) {
        if (comparator != null) {
            records.sort(comparator);
        }
    }

    public MaterializedRecordSet select(Projection projection) throws StatementExecutionException {
        MaterializedRecordSet result = new MaterializedRecordSet(records.size());
        for (Tuple record : records) {
            result.records.add(projection.map(record));
        }
        return result;
    }

}
