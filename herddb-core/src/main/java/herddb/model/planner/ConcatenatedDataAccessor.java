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

package herddb.model.planner;

import herddb.utils.AbstractDataAccessor;
import herddb.utils.DataAccessor;

/**
 * Concatenates two records
 *
 * @author eolivelli
 */
class ConcatenatedDataAccessor extends AbstractDataAccessor {

    private final String[] fieldNames;
    private final DataAccessor a;
    private final DataAccessor b;
    private final int endOfA;

    public ConcatenatedDataAccessor(String[] fieldNames, DataAccessor a, DataAccessor b) {
        this.fieldNames = fieldNames;
        this.a = a;
        this.b = b;
        this.endOfA = a.getNumFields();
    }

    @Override
    public Object get(String property) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (property.equals(fieldNames[i])) {
                return get(i);
            }
        }
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public int getNumFields() {
        return endOfA + b.getNumFields();
    }

    @Override
    public Object get(int index) {
        return index < endOfA ? a.get(index) : b.get(index - endOfA);
    }

    @Override
    public Object[] getValues() {
        Object[] res = new Object[endOfA + b.getNumFields()];
        for (int i = 0; i < res.length; i++) {
            res[i] = get(i);
        }
        return res;
    }

}
