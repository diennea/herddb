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

package herddb.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.function.BiConsumer;

/**
 * A DataAccessor without values, only schema
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class AllNullsDataAccessor extends AbstractDataAccessor {

    private final String[] fieldNames;
    private final Object[] values;

    public AllNullsDataAccessor(String[] fieldNames) {
        this.fieldNames = fieldNames;
        this.values = Constants.getConstantEmptyArray(fieldNames.length);
    }

    @Override
    public Object get(String property) {
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
        for (String fieldName : fieldNames) {
            consumer.accept(fieldName, null);
        }
    }

    @Override
    public Object get(int index) {
        return null;
    }

    @Override
    public boolean fieldEqualsTo(int index, Object value) {
        return value == null;
    }

    @Override
    public int fieldCompareTo(int index, Object value) {
        return SQLRecordPredicateFunctions.compareNullTo(value);
    }

    @Override
    public Object[] getValues() {
        return values;
    }

}
