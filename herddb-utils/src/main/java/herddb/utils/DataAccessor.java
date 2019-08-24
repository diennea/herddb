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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public interface DataAccessor {

    Object get(String property);

    default Map<String, Object> toMap() {
        HashMap<String, Object> res = new HashMap<>();
        forEach(res::put);
        return res;
    }

    String[] getFieldNames();

    default int getNumFields() {
        return getFieldNames().length;
    }

    default void forEach(BiConsumer<String, Object> consumer) {
        for (String property : getFieldNames()) {
            consumer.accept(property, get(property));
        }
    }

    default Object get(int index) {
        return get(getFieldNames()[index]);
    }

    default boolean fieldEqualsTo(int index, Object value) {
        Object val = get(index);
        return SQLRecordPredicateFunctions.objectEquals(val, value);
    }

    default int fieldCompareTo(int index, Object value) {
        Object val = get(index);
        return SQLRecordPredicateFunctions.compare(val, value);
    }

    default Object[] getValues() {
        String[] fieldNames = getFieldNames();
        Object[] result = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            result[i] = get(fieldNames[i]);
        }
        return result;
    }

    DataAccessor NULL = new AbstractDataAccessor() {

        @Override
        public void forEach(BiConsumer<String, Object> consumer) {
        }

        @Override
        public Object get(int index) {
            return null;
        }

        @Override
        public boolean fieldEqualsTo(int index, Object value) {
            return null == value;
        }

        @Override
        public int fieldCompareTo(int index, Object value) {
            return SQLRecordPredicateFunctions.compareNullTo(value);
        }


        @Override
        public Object[] getValues() {
            return Constants.EMPTY_OBJECT_ARRAY;
        }

        @Override
        public Object get(String property) {
            return null;
        }

        @Override
        public Map<String, Object> toMap() {
            return Collections.emptyMap();
        }

        @Override
        public String[] getFieldNames() {
            return Constants.EMPTY_STRING_ARRAY;
        }

    };

    // CHECKSTYLE.OFF: MethodName
    static DataAccessor ALL_NULLS(String[] fieldNames) {
        return new AllNullsDataAccessor(fieldNames);
    }
    // CHECKSTYLE.ON: MethodName

}
