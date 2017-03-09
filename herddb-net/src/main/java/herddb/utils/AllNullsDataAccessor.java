/*
 * Copyright 2017 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.utils;

import java.util.function.BiConsumer;

/**
 * A DataAccessor without values, only schema
 *
 * @author enrico.olivelli
 */
public class AllNullsDataAccessor implements DataAccessor {

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
    public Object[] getValues() {
        return values;
    }

}
