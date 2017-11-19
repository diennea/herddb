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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.function.BiConsumer;

/**
 * Wraps and projects a given DataAccessor
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ProjectedDataAccessor extends AbstractDataAccessor {

    private final String[] fieldNames;
    private final DataAccessor wrapped;

    public ProjectedDataAccessor(String[] fieldNames, DataAccessor wrapped) {
        this.fieldNames = fieldNames;
        this.wrapped = wrapped;
    }

    @Override
    public Object get(String property) {
        return wrapped.get(property);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
        for (String fieldName : fieldNames) {
            Object value = wrapped.get(fieldName);
            consumer.accept(fieldName, value);
        }
    }

    @Override
    public Object get(int index) {
        String fieldName = fieldNames[index];
        return wrapped.get(fieldName);
    }

    @Override
    public Object[] getValues() {
        Object[] data = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            data[i] = wrapped.get(fieldNames[i]);
        }
        return data;
    }

    public DataAccessor getWrapped() {
        return wrapped;
    }

}
