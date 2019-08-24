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
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A simple implementation backed by a Map
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public class MapDataAccessor extends AbstractDataAccessor {

    private final Map<String, Object> map;
    private final String[] fieldNames;

    public MapDataAccessor(Map<String, Object> map, String[] fieldNames) {
        this.map = map;
        this.fieldNames = fieldNames;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public void forEach(BiConsumer<String, Object> consumer) {
        for (String columnName : fieldNames) {
            Object value = map.get(columnName);
            if (value != null) {
                consumer.accept(columnName, value);
            }
        }
    }

    @Override
    public Object get(String property) {
        return map.get(property);
    }

    @Override
    public Map<String, Object> toMap() {
        return map;
    }

}
