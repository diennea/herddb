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
package herddb.model;

import herddb.codec.RecordSerializer;
import herddb.utils.Bytes;
import java.util.Map;
import java.util.Objects;

/**
 * A generic record
 *
 * @author enrico.olivelli
 */
public final class Record {

    public final Bytes key;
    public final Bytes value;
    private Map<String, Object> cache;

    public Record(Bytes key, Bytes value) {
        this.key = key;
        this.value = value;
    }

    public Record(Bytes key, Bytes value, Map<String, Object> cache) {
        this.key = key;
        this.value = value;
        this.cache = cache;
    }

    public final Map<String, Object> toBean(Table table) {
        Map<String, Object> cached = cache;
        if (cached != null) {
            return cached;
        }
        cache = RecordSerializer.toBean(this, table);
        return cache;
    }

    @Override
    public final String toString() {
        return "Record{" + "key=" + key + ", value=" + value + '}';
    }

    public final void clearCache() {
        cache = null;
    }

    @Override
    public final int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.key);
        hash = 53 * hash + Objects.hashCode(this.value);
        return hash;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Record other = (Record) obj;
        if (!Objects.equals(this.key, other.key)) {
            return false;
        }
        if (!Objects.equals(this.value, other.value)) {
            return false;
        }
        return true;
    }

}
