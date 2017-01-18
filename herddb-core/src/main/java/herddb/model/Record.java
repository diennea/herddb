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
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Objects;

/**
 * A generic record
 *
 * @author enrico.olivelli
 */
public class Record {

    public final Bytes key;
    public final Bytes value;
    private SoftReference<Map<String, Object>> cache;

    public Record(Bytes key, Bytes value) {
        this.key = key;
        this.value = value;
    }

    public synchronized Map<String, Object> toBean(Table table) {
        Map<String, Object> cached = cache != null ? cache.get() : null;
        if (cached == null) {
            cached = RecordSerializer.toBean(this, table);
            cache = new SoftReference<>(cached);
        }
        return cached;
    }

    @Override
    public String toString() {
        return "Record{" + "key=" + key + ", value=" + value + '}';
    }

    public void clearCache() {
        cache = null;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.key);
        hash = 53 * hash + Objects.hashCode(this.value);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
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
