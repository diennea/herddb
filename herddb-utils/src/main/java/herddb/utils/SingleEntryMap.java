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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A map with a single entry
 *
 * @author enrico.olivelli
 */
public class SingleEntryMap<K, V> implements Map<K, V>, Serializable {

    private static final long serialVersionUID = 1;

    private final K key;
    private final V value;

    public SingleEntryMap(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return this.key.equals(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.value.equals(value);

    }

    @Override
    public V get(Object key) {
        if (Objects.equals(this.key, key)) {
            return value;
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<K> keySet() {
        return Collections.singleton(key);
    }

    @Override
    public Collection<V> values() {
        return Collections.singleton(value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Collections.singleton(new AbstractMap.SimpleImmutableEntry<>(key, value));
    }

}
