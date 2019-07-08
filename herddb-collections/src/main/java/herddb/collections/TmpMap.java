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
package herddb.collections;

import java.lang.reflect.InvocationTargetException;

/**
 * This Map is useful to store an huge amount of data locally, like caching the results of an huge record set. We don't
 * want to fill the heap with data. The underlying HerdDB database will swap to disk data that are not needed.
 * <p>
 * Null keys and null values are not supported.
 * <p>
 * The expected workflow is:
 * <ul>
 * <li>Load the map
 * <li>Perform lookups
 * <li>Close the map
 * </ul>.
 * <p>
 * <b>The map is not expected to be thread safe. Do not wrap it with an external synchronization mechanism as it is
 * anyway sharing internal data structures with every other collection allocated by the same CollectionsManager</b>
 *
 * @author enrico.olivelli
 */
public interface TmpMap<K, V> extends AutoCloseable {

    /**
     * Map a key to a value, if the key is already mapped to a value it will be mapped to the new value. Null keys and
     * values are not supported. Current implementation supposes that the map does not contain the mapping. In the
     * future we can have
     *
     * @param key
     * @param value
     * @throws Exception
     */
    public void put(K key, V value) throws CollectionsException;

    /**
     * Remove a mapping. Noop if a mapping did not exist.
     *
     * @param key
     * @throws Exception
     */
    public void remove(K key) throws CollectionsException;

    /**
     * Retrieve a mapping or null if the key is not mapped. Null keys and null values are not supported.
     *
     * @param key
     * @return
     * @throws Exception
     */
    V get(K key) throws CollectionsException;

    /**
     * Check if a key is mapped to a value.
     *
     * @param key
     * @return true if a mapping exists.
     * @throws Exception
     */
    boolean containsKey(K key) throws CollectionsException;

    /**
     * Checks if the system started to swap data to disk. This fact does not directly depend on the size of the current
     * map but by the overall usage of resources.
     *
     * @return true if the collection has data written to disk
     */
    boolean isSwapped();

    /**
     * Return the exact number of mappings in the map.
     *
     * @return the number of mappings
     */
    long size();

    /**
     * Estimate the current amount of memory held from this structure. This counter refers only to data that is
     * currently loaded into memory. there is no way to know how much data is written to disk.
     *
     * @return the memory used currently by this map on the Heap.
     */
    long estimateCurrentMemoryUsage();

    /**
     * Scan the collection. Any exception thrown by the Sink will be re-throw wrapped by an InvocationTargetException
     *
     * @param sink
     * @throws InvocationTargetException
     */
    void forEach(BiSink<K, V> sink) throws CollectionsException, InvocationTargetException;

    /**
     * Scan the collection. Any exception thrown by the Sink will be re-throw wrapped by an InvocationTargetException
     *
     * @param sink
     * @throws InvocationTargetException
     */
    void forEachKey(Sink<K> sink) throws CollectionsException, InvocationTargetException;

    @Override
    void close();
}
