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
package herddb.core;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import herddb.core.DataPage.DataPageMetaData;

/**
 * Cache replacement policy for memory pagination.
 *
 * <p>
 * A {@link PageReplacementPolicy} will not load or unload memory pages but will keep track of their usage and
 * instruct the system about which page need to be replaced.
 * </p>
 *
 * @author diego.salvi
 */
public interface PageReplacementPolicy {

    /**
     * Add a new {@code DataPage} to memory.
     * <p>
     * Adding a new page could force an older page to be unloaded
     * </p>
     *
     * @param page page to be added
     * @return selected page to be unloaded or {@code null}
     */
    public DataPageMetaData add(DataPage page);

    /**
     * Remove a {@code DataPage} from memory.
     *
     * @param page page to be removed.
     * @return {@code true} if the memory really contained the given page
     */
    public boolean remove(DataPage page);

    /**
     * Remove many {@code DataPage DataPages} from memory.
     * <p>
     * This method is logically equivalent to multiple {@link #remove(DataPage)} invocations but is expected
     * to be more efficient.
     * </p>
     *
     * @param pages pages to be removed.
     */
    public void remove(Collection<DataPage> pages);

    /**
     * Returns the current number of {@link DataPage DataPages} memorized.
     *
     * @return current number of <tt>DataPages</tt>
     */
    public int size();

    /**
     * Returns the maximum number of {@link DataPage DataPages} memorizable.
     *
     * @return maximum number of <tt>DataPages</tt>
     */
    public int capacity();

    /**
     * Clear any memorized data.
     */
    public void clear();

    /**
     * Returns a {@link ConcurrentMap} which must be used to store and access pages.
     * <p>
     * Map access can be observed by the {@link PageReplacementPolicy} for its internal calculations.
     * </p>
     *
     * @return a map to store pages
     */
    public ConcurrentMap<Long, DataPage> createObservedPagesMap();

}
