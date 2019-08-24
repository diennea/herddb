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
     * Track a page cache hit.
     *
     * @param page for which track an hit
     */
    void pageHit(Page<?> page);

    /**
     * Add a new {@code Page} to memory.
     * <p>
     * Adding a new page could force an older page to be unloaded
     * </p>
     *
     * @param page page to be added
     * @return selected page to be unloaded or {@code null}
     */
    Page.Metadata add(Page<?> page);

    /**
     * Remove a {@code Page} from memory.
     *
     * @param page page to be removed.
     * @return {@code true} if the memory really contained the given page
     */
    boolean remove(Page<?> page);

    /**
     * Remove many {@code Page Pages} from memory.
     * <p>
     * This method is logically equivalent to multiple {@link #remove(Page)} invocations but is expected
     * to be more efficient.
     * </p>
     *
     * @param pages pages to be removed.
     */
    <P extends Page<?>> void remove(Collection<P> pages);

    /**
     * Returns the current number of {@link Page Pages} memorized.
     *
     * @return current number of Pages
     */
    int size();

    /**
     * Returns the maximum number of {@link Page Pages} memorizable.
     *
     * @return maximum number of Pages
     */
    int capacity();

    /**
     * Clear any memorized data.
     */
    void clear();

}
