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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import org.junit.Test;

/**
 * Checks how {@link MemoryManager} sizes the data, index and primary key page pools.
 *
 * <p>
 * The page replacement policy implementation is picked by the {@value #PAGE_REPLACEMENT_POLICY_PROPERTY}
 * system property and is resolved once, when {@link MemoryManager} is initialized. Each concrete subclass
 * selects one implementation from its own static initializer, so every subclass needs a fresh JVM: the build
 * provides one by never reusing a fork between test classes. Every check also asserts which implementation
 * the manager actually built, so a subclass whose selection did not take effect fails instead of silently
 * repeating the checks of another subclass.
 * </p>
 *
 * @author diego.salvi
 */
public abstract class MemoryManagerSuite {

    /**
     * System property selecting the page replacement policy implementation.
     *
     * <p>
     * Spelled out as a literal, and not derived from {@link MemoryManager}, so that a subclass can set it
     * without any risk of initializing the class that reads it beforehand.
     * </p>
     */
    protected static final String PAGE_REPLACEMENT_POLICY_PROPERTY = "herddb.core.MemoryManager.pageReplacementPolicy";

    private static final long PAGE_SIZE = 1024 * 1024L;

    /**
     * Page replacement policy implementation expected for the policy selected by this subclass.
     *
     * @return expected implementation class
     */
    protected abstract Class<? extends PageReplacementPolicy> expectedPolicyImplementation();

    /**
     * A dedicated index pool must be sized on index memory even when index memory is smaller than data memory.
     */
    @Test
    public void indexPoolSmallerThanDataPool() {
        MemoryManager manager = new MemoryManager(64 * PAGE_SIZE, 8 * PAGE_SIZE, 16 * PAGE_SIZE, PAGE_SIZE);

        assertExpectedImplementations(manager);

        assertNotSame("index pages must not share the data pool when index memory is configured",
                manager.getDataPageReplacementPolicy(), manager.getIndexPageReplacementPolicy());

        assertEquals("data pool capacity", 64, manager.getDataPageReplacementPolicy().capacity());
        assertEquals("index pool capacity", 8, manager.getIndexPageReplacementPolicy().capacity());
        assertEquals("primary key pool capacity", 16, manager.getPKPageReplacementPolicy().capacity());
    }

    /**
     * A dedicated index pool must be sized on index memory even when index memory is larger than data memory.
     */
    @Test
    public void indexPoolLargerThanDataPool() {
        MemoryManager manager = new MemoryManager(8 * PAGE_SIZE, 64 * PAGE_SIZE, 16 * PAGE_SIZE, PAGE_SIZE);

        assertExpectedImplementations(manager);

        assertNotSame("index pages must not share the data pool when index memory is configured",
                manager.getDataPageReplacementPolicy(), manager.getIndexPageReplacementPolicy());

        assertEquals("data pool capacity", 8, manager.getDataPageReplacementPolicy().capacity());
        assertEquals("index pool capacity", 64, manager.getIndexPageReplacementPolicy().capacity());
        assertEquals("primary key pool capacity", 16, manager.getPKPageReplacementPolicy().capacity());
    }

    /**
     * Without index memory there is no dedicated index pool: index pages are handled by the very same policy
     * instance used for data pages, sharing its capacity.
     */
    @Test
    public void indexPoolDisabledSharesTheDataPool() {
        MemoryManager manager = new MemoryManager(64 * PAGE_SIZE, 0, 16 * PAGE_SIZE, PAGE_SIZE);

        assertExpectedImplementations(manager);

        assertSame("index pages must share the data pool when no index memory is configured",
                manager.getDataPageReplacementPolicy(), manager.getIndexPageReplacementPolicy());

        assertEquals("data pool capacity", 64, manager.getDataPageReplacementPolicy().capacity());
        assertEquals("primary key pool capacity", 16, manager.getPKPageReplacementPolicy().capacity());
    }

    private void assertExpectedImplementations(MemoryManager manager) {
        Class<? extends PageReplacementPolicy> expected = expectedPolicyImplementation();

        assertSame("data page replacement policy implementation", expected,
                manager.getDataPageReplacementPolicy().getClass());
        assertSame("index page replacement policy implementation", expected,
                manager.getIndexPageReplacementPolicy().getClass());
        assertSame("primary key page replacement policy implementation", expected,
                manager.getPKPageReplacementPolicy().getClass());
    }

}
