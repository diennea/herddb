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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import herddb.core.DataPage.DataPageMetaData;

/**
 * Basic implementation of ClockPro algorithm.
 *
 * Based on the original work:
 *
 * <pre>
 * CLOCK-Pro: An Effective Improvement of the CLOCK Replacement
 *
 * Song Jiang                                    Feng Chen and Xiaodong Zhang
 *
 * Performance and Architecture Laboratory (PAL) Computer Science Department
 * Los Alamos National Laboratory, CCS Division  College of William and Mary
 * Los Alamos, NM 87545, USA                     Williamsburg, VA 23187, USA
 * sjiang@lanl.gov                               fchen@cs.wm.edu, zhang@cs.wm.edu
 * </pre>
 * </p>
 *
 * @see http://web.cse.ohio-state.edu/hpcs/WWW/HTML/publications/papers/TR-05-3.pdf
 * @author diego.salvi
 */
public class ClockProPolicy implements PageReplacementPolicy {

    /** Value for {@link CPMetadata#warm}: hot page */
    private final byte HOT = 0x00;

    /** Value for {@link CPMetadata#warm}: cold page still loaded */
    private final byte COLD = 0x01;

    /** Value for {@link CPMetadata#warm}: old cold page unloaded */
    private final byte NON_RESIDENT_COLD = 0x10;


    /** Capacity */
    private final int m;

    /** Self tuned parameter (target capacity for cold entries) */
    int mc = 0;

    /** Count of hot occurrences */
    int countHot = 0;

    /** Count of cold occurrences */
    int countCold = 0;

    /** Count of cold non resident occurrences */
    int countNonResident = 0;

    /** Clock hand for {@link #hotSweep()} */
    private CPMetadata handHot;

    /** Clock hand for {@link #coldSweep()} */
    private CPMetadata handCold;

    /** Clock hand for {@link #testSweep()} */
    private CPMetadata handTest;

    /** Modification lock */
    private final Lock lock = new ReentrantLock();

    /** Known page space. */
    /*
     * TODO: could be replaced by a Set? Not really sure: a Map is safer (we handle our instances not
     * instances received from outside) and really an efficient Set is... backed by a Map!!!
     */
    private Map<CPMetadata,CPMetadata> space;

    public ClockProPolicy(int capacity) {
        super();

        this.m = capacity;

        this.space = new HashMap<>(2 * capacity);

        handHot = handCold = handTest;
    }

    @Override
    public CPMetadata add(DataPage page) {
        lock.lock();
        try {

            return unsafeAdd(page);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(DataPage page) {
        lock.lock();
        try {

            return unsafeRemove((CPMetadata) page.metadata);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(Collection<DataPage> pages) {
        lock.lock();
        try {
            for(DataPage page : pages) {
                unsafeRemove((CPMetadata) page.metadata);
            }
        } finally {
            lock.unlock();
        }

    }

    @Override
    public int size() {
        return countHot + countCold;
    }

    @Override
    public int capacity() {
        return m;
    }

    @Override
    public void clear() {
        lock.lock();
        try {

            mc = countHot = countCold = countNonResident = 0;
            handHot = handCold = handTest = null;

            space.clear();

        } finally {
            lock.unlock();
        }

    }

    @Override
    public ConcurrentMap<Long, DataPage> createObservedPagesMap() {
        return new ObservedMap();
    }

    /* *********************** */
    /* *** PRIVATE METHODS *** */
    /* *********************** */

    private CPMetadata unsafeAdd(DataPage page) {

        if (handHot == null) {

            /* Initialize hands */

            final CPMetadata metadata = new CPMetadata(page);

            metadata.reference = false;
            metadata.test = true;
            metadata.warm = COLD;

            page.metadata = metadata;

            /* Single element chanin */
            metadata.next = metadata;
            metadata.prev = metadata;

            space.put(metadata, metadata);

            handHot = handCold = handTest = metadata;

            ++countCold;

            return null;

        } else {

            if (countHot + countCold < m) {

                /* There is still space, no cold sweep needed */
                return unsafeAdd(page, false);

            } else {

                return unsafeAdd(page, true);

            }
        }

    }

    private CPMetadata unsafeAdd(DataPage page, boolean freespace) {

        /*
         * When there is a page fault, the faulted page must be a cold page.
         *
         * We first run HANDcold for a free space.
         *
         * ----------------------------
         *
         * If the faulted cold page is not in the list, its reuse distance is highly likely to be larger than
         * the recency of hot pages.
         *
         * So the page is still categorized as a cold page and is placed at the list head. The page also
         * initiates its test period.
         *
         * If the number of cold pages is larger than the threshold (Mc + M), we run HANDtest.
         *
         * ---------------------------
         *
         * If the cold page is in the list, the faulted page turns into a hot page and is placed at the head
         * of the list.
         *
         * We run HANDhot to turn a hot page with a large recency into a cold page.
         */

        /* We first run HANDcold for a free space. */
        final CPMetadata unloaded = freespace ? coldSweep() : null;

        final CPMetadata metadata = new CPMetadata(page);
        final CPMetadata known = space.get(metadata);

        if (known == null) {

            /*
             * If the faulted cold page is not in the list, its reuse distance is highly likely to be larger
             * than the recency of hot pages.
             */

            /*
             * So the page is still categorized as a cold page and is placed at the list head. The page also
             * initiates its test period.
             */

            metadata.reference = false;
            metadata.test = true;
            metadata.warm = COLD;

            page.metadata = metadata;

            space.put(metadata, metadata);

            insertBefore(metadata, handHot);

            ++countCold;


            /* If the number of cold pages is larger than the threshold (Mc + M), we run HANDtest. */
            if (countCold + countNonResident > mc + m ) {
                testSweep();
            }

        } else {

//            assertEquals(NON_RESIDENT_COLD,known.warm);

            if (known.warm != NON_RESIDENT_COLD) {
                throw new IllegalArgumentException("Added a page twice: " + page);
            }

            /*
             * If the cold page is in the list, the faulted page turns into a hot page and is placed at the
             * head of the list.
             */

            known.warm = HOT;
            page.metadata = known;

            moveBefore(known, handHot);

            ++countHot;
            --countNonResident;
//            assertTrue(countNonResident >= 0);

            if (known.test) {
                /* If a cold page is accessed during its test period, we increment Mc by 1. */
                ++mc;
//                assertTrue(mc <= m);
            }


            hotSweep();
        }

        return unloaded;

    }

    private boolean unsafeRemove(CPMetadata metadata) {

        /* Custom addition to CP algorithm, we need to drop pages too */

        final CPMetadata known = space.remove(metadata);

        if (known == null) {
            return false;
        }

        switch (known.warm) {

            case HOT:
                --countHot;
//                assertTrue(countHot >= 0);
                break;

            case COLD:
                --countCold;
//                assertTrue(countCold >= 0);
                break;

            case NON_RESIDENT_COLD:
                --countNonResident;
//                assertTrue(countNonResident >= 0);
                break;

        }

        delete(known);

        return false;

    }


    private CPMetadata coldSweep() {

        CPMetadata unloading = null;
        while(unloading == null) {

            if (handCold.warm == COLD) {
                /*
                 * HANDcold is used to search for a resident cold page for replacement.
                 *
                 * If the reference bit of the cold page currently pointed to by HANDcold is unset, we replace the
                 * cold page for a free space.
                 *
                 * The replaced cold page will remain in the list as a non-resident cold page until it runs out of its
                 * test period, if it is in its test period.
                 *
                 * If not, we move it out of the clock.
                 *
                 * However, if its bit is set and it is in its test period, we turn the cold page into a hot page, and
                 * ask HANDhot for its actions, because an access during the test period indicates a competitively
                 * small reuse distance.
                 *
                 * If its bit is set but it is not in its test period, there are no status
                 * change as well as HANDhot actions. In both of the cases, its reference bit is reset, and we move it
                 * to the list head. The hand will keep moving until it encounters a cold page eligible for
                 * replacement, and stops at the next resident cold page
                 */

//                assertEquals(COLD, handCold.item.warm);

                if (handCold.reference) {

                    if (handCold.test) {


                        /*
                         * However, if its bit is set and it is in its test period, we turn the cold page into a hot
                         * page, and ask HANDhot for its actions, because an access during the test period indicates a
                         * competitively small reuse distance.
                         */
                        handCold.warm = HOT;

                        --countCold;
//                        assertTrue(countCold >= 0);
                        ++countHot;

                        /* If a cold page is accessed during its test period, we increment Mc by 1. */
                        ++mc;
//                        assertTrue(mc <= m);

                        hotSweep();
                    }

                    /* In both of the cases, its reference bit is reset, and we move it to the list head. */
                    handCold.reference = false;

                    /*
                     * The hand will keep moving until it encounters a cold page eligible for replacement, and stops
                     * at the next resident cold page
                     */

                } else {

                    /*
                     * If the reference bit of the cold page currently pointed to by HANDcold is unset, we replace the
                     * cold page for a free space.
                     */

                    if (handCold.test) {

                        /*
                         * The replaced cold page will remain in the list as a non-resident cold page until it runs out
                         * of its test period, if it is in its test period.
                         */

                        handCold.warm = NON_RESIDENT_COLD;

                        // loop fin qui!
                        unloading = handCold;

                        --countCold;
//                        assertTrue(countCold >= 0);
                        ++countNonResident;

                        while (countNonResident > m) {
                            testSweep();
                        }

                    } else {

                        /* If not, we move it out of the clock. */
                        handCold = deleteAndStayStill(handCold);

                        --countCold;
//                        assertTrue(countCold >= 0);
                    }
                }
            }

            handCold = handCold.next;
        }

        /* The hand will keep moving until ... and stops at the next resident cold page */
        if (countCold > 0) {
            while(handCold.warm != COLD) {
                handCold = handCold.next;
            }
        }

        return unloading;

    }

    private void hotSweep() {

        /* Do not sweep, there is still space for hot pages */
        if (countHot < m - mc) {
            return;
        }

        /*
         * As mentioned above, what triggers the movement of HANDhot is that a cold page is found to have been
         * accessed in its test period and thus turns into a hot page, which maybe accordingly turns the hot
         * page with the largest recency into a cold page.
         *
         * If the reference bit of the hot page pointed to by HANDhot is unset, we can simply change its
         * status and then move the hand forward.
         *
         * However, if the bit is set, which indicates the page has been re-accessed, we spare this page,
         * reset its reference bit and keep it as a hot page. This is because the actual access time of the
         * hot page could be earlier than the cold page.
         *
         * Then we move the hand forward and do the same on the hot pages with their bits set until the hand
         * encounters a hot page with a reference bit of zero.
         *
         * Then the hot page turns into a cold page. Note that moving HANDhot forward is equivalent to leaving
         * the page it moves by at the list head.
         *
         * Whenever the hand encounters a cold page, it will terminate the page’s test period. The hand will
         * also remove the cold page from the clock if it is non-resident (the most probable case). It
         * actually does the work on the cold page on behalf of hand HANDtest.
         *
         * Finally the hand stops at a hot page.
         */


        /*
         * As mentioned above, what triggers the movement of HANDhot is that a cold page is found to have been
         * accessed in its test period and thus turns into a hot page, which maybe accordingly turns the hot
         * page with the largest recency into a cold page.
         */

        while(true) {

            if (handHot.warm == HOT) {

                if (handHot.reference) {

                    /*
                     * However, if the bit is set, which indicates the page has been re-accessed, we spare this page,
                     * reset its reference bit and keep it as a hot page. This is because the actual access time of
                     * the hot page could be earlier than the cold page.
                     */
                    handHot.reference = false;

                } else {

                    /*
                     * Then the hot page turns into a cold page. Note that moving HANDhot forward is
                     * equivalent to leaving the page it moves by at the list head.
                     */

                    handHot.warm = COLD;
                    ++countCold;
                    --countHot;
//                    assertTrue(countHot >= 0);

                    break;

                }

            } else {

                /*
                 * Whenever the hand encounters a cold page, it will terminate the page’s test period. The
                 * hand will also remove the cold page from the clock if it is non-resident (the most probable
                 * case). It actually does the work on the cold page on behalf of hand HANDtest.
                 */

                handHot.test = false;

                if (handHot.warm == NON_RESIDENT_COLD) {

                    --countNonResident;
//                    assertTrue(countNonResident >= 0);

                    deleteAndStayStill(handHot);

                } else {

                    if (!handHot.reference) {
                        /* If a cold page passes its test period without a re-access, we decrement Mc by 1 */
                        --mc;
//                        assertTrue(mc >= 0);
                    }

                }
            }

            /*
             * Then we move the hand forward and do the same on the hot pages with their bits set until the hand
             * encounters a hot page with a reference bit of zero.
             */
            handHot = handHot.next;

        }

        /* Finally the hand stops at a hot page. */
        if (countHot > 0) {
            do {
                handHot = handHot.next;
            } while(handHot.warm != HOT);
        }



    }

    private void testSweep() {

        while(handTest.warm == HOT) {
            handTest = handTest.next;
        }

        /*
         * We keep track of the number of non-resident cold pages.
         *
         * Once the number exceeds M, the memory size in the number of pages, we terminate the test period of
         * the cold page pointed to by HANDtest.
         *
         * We also remove it from the clock if it is a non-resident page. Because the cold page has used up
         * its test period without a re-access and has no chance to turn into a hot page with its next access.
         * HANDtest then moves forward and stops at the next cold page.
         */

        if (handTest.warm == NON_RESIDENT_COLD) {

            /*
             * We also remove it from the clock if it is a non-resident page. Because the cold page has used
             * up its test period without a re-access and has no chance to turn into a hot page with its next
             * access.
             */
            handTest = deleteAndAdvance(handTest);

            --countNonResident;
//            assertTrue(countNonResident >= 0);

        } else {
            /* If a cold page passes its test period without a re-access, we decrement Mc by 1 */
            if (!handTest.reference) {
                --mc;
//                assertTrue(mc >= 0);
            }
        }

        /* HANDtest then moves forward and stops at the next cold page. */
        while (handTest.warm == HOT) {
            handTest = handTest.next;
        }

    }

    /**
     * Delete given node from both linked list and node space and returns next node.
     */
    private CPMetadata deleteAndAdvance(CPMetadata node) {
        final CPMetadata next = node.next;

        delete(node);

        return next;
    }

    /**
     * Delete given node from both linked list and node space and returns previous node.
     */
    private CPMetadata deleteAndStayStill(CPMetadata node) {
        final CPMetadata prev = node.prev;

        delete(node);

        return prev;
    }

    /**
     * Delete given node from both linked list and node space.
     * <p>
     * Move forward hands references if needed
     * </p>
     */
    private void delete(CPMetadata node) {

        if (node == node.next) {
//            assertSame(node, node.prev);
//            assertSame(handHot, node);
//            assertSame(handCold, node);
//            assertSame(handTest, node);
            handCold = handHot = handTest = null;
        } else {

            if (handHot == node) {
                handHot = node.next;
            }
            if (handCold == node) {
                handCold = node.next;
            }
            if (handTest == node) {
                handTest = node.next;
            }
        }

        detach(node);

        space.remove(node);

    }

    /**
     * Insert a non referenced node in the linked list place just before given target node.
     */
    private void insertBefore(CPMetadata inserting, CPMetadata point) {
//        assertNull(inserting.next);
//        assertNull(inserting.prev);

        inserting.prev = point.prev;
        inserting.next = point;

        point.prev.next = inserting;
        point.prev = inserting;

    }

    /**
     * Move given node from his place in the linked list to just before given target node.
     */
    private void moveBefore(CPMetadata moving, CPMetadata point) {
//        assertNotNull(moving.next);
//        assertNotNull(moving.prev);

        if (moving == point)
            return;

        detach(moving);
        insertBefore(moving, point);
    }


    /**
     * Remove given node from the linked list.
     */
    private void detach(CPMetadata node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;

        node.next = node.prev = null;
    }



    /* *********************** */
    /* *** PRIVATE CLASSES *** */
    /* *********************** */

    /**
     * Really observes only map gets setting the page as referenced when <i>getted</i>.
     *
     * Just created pages will not be referenced if not accessed from this map (which is a good thing, new
     * pages shouldn't be <i>referenced</i> at their first use)
     *
     * @author diego.salvi
     */
    private static final class ObservedMap extends ConcurrentHashMap<Long, DataPage> {

        /** Default Serial Version UID */
        private static final long serialVersionUID = 1L;

        @Override
        public DataPage get(Object key) {

            final DataPage page = super.get(key);

            if (page != null && page.metadata != null) {
                /* Set the page as referenced */
                ((CPMetadata)page.metadata).reference = true;
            }

            return page;
        }
    }


    /**
     * Implementation of {@link DataPageMetaData} with all data needed for {@link ClockProPolicy}.
     *
     * @author diego.salvi
     */
    private static final class CPMetadata implements DataPageMetaData {

        public final TableManager owner;
        public final long pageId;

        public volatile boolean reference;
        public byte warm;
        public boolean test;

        /** Clock linked list forward reference */
        private CPMetadata prev;

        /** Clock linked list backward reference */
        private CPMetadata next;

        private final int hashcode;

        public CPMetadata(DataPage datapage) {
            this(datapage.owner, datapage.pageId);
        }

        public CPMetadata(TableManager owner, long pageId) {
            super();
            this.owner = owner;
            this.pageId = pageId;

            hashcode = Objects.hash(owner,pageId);

            reference = false;
        }

        @Override
        public int hashCode() {
            return hashcode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof CPMetadata)) {
                return false;
            }
            CPMetadata other = (CPMetadata) obj;
            if (owner == null) {
                if (other.owner != null) {
                    return false;
                }
            } else if (!owner.equals(other.owner)) {
                return false;
            }
            if (pageId != other.pageId) {
                return false;
            }
            return true;
        }

        @Override
        public TableManager getOwner() {
            return owner;
        }

        @Override
        public long getPageId() {
            return pageId;
        }

        @Override
        public String toString() {
            return "CPMetadata {pageId=" + pageId + ", owner=" + owner + ", reference=" + reference + " test=" + test + " warm=" + warm + '}';
        }
    }
}
