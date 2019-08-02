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

import herddb.core.Page;
import herddb.core.PageReplacementPolicy;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
 *
 * See http://web.cse.ohio-state.edu/hpcs/WWW/HTML/publications/papers/TR-05-3.pdf
 *
 * @author diego.salvi
 */
public class ClockProPolicy implements PageReplacementPolicy {

    /** Class logger */
    private static final Logger LOGGER = Logger.getLogger(ClockProPolicy.class.getName());

    /**
     * This <i>constants</i> rules out unecessary and expensive logs at compile level if set to {@code true}.
     * Note: it <b>must</b> be static final to succesfully work.
     */
    private static final boolean COMPILE_EXPENSIVE_LOGS = false;

    /** Value for {@link CPMetadata#warm}: hot page */
    private final static byte HOT = 1;

    /** Value for {@link CPMetadata#warm}: cold page still loaded */
    private final static byte COLD = 2;

    /** Value for {@link CPMetadata#warm}: old cold page unloaded */
    private final static byte NON_RESIDENT_COLD = 4;


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
        this.mc = capacity;

        this.space = new HashMap<>(2 * capacity);

    }

    @Override
    public CPMetadata add(Page<?> page) {

        LOGGER.log(Level.FINER, () -> "Adding page " + page);

        final CPMetadata remove;

        lock.lock();
        try {

            remove = unsafeAdd(page);

        } finally {
            lock.unlock();
        }

        if (remove == null) {
            LOGGER.log(Level.FINER, () -> "Added page " + page + ", no page removal needed");
        } else {
            LOGGER.log(Level.FINER, () -> "Added page " + page + ", page selected for removal: " + remove);
        }

        return remove;
    }

    @Override
    public boolean remove(Page<?> page) {

        LOGGER.log(Level.FINER, () -> "Removing page " + page);

        boolean removed;

        lock.lock();
        try {
            removed = unsafeRemove((CPMetadata) page.metadata);
        } finally {
            lock.unlock();
        }

        if (removed) {
            LOGGER.log(Level.FINER, () -> "Removed page " + page);
        } else {
            LOGGER.log(Level.FINER, () -> "Unknown page " + page);
        }

        return removed;
    }

    @Override
    public <P extends Page<?>> void remove(Collection<P> pages) {
        lock.lock();
        try {
            for(Page<?> page : pages) {
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
    public void pageHit(Page<?> page) {
        hit(page);
    }

    /* *********************** */
    /* *** PRIVATE METHODS *** */
    /* *********************** */

    private CPMetadata unsafeAdd(Page<?> page) {

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

    private CPMetadata unsafeAdd(Page<?> page, boolean freespace) {

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Status[add started]: "
                    + "m = {0}, mc = {1}, countHot = {2}, countCold = {3}, countNonResident = {4}, "
                    + "handHot = {5}, handCold = {6}, handTest = {7}, "
                    + "freeingspace = {8}, adding {9}",
                    new Object[] {m, mc, countHot, countCold, countNonResident, handHot, handCold, handTest, freespace, page});

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

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Inserting unknown page {0}", page);

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

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Inserted unknown page {0}: "
                        + "m = {1}, mc = {2}, countCold = {3}, countNonResident = {4}",
                        new Object[] {page, m, mc, countCold, countNonResident});

            /* If the number of cold pages is larger than the threshold (Mc + M), we run HANDtest. */
            if (countCold + countNonResident > mc + m ) {
                testSweep();
            }

        } else {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Inserting known page {0}", page);

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

            if (known.test) {
                /* If a cold page is accessed during its test period, we increment Mc by 1. */
                ++mc;
                mc = Math.min(m, mc);
                if (mc < 0 || mc > m)
                    throw new IllegalStateException("Mc doens't match 0 <= mc <= m: mc " + mc + " m " + m);

                if (COMPILE_EXPENSIVE_LOGS)
                    LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Inserted known page during test {0}: mc = {1}",
                            new Object[] {page, mc});
            }


            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Inserted known page {0}: "
                        + "m = {0}, mc = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {page, m, mc, countCold, countNonResident});

            hotSweep();
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Status[add completed]: "
                    + "m = {0}, mc = {1}, countHot = {2}, countCold = {3}, countNonResident = {4}, "
                    + "handHot = {5}, handCold = {6}, handTest = {7}, "
                    + "replaced {8}",
                    new Object[] {m, mc, countHot, countCold, countNonResident, handHot, handCold, handTest, unloaded});

        return unloaded;

    }

    private boolean unsafeRemove(CPMetadata metadata) {

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Status[remove started]: "
                    + "m = {0}, mc = {1}, countHot = {2}, countCold = {3}, countNonResident = {4}, "
                    + "handHot = {5}, handCold = {6}, handTest = {7}, "
                    + "removing {8}",
                    new Object[] {m, mc, countHot, countCold, countNonResident, handHot, handCold, handTest, metadata.pageId});


        /* Custom addition to CP algorithm, we need to drop pages too */

        final CPMetadata known = space.remove(metadata);

        if (known == null) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Status[remove ended] unknown page: "
                        + "m = {0}, mc = {1}, countHot = {2}, countCold = {3}, countNonResident = {4}, "
                        + "handHot = {5}, handCold = {6}, handTest = {7}, "
                        + "removing {8}",
                        new Object[] {m, mc, countHot, countCold, countNonResident, handHot, handCold, handTest, metadata.pageId});

            return false;
        }

        switch (known.warm) {

            case HOT:
                --countHot;
                break;

            case COLD:
                --countCold;
                break;

            case NON_RESIDENT_COLD:
                --countNonResident;
                break;

            default:
                throw new IllegalStateException("Unknown warm state " + known.warm);

        }

        delete(known);

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Status[remove ended]: "
                    + "m = {0}, mc = {1}, countHot = {2}, countCold = {3}, countNonResident = {4}, "
                    + "handHot = {5}, handCold = {6}, handTest = {7}, "
                    + "removing {8}",
                    new Object[] {m, mc, countHot, countCold, countNonResident, handHot, handCold, handTest, metadata.pageId});

        return true;

    }


    private CPMetadata coldSweep() {

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: starting: "
                    + "mc = {0}, countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {mc, countHot, countCold, countNonResident});

        CPMetadata unloading = null;
        while(unloading == null) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: checking handCold {0} warm {1}",
                        new Object[] {handCold.pageId, handCold.warm});


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

                if (handCold.reference) {

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: testing cold referenced {0}", handCold.pageId);

                    if (handCold.test) {


                        /*
                         * However, if its bit is set and it is in its test period, we turn the cold page into a hot
                         * page, and ask HANDhot for its actions, because an access during the test period indicates a
                         * competitively small reuse distance.
                         */
                        handCold.warm = HOT;

                        --countCold;
                        ++countHot;

                        /* If a cold page is accessed during its test period, we increment Mc by 1. */
                        ++mc;

                        mc = Math.min(m,mc);
                        if (mc < 0 || mc > m)
                            throw new IllegalStateException("Mc doens't match 0 <= mc <= m: mc " + mc + " m " + m);

                        if (COMPILE_EXPENSIVE_LOGS)
                            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: referenced cold {0} promoted to hot: "
                                    + "mc = {1}, countHot = {2}, countCold = {3}",
                                    new Object[] {handCold.pageId, mc, countHot, countCold});

                        hotSweep();
                    }

                    /* In both of the cases, its reference bit is reset, and we move it to the list head. */
                    handCold.reference = false;

                    /*
                     * The hand will keep moving until it encounters a cold page eligible for replacement, and stops
                     * at the next resident cold page
                     */

                } else {

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: testing cold not referenced {0}", handCold.pageId);

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
                        ++countNonResident;

                        if (COMPILE_EXPENSIVE_LOGS)
                            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: not referenced cold {0} downgraded to non resident (but kept): "
                                    + "mc = {1}, countCold = {2}, countNonResident = {3}",
                                    new Object[] {handCold.pageId, mc, countCold, countNonResident});

                        while (countNonResident > m) {
                            testSweep();
                        }

                    } else {

                        // loop fin qui!
                        unloading = handCold;

                        --countCold;

                        if (COMPILE_EXPENSIVE_LOGS)
                            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: referenced cold {0} deleted: countCold = {1}",
                                    new Object[] {handCold.pageId, countCold});

                        /* If not, we move it out of the clock. */
                        handCold = deleteAndStayStill(handCold);

                        if (COMPILE_EXPENSIVE_LOGS)
                            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: backward handCold after delete to {0}:",
                                    handCold.pageId);

                    }
                }
            }

            handCold = handCold.next;

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: advanced handCold to {0}:", handCold.pageId);
        }

        /* The hand will keep moving until ... and stops at the next resident cold page */
        if (countCold > 0) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: final not cold skip start:"
                        + "countHot = {0}, countCold = {1}, countNonResident = {2}",
                        new Object[] {countHot, countCold, countNonResident});

            int notcoldskip = 0;
            while(handCold.warm != COLD) {
                handCold = handCold.next;
                if (COMPILE_EXPENSIVE_LOGS) ++notcoldskip;
            }

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: final not cold skip end {0}:"
                        + "countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {notcoldskip, countHot, countCold, countNonResident});
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Cold Sweep: exiting: "
                    + "mc = {0}, countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {mc, countHot, countCold, countNonResident});

        return unloading;

    }

    private void hotSweep() {

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: starting: "
                    + "mc = {0}, countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {mc, countHot, countCold, countNonResident});

        /* Do not sweep, there is still space for hot pages */
        if (countHot < m - mc) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: skipping due to still available space: "
                        + "m = {0}, mc = {1}, countHot = {2}",
                            new Object[] {m, mc, countHot});

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

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: checking handHot {0} warm {1}",
                        new Object[] {handHot.pageId, handHot.warm});

            if (handHot.warm == HOT) {

                if (handHot.reference) {

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: testing hot referenced {0}", handHot.pageId);

                    /*
                     * However, if the bit is set, which indicates the page has been re-accessed, we spare this page,
                     * reset its reference bit and keep it as a hot page. This is because the actual access time of
                     * the hot page could be earlier than the cold page.
                     */
                    handHot.reference = false;

                } else {

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: testing hot not referenced {0}", handHot.pageId);

                    /*
                     * Then the hot page turns into a cold page. Note that moving HANDhot forward is
                     * equivalent to leaving the page it moves by at the list head.
                     */

                    handHot.warm = COLD;
                    ++countCold;
                    --countHot;

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: not referenced hot {0} downgraded to cold (but kept): "
                                + "mc = {1}, countHot = {2}, countCold = {3}",
                                new Object[] {handHot.pageId, mc, countHot, countCold});

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

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: non resident {0} deleted: countNonResident = {1}",
                                new Object[] {handHot.pageId, countNonResident});

                    /* If not, we move it out of the clock. */
                    handHot = deleteAndStayStill(handHot);

                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: backward handHot after delete to {0}:",
                                handHot.pageId);

                } else {

                    if (!handHot.reference) {
                        /* If a cold page passes its test period without a re-access, we decrement Mc by 1 */

                        --mc;
                        mc = Math.max(0, mc);
                        if (mc < 0 || mc > m)
                            throw new IllegalStateException("Mc doens't match 0 <= mc <= m: mc " + mc + " m " + m);

                        if (COMPILE_EXPENSIVE_LOGS)
                            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: checked cold not referenced {0}: mc = {1}",
                                    new Object[] {handHot.pageId, mc});
                    }

                }
            }

            /*
             * Then we move the hand forward and do the same on the hot pages with their bits set until the hand
             * encounters a hot page with a reference bit of zero.
             */
            handHot = handHot.next;

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: advanced handHot to {0}:", handHot.pageId);

        }

        /* Finally the hand stops at a hot page. */
        if (countHot > 0) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: final not hot skip start:"
                        + "countHot = {0}, countCold = {1}, countNonResident = {2}",
                        new Object[] {countHot, countCold, countNonResident});

            int nothotskip = 0;

            do {
                handHot = handHot.next;
                if (COMPILE_EXPENSIVE_LOGS) ++nothotskip;
            } while(handHot.warm != HOT);

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: final not hot skip end {0}:"
                        + "countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {nothotskip, countHot, countCold, countNonResident});
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Hot Sweep: exiting: "
                    + "mc = {0}, countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {mc, countHot, countCold, countNonResident});

    }

    private void testSweep() {

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: starting: "
                    + "mc = {0}, countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {mc, countHot, countCold, countNonResident});

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: initial hot skip start: countHot = {0}", new Object[] {countHot});

        int hotskip = 0;
        while(handTest.warm == HOT) {
            handTest = handTest.next;
            if (COMPILE_EXPENSIVE_LOGS) ++hotskip;
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: initial hot skip end {0}: countHot = {1}", new Object[] {hotskip, countHot});

        /*
         * We keep track of the number of non-resident cold pages.
         *
         * Once the number exceeds M, the memory size in the number of pages, we terminate the test period of
         * the cold page pointed to by HANDtest.
         *
         * We also remove it from the clock if it is a non-resident page. Because the cold page has used up
         * its test period without a re-access and has no chance to turn into a hot page with its next access.
         * HANDtest then moves forward and stops at the next cold page.
         *
         * Note the aforementioned cold pages include resident and non-resident cold pages.
         */

        /*
         * We terminate the test period of the cold page pointed to by HANDtest.
         */
        handTest.test = false;

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: checking handTest {0} warm {1}",
                new Object[] {handTest.pageId, handTest.warm});

        if (handTest.warm == NON_RESIDENT_COLD) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: deleting non resident {0}", handTest.pageId);

            /*
             * We also remove it from the clock if it is a non-resident page. Because the cold page has used
             * up its test period without a re-access and has no chance to turn into a hot page with its next
             * access.
             */
            handTest = deleteAndStayStill(handTest);

            --countNonResident;

            --mc;
            mc = Math.max(0, mc);
            if (mc < 0 || mc > m)
                throw new IllegalStateException("Mc doens't match 0 <= mc <= m: mc " + mc + " m " + m);

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: advanced handTest after delete to {0}: mc = {1}, countNonResident = {2}",
                        new Object[] {handTest.pageId, mc, countNonResident});

        } else {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: checking cold resident {0}", handTest.pageId);

            /* If a cold page passes its test period without a re-access, we decrement Mc by 1 */
            if (!handTest.reference) {
                --mc;
                mc = Math.max(0, mc);
                if (mc < 0 || mc > m)
                    throw new IllegalStateException("Mc doens't match 0 <= mc <= m: mc " + mc + " m " + m);

                if (COMPILE_EXPENSIVE_LOGS)
                    LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: unreferenced cold resident {0}: mc = {1}",
                            new Object[] {handTest.pageId, mc});
            }

        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: final hot skip start: countHot = {0}", new Object[] {countHot});

        /* Advance */
        handTest = handTest.next;

        /* HANDtest then moves forward and stops at the next cold page. */
        hotskip = 0;
        while (handTest.warm == HOT) {
            handTest = handTest.next;
            if (COMPILE_EXPENSIVE_LOGS) ++hotskip;
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: final hot skip end {0}: countHot = {1}", new Object[] {hotskip, countHot});

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, System.identityHashCode(this) + " m " + m + " Test Sweep: exiting: "
                    + "mc = {0}, countHot = {1}, countCold = {2}, countNonResident = {3}",
                        new Object[] {mc, countHot, countCold, countNonResident});

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

    /** Signal a page hit if really the page exists and ase the right metadata */
    private static final <P extends Page<?>> P hit(P page) {

        if (page != null && page.metadata != null) {
            /* Set the page as referenced */
            ((CPMetadata)page.metadata).reference = true;
        }

        return page;
    }

    /* *********************** */
    /* *** PRIVATE CLASSES *** */
    /* *********************** */

    /**
     * Implementation of {@link Page.Metadata} with all data needed for {@link ClockProPolicy}.
     *
     * @author diego.salvi
     */
    private static final class CPMetadata extends Page.Metadata {

        public volatile boolean reference;
        public byte warm;
        public boolean test;

        /** Clock linked list forward reference */
        private CPMetadata prev;

        /** Clock linked list backward reference */
        private CPMetadata next;

        private final int hashcode;

        public CPMetadata(Page<?> page) {
            this(page.owner, page.pageId);
        }

        public CPMetadata(Page.Owner owner, long pageId) {
            super(owner,pageId);

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
        public String toString() {
            return "CPMetadata {pageId=" + pageId + ", owner=" + owner + ", reference=" + reference + " test=" + test + " warm=" + warm + '}';
        }
    }
}
