package herddb.core;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.utils.ListWithMap;

/**
 * Basic implementation of CAR algorithm.
 *
 * Based on the original work:
 *
 * <pre>
 * CAR: Clock with Adaptive Replacement
 *
 * Sorav Bansal† and Dharmendra S. Modha‡
 *
 * †Stanford University,
 * ‡IBM Almaden Research Center
 * </pre>
 * </p>
 *
 * @see http://www-cs.stanford.edu/~sbansal/pubs/fast04.pdf
 * @author diego.salvi
 */
public class ClockAdaptiveReplacement implements PageReplacementPolicy {

    /** Class logger */
    private static final Logger LOGGER = Logger.getLogger(ClockAdaptiveReplacement.class.getName());

    /**
     * This <i>constants</i> rules out unecessary and expensive logs at compile level if set to {@code true}.
     * Note: it <b>must</b> be static final to succesfully work.
     */
    private static final boolean COMPILE_EXPENSIVE_LOGS = true;

    /** Capacity */
    private final int c;

    /** Recency clock */
    private final ListWithMap<DataPage> t1;

    /** Frequency clock */
    private final ListWithMap<DataPage> t2;

    /** Unloaded recency */
    private final ListWithMap<Long> b1;

    /** Unloaded frequency */
    private final ListWithMap<Long> b2;

    /** Self tuned parameter (target size of T1) */
    private int p;

    /** Modification lock */
    private final Lock lock = new ReentrantLock();

    public ClockAdaptiveReplacement(int capacity) {

        c = capacity;
        p = 0;

        t1 = new ListWithMap<>();
        t2 = new ListWithMap<>();
        b1 = new ListWithMap<>();
        b2 = new ListWithMap<>();
    }

    @Override
    public int capacity() {
        return c;
    }

    @Override
    public int size() {
        return t1.size() + t2.size();
    }

    @Override
    public DataPage add(DataPage page) {
        lock.lock();
        try {
            return unsafeAdd(page);
        } finally {
            lock.unlock();
        }
    }

    public DataPage pop() {
        lock.lock();
        try {
            return unsafeReplace();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(Collection<DataPage> pages) {
        lock.lock();
        try {
            for(DataPage page : pages) {
                unsafeRemove(page);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(DataPage page) {
        lock.lock();
        try {
            return unsafeRemove(page);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            t1.clear();
            t2.clear();

            b1.clear();
            b2.clear();

            p = 0;
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

    private DataPage unsafeAdd(DataPage page) {

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, "Status[add started]: p = {0}, |T1| = {1}, |T2| = {2}, |B1| = {3}, |B2| = {4}, adding {5}",
                    new Object[] {p, t1.size(), t2.size(), b1.size(), b2.size(), page});

        /* Assume che sia avvenuto un cache miss e sia stato necessario caricare una pagina. */

//        4:  if (|T1| + |T2| = c) then
//              /* cache full, replace a page from cache */
//        5:    replace()
//              /* cache directory replacement */
//        6:    if ((x is not in B1 ∪ B2) and (|T1| + |B1| = c)) then
//        7:      Discard the LRU page in B1.
//        8:    elseif ((|T1| + |T2| + |B1| + |B2| = 2c) and (x is not in B1 ∪ B2)) then
//        9:      Discard the LRU page in B2.
//        10: endif

        /*
         * Diamo per assodato che la cache NON contiene la pagina in questione. Dato dunque il resto del
         * codice se v1 o b2 contengono la pagina lo faranno sempre finché non esplicitamente rimossi (ci sono
         * dei poll nel cache directory replacement ma vengono eseguiti solo se b1 E b2 NON contengono la
         * pagina).
         */

        /* Explicit boxing just to avoid too many implicit ones */
        final Long pageId = page.pageId;

        final boolean b1Hit = b1.contains(pageId);
        final boolean b2Hit = b2.contains(pageId);

        DataPage replaced = null;
        if (t1.size() + t2.size() == c) {

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "T1 + T2 = c: looking for a replacement for {0}", page);

            /* cache full, replace a page from cache */
            replaced = unsafeReplace();

            /* cache directory replacement */
            if (!b1Hit && !b2Hit) {
                if (t1.size() + b1.size() == c) {
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T1 + B1 = c: discarding B1 {0}", b1.peek());
                    b1.poll();
                } else if (t1.size() + t2.size() + b1.size() + b2.size() == 2*c){
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T1 + T2 + B1 + B2 = 2c: discarding B2 {0}", b2.peek());
                    b2.poll();
                }
            }

        }

//        /* cache directory miss */
//        12: if (x is not in B1 ∪ B2) then
//        13:   Insert x at the tail of T1. Set the page reference bit of x to 0.
//              /* cache directory hit */
//        14: elseif (x is in B1) then
//        15:   Adapt: Increase the target size for the list T1 as: p = min {p + max{1, |B2|/|B1|}, c}
//        16:   Move x at the tail of T2. Set the page reference bit of x to 0.
//        /* cache directory hit */
//        17: else /* x must be in B2 */
//        18: Adapt: Decrease the target size for the list T1 as: p = max {p − max{1, |B1|/|B2|}, 0}
//        19: Move x at the tail of T2. Set the page reference bit of x to 0.
//        20: endif

        if (!b1Hit && !b2Hit) {
            /* cache directory miss */

            /* Insert x at the tail of T1. Set the page reference bit of x to 0. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Not in B1 U B2: insert {0} into T1 tail", page);

            page.reference = false;
            t1.append(page);

        } else if (b1Hit) {
            /* cache directory hit */
            /* Adapt: Increase the target size for the list T1 as: p = min {p + max{1, |B2|/|B1|}, c} */
            p = Math.min(p + Math.max(1, b2.size() / b1.size()), c);

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Adapt: p = min {p + max{1, |B2|/|B1|}, c} = {0}", p);

            /* Move x at the tail of T2. Set the page reference bit of x to 0. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "In B1: insert {0} into T2 tail", page);

            page.reference = false;
            b1.remove(pageId);
            t2.append(page);

        } else {
            /* x must be in B2 */

            /* Adapt: Decrease the target size for the list T1 as: p = max {p − max{1, |B1|/|B2|}, 0} */
            p = Math.max(p - Math.max(1, b1.size() / b2.size()), 0);

            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Adapt: p = max {p − max{1, |B1|/|B2|}, 0} = {0}", p);

            /* Move x at the tail of T2. Set the page reference bit of x to 0. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "In B2: insert {0} into T2 tail", page);

            page.reference = false;
            b2.remove(pageId);
            t2.append(page);
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, "Status[add completed]: p = {0}, |T1| = {1}, |T2| = {2}, |B1| = {3}, |B2| = {4}, replaced {5}",
                    new Object[] {p, t1.size(), t2.size(), b1.size(), b2.size(), replaced});

        return replaced;
    }

    private DataPage unsafeReplace() {

//        22: found = 0
//        23: repeat
//        24:   if (|T1| >= max(1, p)) then
//        25:     if (the page reference bit of head page in T1 is 0) then
//        26:       found = 1;
//        27:       Demote the head page in T1 and make it the MRU page in B1.
//        28:     else
//        29:       Set the page reference bit of head page in T1 to 0, and make it the tail page in T2.
//        30:     endif
//        31:   else
//        32:     if (the page reference bit of head page in T2 is 0), then
//        33:       found = 1;
//        34:       Demote the head page in T2 and make it the MRU page in B2.
//        35:     else
//        36:       Set the page reference bit of head page in T2 to 0, and make it the tail page in T2.
//        37:     endif
//        38:   endif
//        39: until (found)

        while(true) {
            if (t1.size() >= Math.max(1, p)) {

                final DataPage t1h = t1.poll();
                if (t1h.reference == false) {

                    /* Demote the head page in T1 and make it the MRU page in B1. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T1 head {0} not referenced: demote to B1 MRU", t1h);

                    b1.append(t1h.pageId);

                    return t1h;

                } else {
                    /* Set the page reference bit of head page in T1 to 0, and make it the tail page in T2. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T1 head {0} not referenced: move into T2 tail", t1h);

                    t1h.reference = false;
                    t2.append(t1h);
                }
            } else {

                final DataPage t2h = t2.poll();
                if (t2h.reference == false) {

                    /* Demote the head page in T2 and make it the MRU page in B2. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T2 head {0} not referenced: demote to B2 MRU", t2h);

                    b2.append(t2h.pageId);

                    return t2h;

                } else {
                    /* Set the page reference bit of head page in T2 to 0, and make it the tail page in T2. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T2 head {0} not referenced: move into T2 tail", t2h);

                    t2h.reference = false;
                    t2.append(t2h);
                }
            }
        }
    }

    private boolean unsafeRemove(DataPage page) {

        /* Custom addition to CAR algorithm, we need to drop pages too */

        final DataPage t1r = t1.remove(page);
        if (t1r != null) {

            /* Demote the head page in T1 and make it the MRU page in B1. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Removing T1 element: demote to B1 MRU", t1r);

            b1.append(page.pageId);

            return true;

        }

        final DataPage t2r = t2.remove(page);
        if (t2r != null) {

            /* Demote the head page in T2 and make it the MRU page in B2. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Removing T2 element: demote to B2 MRU", t1r);

            b2.append(page.pageId);

            return true;

        }

        return false;
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

            if (page != null) {
                /* Set the page as referenced */
                page.reference = true;
            }

            return page;
        }

    }

}
