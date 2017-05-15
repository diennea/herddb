package herddb.core;

import java.util.Collection;
import java.util.Objects;
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
 *
 * See http://www-cs.stanford.edu/~sbansal/pubs/fast04.pdf
 * @author diego.salvi
 */
public class ClockAdaptiveReplacement implements PageReplacementPolicy {

    /** Class logger */
    private static final Logger LOGGER = Logger.getLogger(ClockAdaptiveReplacement.class.getName());

    /**
     * This <i>constants</i> rules out unecessary and expensive logs at compile level if set to {@code true}.
     * Note: it <b>must</b> be static final to succesfully work.
     */
    private static final boolean COMPILE_EXPENSIVE_LOGS = false;

    /** Capacity */
    private final int c;

    /** Recency clock */
    private final ListWithMap<CARMetadata> t1;

    /** Frequency clock */
    private final ListWithMap<CARMetadata> t2;

    /** Unloaded recency */
    private final ListWithMap<CARMetadata> b1;

    /** Unloaded frequency */
    private final ListWithMap<CARMetadata> b2;

    /** Self tuned parameter (target size of T1) */
    private int p;

    /** Modification lock */
    private final Lock lock = new ReentrantLock();

    public ClockAdaptiveReplacement(int capacity) {

        if (capacity < 1) {
            throw new IllegalArgumentException("Invalid capacity " + capacity);
        }
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
    public Page.Metadata add(Page<?> page) {
        lock.lock();
        try {

            final CARMetadata metadata = new CARMetadata(page);
            page.metadata = metadata;

            return unsafeAdd(metadata);
        } finally {
            lock.unlock();
        }
    }

    public Page.Metadata pop() {
        lock.lock();
        try {
            return unsafeReplace();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <P extends Page<?>> void remove(Collection<P> pages) {
        lock.lock();
        try {
            for(Page<?> page : pages) {
                unsafeRemove((CARMetadata) page.metadata);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Page<?> page) {
        lock.lock();
        try {
            return unsafeRemove((CARMetadata) page.metadata);
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
    public void pageHit(Page<?> page) {
        hit(page);
    }

    /* *********************** */
    /* *** PRIVATE METHODS *** */
    /* *********************** */

    private Page.Metadata unsafeAdd(CARMetadata page) {

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

        final boolean b1Hit = b1.contains(page);
        final boolean b2Hit = b2.contains(page);

        CARMetadata replaced = null;
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
            b1.remove(page);
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
            b2.remove(page);
            t2.append(page);
        }

        if (COMPILE_EXPENSIVE_LOGS)
            LOGGER.log(Level.SEVERE, "Status[add completed]: p = {0}, |T1| = {1}, |T2| = {2}, |B1| = {3}, |B2| = {4}, replaced {5}",
                    new Object[] {p, t1.size(), t2.size(), b1.size(), b2.size(), replaced});

        return replaced;
    }

    private CARMetadata unsafeReplace() {

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

                final CARMetadata t1h = t1.poll();
                if (t1h.reference == false) {

                    /* Demote the head page in T1 and make it the MRU page in B1. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T1 head {0} not referenced: demote to B1 MRU", t1h);

                    b1.append(t1h);

                    return t1h;

                } else {
                    /* Set the page reference bit of head page in T1 to 0, and make it the tail page in T2. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T1 head {0} not referenced: move into T2 tail", t1h);

                    t1h.reference = false;
                    t2.append(t1h);
                }
            } else {

                final CARMetadata t2h = t2.poll();
                if (t2h.reference == false) {

                    /* Demote the head page in T2 and make it the MRU page in B2. */
                    if (COMPILE_EXPENSIVE_LOGS)
                        LOGGER.log(Level.SEVERE, "T2 head {0} not referenced: demote to B2 MRU", t2h);

                    b2.append(t2h);

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

    private boolean unsafeRemove(CARMetadata page) {

        /* Custom addition to CAR algorithm, we need to drop pages too */

        final CARMetadata t1r = t1.remove(page);
        if (t1r != null) {

            /* Demote the head page in T1 and make it the MRU page in B1. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Removing T1 element: demote to B1 MRU", t1r);

            t1r.reference = false;

            b1.append(t1r);

            return true;
        }

        final CARMetadata t2r = t2.remove(page);
        if (t2r != null) {

            /* Demote the head page in T2 and make it the MRU page in B2. */
            if (COMPILE_EXPENSIVE_LOGS)
                LOGGER.log(Level.SEVERE, "Removing T2 element: demote to B2 MRU", t1r);

            t2r.reference = false;

            b2.append(t2r);

            return true;
        }

        return false;
    }

    /** Signal a page hit if really the page exists and ase the right metadata */
    private static final <P extends Page<?>> P hit(P page) {

        if (page != null && page.metadata != null) {
            /* Set the page as referenced */
            ((CARMetadata)page.metadata).reference = true;
        }

        return page;
    }

    /* *********************** */
    /* *** PRIVATE CLASSES *** */
    /* *********************** */

    /**
     * Implementation of {@link Page.Metadata} with all data needed for {@link ClockAdaptiveReplacement}.
     *
     * @author diego.salvi
     */
    private static final class CARMetadata extends Page.Metadata {

        public volatile boolean reference;

        private final int hashcode;

        public CARMetadata(Page<?> datapage) {
            this(datapage.owner, datapage.pageId);
        }

        public CARMetadata(Page.Owner owner, long pageId) {
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
            if (!(obj instanceof CARMetadata)) {
                return false;
            }
            CARMetadata other = (CARMetadata) obj;
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
            return "CARMetadata {pageId=" + pageId + ", owner=" + owner + '}';
        }
    }

}
