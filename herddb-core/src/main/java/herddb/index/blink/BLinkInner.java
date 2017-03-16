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
package herddb.index.blink;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;

/**
 * Inner node (non leaf) of {@link BLink}
 *
 * @author diego.salvi
 */
final class BLinkInner<K extends Comparable<K>> implements BLinkNode<K> {

    public static final long CONSTANT_NODE_BYTE_SIZE = 432;

    /** Doesn't account key occupancy */
    public static final long CONSTANT_ENTRY_BYTE_SIZE = 48;

    private static final Logger LOGGER = Logger.getLogger(BLinkInner.class.getName());

    private final BLinkPage page;
    private volatile long storeId;

    private BLinkInner<K> substitute;

    private final BLinkIndexDataStorage<K> storage;
    private final PageReplacementPolicy policy;

    private final ReadWriteLock loadLock = new ReentrantReadWriteLock();

    private volatile boolean loaded;
    private volatile boolean dirty;

    private final ConcurrentSkipListMap<Comparable<?>,Long> map;

    private final long maxElements;
    private final long minElements;
    private long elements;

    private final K highKey;
    private final BLinkPtr right;

    @SuppressWarnings("rawtypes")
    private static final class EverBiggerKey implements Comparable {

        private static final EverBiggerKey INSTANCE = new EverBiggerKey();

        /** Do not create new instances: use Singleton */
        private EverBiggerKey() {}

        @Override
        public int compareTo(Object o) {
            /* Never really used */
            return Integer.MAX_VALUE;
        }

    }

    @SuppressWarnings("rawtypes")
    private static final class EverBiggerKeyComparator implements Comparator<Comparable> {

        private static final EverBiggerKeyComparator INSTANCE = new EverBiggerKeyComparator();

        @Override
        @SuppressWarnings("unchecked")
        public int compare(Comparable o1, Comparable o2) {

            if (o1 == EverBiggerKey.INSTANCE) {

                /*
                 * Without this second check the map will continue to append EverBiggerKey element at the end
                 * (because it can't handle equality I presume).
                 */
                if (o2 == EverBiggerKey.INSTANCE) {
                    return 0;
                }

                return Integer.MAX_VALUE;

            } else if (o2 == EverBiggerKey.INSTANCE) {

                return Integer.MIN_VALUE;

            } else {
                return o1.compareTo(o2);
            }
        }

    }

    public BLinkInner(BLinkNodeMetadata<K> metadata, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {

        this.storage = storage;
        this.policy = policy;

        this.storeId = metadata.storeId;

        this.page = new BLinkPage(metadata.nodeId, this);

        this.maxElements = metadata.maxKeys;
        this.minElements = maxElements / 2;

        this.elements = metadata.keys;
        this.map = createNewMap();

        this.highKey = metadata.highKey;

        this.right = BLinkPtr.link(metadata.right);

        this.dirty  = false;
        this.loaded = false;
    }

    public BLinkInner(long storeId, long page, long maxElements, K key1, long value1, long value2, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {
        super();

        this.storage = storage;
        this.policy = policy;

        this.storeId = storeId;

        this.page = new BLinkPage(page, this);

        this.maxElements = maxElements;
        this.minElements = maxElements / 2;

        this.elements = 1;
        this.map = createNewMap();
        this.map.put(key1, value1);

        /* Add the placeholder for null key */
        this.map.put(EverBiggerKey.INSTANCE, value2);

        this.highKey = null;

        this.right = BLinkPtr.empty();

        /* Dirty by default */
        this.dirty  = true;
        this.loaded = true;
    }

    private BLinkInner(long storeId, BLinkPage page, long maxElements, long elements, ConcurrentSkipListMap<Comparable<?>,Long> map, K highKey, BLinkPtr right, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {
        super();

        this.storage = storage;
        this.policy = policy;

        this.storeId = storeId;

        this.page = page;

        this.maxElements = maxElements;
        this.minElements = maxElements / 2;

        this.elements = elements;
        this.map = map;

        this.highKey = highKey;

        this.right = right;

        /* Dirty by default */
        this.dirty  = true;
        this.loaded = true;
    }

    @Override
    public long getPageId() {
        return page.pageId;
    }

    @Override
    public BLinkPage getPage() {
        return page;
    }

    @Override
    public BLinkPtr getRight() {
        return right;
    }

    @Override
    public K getHighKey() {
        return highKey;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K getLowKey() {
        /* Inner node MUST have at least one key (so no EverBiggerKey will pop out) */
        return (K) map.firstKey();
    }

    @Override
    public long keys() {
        return elements;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public boolean isSafe() {
        return elements < maxElements;
    }

    @Override
    public boolean isSafeDelete() {
        return elements > minElements;
    }

    @Override
    public void unload(boolean flush) {
        try {
            doUnload(flush);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean doUnload(boolean flush) throws IOException {

        BLinkInner<K> substitute = null;

        final Lock lock = loadLock.writeLock();
        lock.lock();

        try {

            substitute = this.substitute;

            if (substitute == null) {
                if (!loaded) {
                    return false;
                }
                if (flush && dirty) {
                    checkpoint();
                }

                map.clear();
                loaded = false;

                LOGGER.log(Level.FINE, "unloaded inner node {0}", new Object[] {page.pageId});

                return true;
            }

        } finally {
            lock.unlock();
        }

        /* Invoke substituite outside lock */
        return substitute.doUnload(flush);
    }


    @Override
    public BLinkMetadata.BLinkNodeMetadata<K> checkpoint() throws IOException {

        BLinkNode<K> substitute = null;

        final Lock lock = loadLock.writeLock();
        lock.lock();

        try {

            substitute = this.substitute;

            if (substitute == null) {
                return doCheckpoint();
            }

        } finally {
            lock.unlock();
        }

        /* Invoke substituite outside lock */
        return substitute.checkpoint();
    }

    private BLinkMetadata.BLinkNodeMetadata<K> doCheckpoint() throws IOException {

        if (!dirty || !loaded) {

            BLinkMetadata.BLinkNodeMetadata<K> metadata = new BLinkMetadata.BLinkNodeMetadata<>(
                    BLinkNodeMetadata.NODE_TYPE, page.pageId, storeId, highKey, maxElements, elements, right.value);
            return metadata;
        }

        // LOTHRUIN
        /* TODO: scamuffo per ora, se va andrà cambiato */

        Element<K> root = null;
        Element<K> current = null;

        for(Map.Entry<Comparable<?>,Long> entry : map.entrySet()) {

            final Comparable<?> k = entry.getKey();

            @SuppressWarnings("unchecked")
            final K key = (k == EverBiggerKey.INSTANCE) ? null : (K) k;

            if (root == null) {
                root = new Element<>(key,entry.getValue());
                current = root;
            } else {
                current.next = new Element<>(key,entry.getValue());
                current = current.next;
            }
        }

        long storeId = storage.createDataPage(root);

        this.storeId = storeId;

        BLinkMetadata.BLinkNodeMetadata<K> metadata = new BLinkMetadata.BLinkNodeMetadata<>(
                BLinkNodeMetadata.NODE_TYPE, page.pageId, storeId, highKey, maxElements, elements, right.value);

        dirty = false;

        LOGGER.log(Level.FINE, "checkpoint inner node " + page.pageId + ": newpage -> " + storeId + " with " + elements + " keys x " + (elements + 1) + " pointers");

        return metadata;

    }

    private final Lock loadAndLock() {

        Lock read = loadLock.readLock();
        read.lock();

        if (!loaded) {

            /*
             * We need an upgrade from read to write, with ReentrantReadWriteLock isn't possible thus we
             * release current read lock and retrieve a write lock before recheck the condition.
             */
            read.unlock();

            Lock write = loadLock.writeLock();
            write.lock();

            try {
                /* Recheck condition (Another thread just loaded the node?) */
                if (!loaded) {

                    /* load */

                    Element<K> current = storage.loadPage(storeId);

                    while(current != null) {

                        if (current.key == null) {
                            /* Add the placeholder for null key */
                            map.put(EverBiggerKey.INSTANCE,current.page);
                        } else {
                            map.put(current.key,current.page);
                        }
                        current = current.next;
                    }

                    loaded = true;

                    final Metadata unload = policy.add(page);
                    if (unload != null) {
                        unload.owner.unload(unload.pageId);
                    }

                } else {
                    policy.pageHit(page);
                }


                /* Downgrade the lock (permitted) */
                read.lock();

            } catch (IOException err) {

                throw new RuntimeException(err);

            } finally {

                write.unlock();
            }

        } else {
            policy.pageHit(page);
        }

        return read;
    }

    @Override
    public BLinkPtr getFirstChild() {

        final Lock lock = loadAndLock();
        try {

            return map.isEmpty() ?  BLinkPtr.empty() : BLinkPtr.page(map.firstEntry().getValue());

        } finally {
            lock.unlock();
        }

    }

    @Override
    public BLinkPtr scanNode(K key) {

        /*
         * We could just load and copy root reference, but the node could be unloaded and loaded again
         * generating multiple node chain versions in memory with too much space used. We prefer a slower
         * approach locking load for the whole scan method
         */

        if (highKey != null && key.compareTo(highKey) >= 0) {
            return right;
        }

        final Map.Entry<Comparable<?>,Long> entry;
        final Lock lock = loadAndLock();
        try {

            /*
             * This is the sole procedure that can invoked on replaced nodes because isn't retrieved a write lock
             * for the node itself
             */

            entry = map.higherEntry(key);

        } finally {
            lock.unlock();
        }

        return entry == null ? BLinkPtr.empty() : BLinkPtr.page(entry.getValue());

    }

    /**
     * The method is invoked only on a split of a child node.
     *
     * <p>
     * Received parameters are:
     * <ul>
     * <li>the new split point (new high key of existing child)</li>
     * <li>a new child (with high key equals to the old one)</li>
     * </ul>
     * </p>
     * <p>
     * Given a key X and a pointer X' and a node as
     *
     * <pre>
     *     A -> B -> C -> n -> r
     *    /    /    /    /
     *   A'   B'   C'   D'
     * </pre>
     *
     * the node will became:
     *
     * <pre>
     *     A -> X -> B -> C -> n -> r
     *    /    /    /    /    /
     *   A'   B'   X'   C'   D'
     * </pre>
     *
     * (Insert after last)
     * <pre>
     *     A -> B -> C -> X -> n -> r
     *    /    /    /    /    /
     *   A'   B'   C'   D'   X'
     * </pre>
     *
     * Note that couple <tt>(B,B')->C</tt> will be split into <tt>(X,B')->B</tt> <tt>(B,X')->C</tt>
     * </p>
     */
    @Override
    public BLinkInner<K> insert(K key, long pointer) {

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );

        if (!isSafe()) {
            throw new IllegalStateException("Invoking insert on a unsafe node");
        }

        toString();

        final Lock lock = loadAndLock();

        try {
            /* Lock already held for modifications */


            final Map.Entry<Comparable<?>,Long> ceiling = map.ceilingEntry(key);

            if (ceiling.getKey().equals(key)) {
                throw new InternalError("Update Key NOT expected!");
            }

            /* Insert data in two phases, it can be done and read concurrently */
            /**
             * From
             * <pre>
             *     A -> B -> C -> n -> r
             *    /    /    /    /
             *   A'   B'   C'   D'
             * </pre>
             *
             * To
             * <pre>
             *     A -> X -> B -> C -> n -> r
             *    /    /    /    /    /
             *   A'   B'   B'   C'   D'
             * </pre>
             */
            map.put(key, ceiling.getValue());

            /**
             * From
             * <pre>
             *     A -> X -> B -> C -> n -> r
             *    /    /    /    /    /
             *   A'   B'   B'   C'   D'
             * </pre>
             *
             * To
             * <pre>
             *     A -> X -> B -> C -> n -> r
             *    /    /    /    /    /
             *   A'   B'   X'   C'   D'
             * </pre>
             */
            map.put(ceiling.getKey(), pointer);

            ++elements;

            dirty = true;

        } finally {
            lock.unlock();
        }

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERTED page " + this.page + " modified " + this + " K " + key + " ptr " + pointer );

        return this;
    }

    /**
     * The method is invoked only on a split of a child node when current node too need to split.
     *
     * <p>
     * Received parameters are:
     * <ul>
     * <li>the new split point (new high key of existing child)</li>
     * <li>a new child (with high key equals to the old one)</li>
     * <li>a new page to store the new sibling</li>
     * </ul>
     * </p>
     * <p>
     * Given a key X, a pointer X', a new page u and a node as
     *
     * <pre>
     * p -> A -> B -> C -> D -> n -> r
     *     /    /    /    /    /
     *    A'   B'   C'   D'   N'
     * </pre>
     *
     * the nodes will became
     *
     * <pre>
     * p -> A -> X -> B -> C -> D -> n -> r
     *     /    /    /    /    /    /
     *    A'   B'   X'   C'   D'   N'
     * </pre>
     *
     * and finally
     *
     * <pre>
     * p -> A -> X -> n -> u   u -> C -> D -> n -> r
     *     /    /    /             /    /    /
     *    A'   B'   X'            C'   D'   N'
     *
     * push B up
     * </pre>
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public BLinkNode<K>[] split(K key, long pointer, long newPage) {

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );

        if (isSafe()) {
            throw new IllegalStateException("Invoking rearrange on a safe node");
        }

        /* Unreferenced page from page policy replacement */
        Page.Metadata unload = null;

        /* Lock already held for modifications */

        final long splitpoint = (elements + 1) / 2;

        K push = null;

        int count = 0;
        boolean insert = true;

        ConcurrentSkipListMap<Comparable<?>,Long> amap = null;
        ConcurrentSkipListMap<Comparable<?>,Long> bmap = null;

        ConcurrentSkipListMap<Comparable<?>,Long> currentmap = createNewMap();

        /* Retrieve lock to avoid concurrent page unloads */
        final Lock lock = loadAndLock();

        try {

            for( Map.Entry<Comparable<?>,Long> entry : map.entrySet() ) {

                /* If still needs to insert */
                if (insert) {

                    /* First of all check if it needs to interleave the new key/value */
                    final int cmp = EverBiggerKeyComparator.INSTANCE.compare(entry.getKey(), key);

                    if (cmp > 0) {

                        /* Need to interleave the key/value */

                        if (count++ == splitpoint) {

                            /* Save the key as the "push" key */
                            push = key;

                            /* Attach the last element to current map */
                            /* Attention! Split point cannot be the first element! (There cannot be a node with no key!)*/
                            currentmap.put(EverBiggerKey.INSTANCE, entry.getValue());

                            /* Save old current map as the new a map */
                            amap = currentmap;

                            /* Reset the current map */
                            currentmap = createNewMap();

                        } else {

                            /* Otherwise just append the element to the current map  */
                            currentmap.put(key, entry.getValue());
                        }

                        if (count++ == splitpoint) {

                            /* Save the key as the "push" key */
                            /* Inner node MUST have at least one key (so no EverBiggerKey will pop out) */
                            push = (K) entry.getKey();

                            /* Attach the last element to current map */
                            /* Attention! Split point cannot be the first element! (There cannot be a node with no key!)*/
                            currentmap.put(EverBiggerKey.INSTANCE, pointer);

                            /* Save old current map as the new a map */
                            amap = currentmap;

                            /* Reset the current map */
                            currentmap = createNewMap();

                        } else {

                            /* Otherwise just append the element to the current map  */
                            currentmap.put(entry.getKey(), pointer);
                        }

                        /* Signal that the element has been inserted */
                        insert = false;

                        continue;

                    }

                }

                if (count++ == splitpoint) {

                    /* Save the key as the "push" key */
                    push = (K) entry.getKey();

                    /* Attach the last element to current map */
                    /* Attention! Split point cannot be the first element! (There cannot be a node with no key!)*/
                    currentmap.put(EverBiggerKey.INSTANCE, entry.getValue());

                    /* Save old current map as the new a map */
                    amap = currentmap;

                    /* Reset the current map */
                    currentmap = createNewMap();

                } else {

                    /* Otherwise just append the element to the current map  */
                    currentmap.put(entry.getKey(), entry.getValue());
                }
            }

            if (insert) {
                throw new InternalError(
                        "We should have inserted the node");
            }

            if (amap == null) {
                throw new InternalError(
                        "We should have split the node");
            }

            /* Sets the bprime map, aprime map has already been set */
            bmap = currentmap;

    //      make high key of A' equal y;
            //      make right-link of A' point to B';
            BLinkInner<K> aprime = new BLinkInner<>(storeId, page, maxElements, splitpoint, amap, push, BLinkPtr.link(newPage), storage, policy);

            /*
             * Replace page loading management owner... If we are to unload during this procedure the thread will
             * wait and then will see a new substitute owner pointing to the right owner!
             */
            substitute = aprime;
            page.owner.setOwner(aprime);

            BLinkPage bpage = new BLinkPage(newPage);
            //      make high key of B' equal old high key of A';
            //      make right-link of B' equal old right-link of A';
            BLinkInner<K> bprime = new BLinkInner<>(BLinkIndexDataStorage.NEW_PAGE, bpage,   maxElements, elements - splitpoint, bmap, highKey, right, storage, policy);

            /* Set page owner after construction */
            bpage.owner.setOwner(bprime);

    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " push " + push );
    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " A " + aprime );
    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " B " + bprime );

            unload = policy.add(bpage);

            final BLinkNode<K>[] result = new BLinkNode[] { aprime, bprime };

            return result;
        } finally {
            lock.unlock();

            /* Unload dereferenced page out of lock */
            if (unload != null) {
                unload.owner.unload(unload.pageId);
            }
        }
    }

    @Override
    public BLinkInner<K> delete(K key) {

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " DELETE page " + this.page + " orig " + this + " K " + key );

        /* Retrieve lock to avoid concurrent page unloads */
        final Lock lock = loadAndLock();

        try {
            /* Lock already held for modifications */

            Long old = map.remove(key);

            if (old == null) {
                throw new InternalError("An element to delete was expected!");
            }

            --elements;

            dirty = true;

            return this;

        } finally {
            lock.unlock();
        }

    }

    private static final ConcurrentSkipListMap<Comparable<?>,Long> createNewMap() {
        return new ConcurrentSkipListMap<>(EverBiggerKeyComparator.INSTANCE);
    }
    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();

        builder
            .append("BLinkInner [size: ").append(elements)
            .append(", page: ").append(page.pageId)
            .append(", dirty: ").append(dirty)
            .append(", loaded: ").append(loaded)
            .append(", high: ").append(highKey)
            .append(", right: ").append(right)
            .append(", data: ");

        for(Map.Entry<Comparable<?>,Long> entry : map.entrySet()) {
            if (entry.getKey() == EverBiggerKey.INSTANCE) {
                builder.setLength(builder.length() - 2);

                builder
                    .append(" -> ")
                    .append(entry.getValue());

            } else {
                builder
                    .append(entry.getValue())
                    .append(" <- ")
                    .append(entry.getKey())
                    .append(", ");
            }
        }

        builder.append("]");

        return builder.toString();
    }

}