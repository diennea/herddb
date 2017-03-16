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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
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
 * Leaf of {@link BLink}
 *
 * @author diego.salvi
 */
final class BLinkLeaf<K extends Comparable<K>> implements BLinkNode<K> {

    public static final long CONSTANT_NODE_BYTE_SIZE = 432;

    /** Doesn't account key occupancy */
    public static final long CONSTANT_ENTRY_BYTE_SIZE = 48;

    private static final Logger LOGGER = Logger.getLogger(BLinkLeaf.class.getName());

    private final BLinkPage page;
    private volatile long storeId;

    private BLinkLeaf<K> substitute;

    private final BLinkIndexDataStorage<K> storage;
    private final PageReplacementPolicy policy;

    private final ReadWriteLock loadLock = new ReentrantReadWriteLock();

    private volatile boolean loaded;
    private volatile boolean dirty;

    private final ConcurrentSkipListMap<K,Long> map;

    private final long maxElements;
    private final long minElements;
    private long elements;

    private final K highKey;
    private final BLinkPtr right;

    public BLinkLeaf(BLinkNodeMetadata<K> metadata, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {

        this.storage = storage;
        this.policy = policy;

        this.storeId = metadata.storeId;

        this.page = new BLinkPage(metadata.nodeId, this);

        this.maxElements = metadata.maxKeys;
        this.minElements = maxElements / 2;

        this.elements = metadata.keys;
        this.map = new ConcurrentSkipListMap<>();

        this.highKey = metadata.highKey;

        this.right = BLinkPtr.link(metadata.right);

        this.dirty  = false;
        this.loaded = false;

    }

    public BLinkLeaf(long storeId, long page, long maxElements, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {
        super();

        this.storage = storage;
        this.policy = policy;

        this.storeId = storeId;

        this.page = new BLinkPage(page, this);

        this.maxElements = maxElements;
        this.minElements = maxElements / 2;

        if (minElements < 1) {
            throw new IllegalArgumentException("At least one element!");
        }

        this.elements = 0;
        this.map = new ConcurrentSkipListMap<>();

        this.highKey = null;

        this.right = BLinkPtr.empty();

        /* Dirty by default */
        this.dirty  = true;
        this.loaded = true;
    }

    public BLinkLeaf(long storeId, long page, long maxElements, K key, long value, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {
        super();

        this.storage = storage;
        this.policy = policy;

        this.storeId = storeId;

        this.page = new BLinkPage(page, this);

        this.maxElements = maxElements;
        this.minElements = maxElements / 2;

        if (minElements < 1) {
            throw new IllegalArgumentException("At least one element!");
        }

        this.elements = 1;
        this.map = new ConcurrentSkipListMap<>();
        this.map.put(key, value);

        this.highKey = null;

        this.right = BLinkPtr.empty();

        /* Dirty by default */
        this.dirty  = true;
        this.loaded = true;
    }

    private BLinkLeaf(long storeId, BLinkPage page, long maxElements, long elements, ConcurrentSkipListMap<K,Long> map, K highKey, BLinkPtr right, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {
        super();

        this.storage = storage;
        this.policy = policy;

        this.storeId = storeId;

        this.page = page;

        this.maxElements = maxElements;
        this.minElements = maxElements / 2;

        if (minElements < 1) {
            throw new IllegalArgumentException("At least one element!");
        }

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
    public K getLowKey() {
        return map.firstKey();
    }


    @Override
    public long keys() {
        return elements;
    }

    @Override
    public boolean isLeaf() {
        return true;
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

        BLinkLeaf<K> substitute = null;

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

                LOGGER.log(Level.FINE, "unloaded leaf node {0}", new Object[] {page.pageId});

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

        BLinkLeaf<K> substitute = null;

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
                    BLinkNodeMetadata.LEAF_TYPE, page.pageId, storeId, highKey, maxElements, elements, right.value);
            return metadata;
        }

        // LOTHRUIN
        /* TODO: scamuffo per ora, se va andrà cambiato */

        Element<K> root = null;
        Element<K> current = null;

        for(Map.Entry<K,Long> entry : map.entrySet()) {
            if (root == null) {
                root = new Element<>(entry.getKey(),entry.getValue());
                current = root;
            } else {
                current.next = new Element<>(entry.getKey(),entry.getValue());
                current = current.next;
            }
        }

        long storeId = storage.createDataPage(root);

        this.storeId = storeId;

        BLinkMetadata.BLinkNodeMetadata<K> metadata = new BLinkMetadata.BLinkNodeMetadata<>(
                BLinkNodeMetadata.LEAF_TYPE, page.pageId, storeId, highKey, maxElements, elements, right.value);

        dirty = false;

        LOGGER.log(Level.FINE, "checkpoint leaf node " + page.pageId + ": newpage -> " + storeId + " with " + elements + " keys x " + elements + " values");

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
                        map.put(current.key,current.page);
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

    public List<Entry<K, Long>> getValues(K start, K end) {

        final Lock lock = loadAndLock();
        try {

            if (map.isEmpty()) {
                return Collections.emptyList();
            }

            ConcurrentNavigableMap<K,Long> submap;
            if (start == null) {
                if (end == null) {
                    submap = map;
                } else {
                    submap = map.headMap(end);
                }
            } else {
                if (end == null) {
                    submap = map.tailMap(start);
                } else {
                    submap = map.subMap(start,end);
                }
            }

            /* Data copy */
            return new ArrayList<>(submap.entrySet());

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

        Long value;
        final Lock lock = loadAndLock();
        try {

            value = map.get(key);

        } finally {
            lock.unlock();
        }

        return (value == null) ? BLinkPtr.empty() : BLinkPtr.page(value);
    }

    @Override
    public BLinkLeaf<K> insert(K key, long pointer) {

        /* Retrieve lock to avoid concurrent page unloads */
        final Lock lock = loadAndLock();

        //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );
        try {
            /* Lock already held for modifications */

            Long old = map.put(key, pointer);

            if (old == null) {
                ++elements;
            }

            dirty = true;

    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERTED page " + this.page + " modified " + this + " K " + key + " ptr " + pointer );

            return this;

        } catch (Throwable t) {
//            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " THROW page " + this.page + " modified " + this + " K " + key + " ptr " + pointer + " t " + t);
            t.printStackTrace();
            throw t;

        } finally {
            lock.unlock();
        }
    }

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

        int count = 0;
        boolean insert = true;

        final ConcurrentSkipListMap<K,Long> amap = new ConcurrentSkipListMap<>();
        final ConcurrentSkipListMap<K,Long> bmap = new ConcurrentSkipListMap<>();

        /* Retrieve lock to avoid concurrent page unloads */
        final Lock lock = loadAndLock();

        try {

            for( Map.Entry<K,Long> entry : map.entrySet() ) {

                if (insert) {
                    final int cmp = entry.getKey().compareTo(key);

                    if (cmp > 0) {

                        /* Insert here! */

                        /* Check and force count increment */
                        if (count++ < splitpoint) {
                            amap.put(key, pointer);
                        } else {
                            bmap.put(key, pointer);
                        }

                        /* Signal that the element has been inserted */
                        insert = false;

                        /* Continue to append */

                    } else if (cmp == 0) {
                        throw new InternalError("Replacement inside a split!!!");

                    }

                }

                /* Append */

                /* Check and force count increment */
                if (count++ < splitpoint) {
                    amap.put(entry.getKey(), entry.getValue());
                } else {
                    bmap.put(entry.getKey(), entry.getValue());
                }

            }

            if (insert) {

                /* Insert here! */

                /* Check and force count increment */
                if (count++ < splitpoint) {
                    amap.put(key, pointer);
                } else {
                    bmap.put(key, pointer);
                }
            }

            // make high key of A' equal y;
            // make right-link of A' point to B';
            BLinkLeaf<K> aprime = new BLinkLeaf<>(storeId, page, maxElements, splitpoint, amap, bmap.firstKey(), BLinkPtr.link(newPage), storage, policy);

            /*
             * Replace page loading management owner... If we are to unload during this procedure the thread will
             * wait and then will see a new substitute owner pointing to the right owner!
             */
            substitute = aprime;
            page.owner.setOwner(aprime);

            final BLinkPage bpage = new BLinkPage(newPage);
            // make high key of B' equal old high key of A';
            // make right-link of B' equal old right-link of A';
            BLinkLeaf<K> bprime = new BLinkLeaf<>(BLinkIndexDataStorage.NEW_PAGE, bpage, maxElements, count - splitpoint, bmap, highKey, right, storage, policy);

            /* Set page owner after construction */
            bpage.owner.setOwner(bprime);

            unload = policy.add(bpage);

            @SuppressWarnings("unchecked")
            final BLinkNode<K>[] result = new BLinkNode[] { aprime, bprime };

    //        System.out.println("SPLIT: " + this.page + " count " + splitpoint + " high " + acurrent.key);
    //        System.out.println("NEW SPLIT: " + newPage + " count " + (count - splitpoint) + " high " + highKey);

    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " A " + aprime );
    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " B " + bprime );

            return result;

        } finally {

            /* Wait the method end... we should at least wait substituite setup */
            lock.unlock();

            /* Unload dereferenced page out of lock */
            if (unload != null) {
                unload.owner.unload(unload.pageId);
            }
        }

    }

    @Override
    public BLinkLeaf<K> delete(K key) {

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " DELETE page " + this.page + " orig " + this + " K " + key );

        /* We'll not rebalance the tree during deletions! */
//        if (!isSafeDelete()) {
//            throw new IllegalStateException("Invoking delete on a unsafe safe delete node");
//        }

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

////    @Override
//    public BLinkNode<K> merge(
//            /* chiave da cancellare */ K key,
//            BLinkNode<K> sibling) {
//
//        /* Il fratello E' già stato loccato */
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " MERGE page " + this.page + " orig " + this + " sibling " + sibling + " K " + key );
//
//        if (isSafeDelete()) {
//            throw new IllegalStateException("Invoking merge on a safe node");
//        }
//
//        BLinkLeaf<K> leaf = (BLinkLeaf<K>) sibling;
//
//        /* Lock already held for modifications */
//
//        Element<K> chainroot = null;
//        Element<K> chaincurrent = null;
//
//        Element<K> next;
//
//        Element<K> current = root;
//
//        do {
//
//            final int cmp = current.key.compareTo(key);
//
//            if (cmp < 0) {
//
//                /* Just append the element to the current chain  */
//                next = new Element<>(current.key, current.page);
//
//                if (chainroot == null) {
//                    chainroot = next;
//                } else {
//                    chaincurrent.next = next;
//                }
//
//                chaincurrent = next;
//
//            } else if ( cmp == 0 ) {
//
//                /* Delete! Just ignore and exit from deleting while */
//                break;
//            } else {
//
//                throw new InternalError("An element to delete was expected!");
//            }
//
//        } while ((current = current.next) != null);
//
//        /* Append the rest of the chain */
//        if (current != null) {
//            do {
//
//                /* Just append the element to the current chain  */
//                next = new Element<>(current.key, current.page);
//
//                if (chainroot == null) {
//                    chainroot = next;
//                } else {
//                    chaincurrent.next = next;
//                }
//
//                chaincurrent = next;
//
//            } while ((current = current.next) != null);
//        }
//
//        current = leaf.root;
//
//        /* Append the sibling chain */
//        if (current != null) {
//            do {
//
//                /* Just append the element to the current chain  */
//                next = new Element<>(current.key, current.page);
//
//                if (chainroot == null) {
//                    chainroot = next;
//                } else {
//                    chaincurrent.next = next;
//                }
//
//                chaincurrent = next;
//
//            } while ((current = current.next) != null);
//        }
//
////      make high key of A' equal high Key of B;
//        BLinkLeaf<K> aprime = new BLinkLeaf<>(this.page, maxElements, elements + leaf.elements - 1, chainroot, leaf.highKey);
////      make right-link of A' equal old right-link of B;
//        aprime.right = leaf.right;
//
//        System.out.println("MERGE: " + this.page + " count " + aprime.elements + " high " + aprime.highKey);
//
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " MERGE page " + this.page + " A " + this );
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " MERGE page " + leaf.page + " B " + leaf );
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " MERGE page " + this.page + " A' " + aprime );
//
//        return aprime;
//    }






    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();

        builder
            .append("BLinkLeaf [size: ").append(elements)
            .append(", page: ").append(page.pageId)
            .append(", dirty: ").append(dirty)
            .append(", loaded: ").append(loaded)
            .append(", high: ").append(highKey)
            .append(", right: ").append(right)
            .append(", data: ");

        int len = builder.length();
        for(Map.Entry<K,Long> entry : map.entrySet()) {
            builder
                .append("(")
                .append(entry.getKey())
                .append(",")
                .append(entry.getValue())
                .append("), ");
        }

        if (builder.length() > len) {
            /* We added something and we need to remove last ', ' chars */
            builder.setLength(builder.length() - 2);
        }

        builder.append("]");

        return builder.toString();
    }

}