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
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.utils.SizeAwareObject;

/**
 * @author diego.salvi
 */
public final class BLink<K extends Comparable<K> & SizeAwareObject> {

    private static final Logger LOGGER = Logger.getLogger(BLink.class.getName());

    public static final long NO_RESULT = -1;
    public static final long NO_PAGE = -1;

    private static final long minNodeKeys = 3;
    private static final long minLeafKeys = 3;

    private static final long minNodeAvalilableSize = 1024;
    private static final long minLeafAvalilableSize = 1024;

    private final long nodeSize;
    private final long leafSize;
    private final long maxKeySize;

    private final BLinkIndexDataStorage<K> storage;
    private final PageReplacementPolicy policy;

    private final AtomicLong nextNodeId;

    public final ConcurrentMap<Long,BLinkNode<K>> nodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long,Lock> locks = new ConcurrentHashMap<>();

    private final LongAdder size;

    private volatile BLinkPtr root;

    public BLink(long nodeSize, long leafSize, BLinkMetadata<K> metadata,
            BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {

        checkNodeSize(nodeSize, leafSize);

        this.nodeSize = nodeSize;
        this.leafSize = leafSize;
        this.maxKeySize = evaluateMaxKeySize(nodeSize, leafSize);

        this.storage = storage;
        this.policy  = policy;

        this.nextNodeId = new AtomicLong(metadata.nextNodeId);

        this.size = new LongAdder();

        for( BLinkNodeMetadata<K> nodeMetadata : metadata.nodeMetadatas ) {
            BLinkNode<K> node;
            switch(nodeMetadata.type) {
                case BLinkNodeMetadata.NODE_TYPE:
                    node = new BLinkInner<>(nodeMetadata, nodeSize, storage, policy);
                    break;
                case BLinkNodeMetadata.LEAF_TYPE:
                    node = new BLinkLeaf<>(nodeMetadata, leafSize, storage, policy);
                    size.add(node.keys());
                    break;
                default:
                    LOGGER.log(Level.SEVERE, "ignoring unknown node type {0} during startup", new Object[]{nodeMetadata.type});
                    continue;
            }

            publish(node, BLinkPtr.page(node.getPageId()), false);
        }

        if (!nodes.containsKey(metadata.root)) {
            throw new IllegalArgumentException("Malformed metadata, unknown root " + metadata.root);
        }

        this.root = BLinkPtr.page(metadata.root);

    }

    public BLink(long nodeSize, long leafSize,
            BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {

        checkNodeSize(nodeSize, leafSize);

        this.nodeSize = nodeSize;
        this.leafSize = leafSize;
        this.maxKeySize = evaluateMaxKeySize(nodeSize, leafSize);

        this.storage = storage;
        this.policy  = policy;

        this.nextNodeId = new AtomicLong(1);

        this.size = new LongAdder();

        initEmptyRoot();

    }

    private void initEmptyRoot() {
        final long id = nextNodeId.getAndIncrement();
        this.root = BLinkPtr.page(id);

        /* Root vuota */
        final BLinkNode<K> node = new BLinkLeaf<>(BLinkIndexDataStorage.NEW_PAGE, id, leafSize, storage, policy);

        publish(node,root,false);

        final Page.Metadata unload = policy.add(node.getPage());
        if (unload != null) {
            unload.owner.unload(unload.pageId);
        }
    }

    private long evaluateMaxKeySize(long nodeSize, long leafSize) {

        long node = ((nodeSize - BLinkInner.CONSTANT_NODE_BYTE_SIZE) / minNodeKeys) - BLinkInner.CONSTANT_ENTRY_BYTE_SIZE;
        long leaf = ((leafSize - BLinkLeaf.CONSTANT_NODE_BYTE_SIZE) / minLeafKeys) - BLinkLeaf.CONSTANT_ENTRY_BYTE_SIZE;

        return Math.min(node, leaf);
    }

    private void checkNodeSize(long nodeSize, long leafSize) {

        long realMinNodeSize = minNodeAvalilableSize + BLinkInner.CONSTANT_NODE_BYTE_SIZE
                + BLinkInner.CONSTANT_ENTRY_BYTE_SIZE * minNodeKeys;

        if (nodeSize < realMinNodeSize) {
            throw new IllegalArgumentException(
                    "Node size too small! Node size " + nodeSize + " minimum node size " + realMinNodeSize);
        }

        long realMinLeafSize = minLeafAvalilableSize + BLinkLeaf.CONSTANT_NODE_BYTE_SIZE
                + BLinkLeaf.CONSTANT_ENTRY_BYTE_SIZE * minLeafKeys;

        if (leafSize < realMinLeafSize) {
            throw new IllegalArgumentException(
                    "Leaf size too small! Leaf size " + leafSize + " minimum leaf size " + realMinLeafSize);
        }
    }


    public long size() {
        return size.sum();
    }

    /**
     * Executes a complete tree checkpoint.
     * <p>
     * Invoking method must ensure that there isn't any concurrent update, read operations could be executed
     * concurrently with checkpoint.
     * </p>
     *
     * @return tree checkpoint metadata
     * @throws IOException
     */
    public BLinkMetadata<K> checkpoint() throws IOException {
        final List<BLinkMetadata.BLinkNodeMetadata<K>> metadatas = new LinkedList<>();
        for( BLinkNode<K> node : nodes.values() ) {
            BLinkNodeMetadata<K> metadata = node.checkpoint();

            metadatas.add(metadata);
        }

        return new BLinkMetadata<>(root.value, nextNodeId.get(), metadatas);
    }

    /** Non threadsafe */
    public void close() {
        root = null;

        /*
         * Each page has a reference to the node so it will fully removed when both page and node have no
         * other references. A node without data doesn't require much memory so it's imperative to "unload"
         * data when removed from page replacement policy.
         */

        List<BLinkPage> pages = new ArrayList<>(nodes.size());
        for(BLinkNode<K> node : nodes.values()) {
            pages.add(node.getPage());

            /* Unload live data (avoid checkpoint) */
            node.unload(false);
        }

        /* Remove from the policy */
        policy.remove(pages);

        nodes.clear();
        locks.clear();
        size.reset();
    }

    /** Non threadsafe */
    public void truncate() {

        /* Like close: need to remove live data but can keep stored data untill next checkpoint. */
        close();

        /* ... but we must accept data again */
        initEmptyRoot();

    }

//    public BLinkLeaf<K> scannode(K v) {
//
//        /* Get ptr to root node */
//        BLinkPtr current = root;
//
//        /* Read node into memory */
//        BLinkNode<K> a = get(current);
//
//        /* Missing root, no data in current index */
//        if (a == null) {
//            return null;
//        }
//
//        /* Scan through tree */
//        while( !a.isLeaf() ) {
//            /* Find correct (maybe link) ptr */
//            current = a.scanNode(v);
//            /* Read node into memory */
//            a = get(current);
//        }
//
//        /* Now we have reached leaves. */
//
//        /* Keep moving right if necessary */
//        BLinkPtr t;
//        while( ((t = a.scanNode(v)).isLink()) ) {
//            current = t;
//            /* Get node */
//            a = get(t);
//        }
//
//        /* Now we have the leaf node in which u should exist. */
//
//        return (BLinkLeaf<K>) a;
//
//    }

    public long search(K v) {

        /* Get ptr to root node */
        BLinkPtr current = root;

        /* Read node into memory */
        BLinkNode<K> a = get(current);

        /* Missing root, no data in current index */
        if (a == null) {
            return NO_RESULT;
        }

        /* Scan through tree */
        while( !a.isLeaf() ) {
            /* Find correct (maybe link) ptr */
            current = a.scanNode(v);
            /* Read node into memory */
            a = get(current);
        }

        /* Now we have reached leaves. */

        /* Keep moving right if necessary */
        BLinkPtr t;
        while( ((t = a.scanNode(v)).isLink()) ) {
            current = t;
            /* Get node */
            a = get(t);
        }

        /* Now we have the leaf node in which u should exist. */

        if (t.isEmpty()) {
            return NO_RESULT;
        } else {

            /* Already known to not be a link and not empty */
            return t.value;
        }
    }

    public Stream<Entry<K, Long>> fullScan() {

        /* Get ptr to root node */
        BLinkPtr current = root;

        /* Read node into memory */
        BLinkNode<K> a = get(current);

        /* Missing root, no data in current index */
        if (a == null) {
            return Stream.empty();
        }

        /* Scan through tree */
        while( !a.isLeaf() ) {
            /* Find correct (maybe link) ptr */
            current = a.getFirstChild();
            /* Read node into memory */
            a = get(current);
        }

        /* Now we have reached leaves. */

        final BLinkLeaf<K> leaf = (BLinkLeaf<K>) a;

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new LeafValueIterator<>(this, leaf, null, null),
                        /* No characteristics */ 0),
                /* No parallel */ false);
    }

    public Stream<Entry<K, Long>> scan(K start, K end) {

        /* Get ptr to root node */
        BLinkPtr current = root;

        /* Read node into memory */
        BLinkNode<K> a = get(current);

        /* Missing root, no data in current index */
        if (a == null) {
            return Stream.empty();
        }

        /* Scan through tree */
        while( !a.isLeaf() ) {
            /* Find correct (maybe link) ptr */
            current = a.scanNode(start);
            /* Read node into memory */
            a = get(current);
        }

        /* Now we have reached leaves. */

        /* Keep moving right if necessary */
        BLinkPtr t;
        while( ((t = a.scanNode(start)).isLink()) ) {
            current = t;
            /* Get node */
            a = get(t);
        }

        final BLinkLeaf<K> leaf = (BLinkLeaf<K>) a;

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new LeafValueIterator<>(this, leaf, start, end),
                        /* No characteristics */ 0),
                /* No parallel */ false);
    }

    public long insert(K v, long z) {

        if (v.getEstimatedSize() > maxKeySize) {
            throw new IllegalArgumentException("Key size too big for current page size! Key " + v.getEstimatedSize()
                    + " maximum key size " + maxKeySize);
        }

        /* For remembering ancestors */
        Deque<BLinkPtr> stack = new LinkedList<>();

        /* Get ptr to root node */
        BLinkPtr current = root;

        /* Root is ensure existent at BLink creation, see initEmptyRoot */
        BLinkNode<K> a = get(current);

//        /* Missing root, no data in current index */
//        if (a == null) {
//
//            synchronized (this) {
//                /* Root initialization */
//
//                /* Check if already created */
//                if ( root.isEmpty() ) {
//
//                    final long root = createNewPage();
//                    final BLinkPtr ptr = BLinkPtr.page(root);
//
//                    BLinkNode<K> node = new BLinkLeaf<>(BLinkIndexDataStorage.NEW_PAGE, root, leafSize, v, z, storage, policy);
//
//                    publish(node, ptr, false);
//
//                    final Page.Metadata unload = policy.add(node.getPage());
//                    if (unload != null) {
//                        unload.owner.unload(unload.pageId);
//                    }
//
//                    this.root = ptr;
//
//                    size.increment();
//
//                    return NO_RESULT;
//                }
//
//            }
//
//            /* Get ptr to root node (now should exist!) */
//            current = root;
//
//            a = get(current);
//        }

        BLinkPtr oldRoot = current;

        /* Scan down tree */
        BLinkPtr t;
        while(!a.isLeaf()) {
            t = current;
            current = a.scanNode(v);

            if (!current.isLink()) {
                /* Remember node at that level */
                stack.push(t);
            }

            a = get(current);
        }

        /* We have a candidate leaf */
        lock(current);

        a = get(current);

        /* If necessary */
        MoveRightResult<K> moveRight = moveRight(a, v, current);
        t = moveRight.t;
        current = moveRight.current;
        a = moveRight.a;


        /* if v is in A then stop “v already exists in tree”... And t points to its record */
        if (!t.isEmpty()) {

            long result = t.value;

            /* Insert even is unsafe! This is really an update! */
            a.insert(v, z);

//            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Exit update " + z );

            unlock(current);

            if ( result != NO_RESULT ) {
                size.increment();
            }

            /* stop */
            return result;
        }

        /* w <- pointer to pages allocated for record associated with v; */
        long w = z;

        /* Do insertion */

        while(!a.isSafeInsert(v)) {

            /* Original "if is safe = false" branch */

            /* Must split node */

            long u = createNewPage();

            /*
             * A, B <- rearrange old A, adding v and w, to make 2 nodes,
             * where (link ptr of A, link ptr of B) <- (u, link ptr of old A);
             */
            BLinkNode<K>[] nodes = a.split(v, w, u);

            BLinkNode<K> aprime = nodes[0];
            BLinkNode<K> bprime = nodes[1];

            /* For insertion into parent */
            K y = aprime.getHighKey();

            /* Insert B before A */
            BLinkPtr bptr = BLinkPtr.page(u);
            publish(bprime,bptr,true);

            /* Instantaneous change of 2 nodes */
            republish(aprime,current);

            /* Now insert pointer in parent */
            BLinkPtr oldnode = current;

            v = y;
            w = u;

            if (stack.isEmpty()) {

                synchronized (this) {

                    BLinkPtr currentRoot = root;
                    if (oldRoot.value == currentRoot.value) {

                        /* We are exiting from root! */
                        long r = createNewPage();

//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CREATION page " + r + " cr " + currentRoot + " or " + oldRoot);

//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CREATION A " + aprime );
//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CREATION B " + bprime );

                        BLinkNode<K> newRoot = new BLinkInner<>(BLinkIndexDataStorage.NEW_PAGE, r, nodeSize, v, aprime.getPageId(), bprime.getPageId(), storage, policy);

//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CREATION root " + newRoot);

                        final BLinkPtr ptr = BLinkPtr.page(r);
                        publish(newRoot, ptr, false);
                        root = ptr;

                        /* Success-done backtracking */
                        unlock(oldnode);
                        unlock(bptr);

                        final Metadata unload = policy.add(newRoot.getPage());
                        if (unload != null) {
                            unload.owner.unload(unload.pageId);
                        }

                        size.increment();

                        return NO_RESULT;

                    } else {

                        /* La root è cambiaaataaa!!!! */

//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CHANGE cr " + currentRoot + " or " + oldRoot);

//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CHANGE A " + aprime );
//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " ROOT CHANGE B " + bprime );


                        /* Get ptr to root node */
                        current = root;

                        /* Save it as the new "old root"*/
                        oldRoot = current;

                        a = get(current);

                        /* Scan down tree */
                        /* è un po' diverso dallo scan normale poiché dobbiamo essenzialmente trovare
                         * il nodo che è diventato il padre della vecchia root per inserire i dati
                         * In linea teorica potrebbe anche interessarci un solo nodo e rifare il giro se
                         * è cambiato di nuovo! */
                        K searchKey = aprime.getHighKey();


                        /* Scan down tree */
                        while(!a.isLeaf()) {
                            t = current;
                            current = a.scanNode(searchKey);

                            if (!current.isLink()) {
                                /* Remember node at that level */
                                stack.push(t);
                            }
//                            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Searching roots: a " + a + " current " + current + " key " + searchKey);

                            if (current.value == aprime.getPageId())
                                break;

                            a = get(current);
                        }

                        /* Now we have the node path again into stack */

//                        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " stack " + stack );
                    }
                }

            }

            /* Backtrack */
            current = stack.pop();

            /* Well ordered */
            lock(current);

            a = get(current);

            /* If necessary */
            moveRight = moveRight(a, v, current);

            t = moveRight.t;
            current = moveRight.current;
            a = moveRight.a;

            unlock(oldnode);
            unlock(bptr);

            /* And repeat procedure for parent */

        }

        /* Original "if is safe = true" branch */
        /* Exact manner depends if current is a leaf */

        a = a.insert(v,w);
        republish(a,current);

        /* Success-done backtracking */
        unlock(current);

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Exit insert " + z );

        size.increment();

        return NO_RESULT;

    }

    public long delete(K v) {

        /* Get ptr to root node */
        BLinkPtr current = root;

        BLinkNode<K> a = get(current);

        /* Missing root, no data in current index */
        if (a == null) {

            synchronized (this) {

                /* Check if created by another thread */
                if ( root.isEmpty() ) {
                    return NO_RESULT;
                }

            }

            /* Get ptr to root node (now should exist!) */
            current = root;

            a = get(current);
        }

        /* Scan down tree */
        BLinkPtr t;
        while(!a.isLeaf()) {
            t = current;
            current = a.scanNode(v);
            a = get(current);
        }

        /* We have a candidate leaf */
        lock(current);

        a = get(current);

        /* If necessary */
        MoveRightResult<K> moveRight = moveRight(a, v, current);
        t = moveRight.t;
        current = moveRight.current;
        a = moveRight.a;


        /* if v is in not A then stop “v dowsn't exist in tree” */
        if (t.isEmpty()) {
            unlock(current);
            return NO_RESULT;
        }

        /* result <- pointer to page allocated for stored record */
        long result = t.value;

        /* Just delete on leaf, the tree will be rebalanced by a batch procedure */

        a = a.delete(v);
        republish(a,current);

        /* Success-done backtracking */
        unlock(current);

//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Exit delete " + v );

        size.decrement();

        return result;

    }

    private BLinkNode<K> get(BLinkPtr pointer) {
        return nodes.get(pointer.value);
    }

    private void republish(BLinkNode<K> node, BLinkPtr pointer) {
        /* Il lock e una enty di nodo devono già esistere! */
        if (!locks.containsKey(pointer.value)){
            throw new InternalError("Lock expected");
        }

        nodes.put(pointer.value,node);
    }

    private void publish(BLinkNode<K> node, BLinkPtr pointer, boolean locked) {
        Lock lock = new ReentrantLock();
        if (locked) {
//            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Locking " + pointer);
            lock.lock();
//            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Lock " + pointer);
        }

        locks.put(pointer.value, lock);
        nodes.put(pointer.value,node);
    }



    private long createNewPage() {
        return nextNodeId.getAndIncrement();
    }

    private void lock(BLinkPtr pointer) {
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Locking " + pointer);

//        try {
//            if (!locks.get(pointer.value).tryLock(3, TimeUnit.SECONDS)) {
//                System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " --------------> Deadlock " + pointer);
//
//                Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
//
//                for( Thread thread : threadSet )
//                    for (StackTraceElement ste : thread.getStackTrace()) {
//                        System.out.println("T" + Thread.currentThread().getId() + " TD" + thread.getId() + " -> " + ste);
//                }
//
//                throw new InternalError("Deadlock " + pointer);
//            }
//        } catch (InterruptedException e) {
//            throw new InternalError("interrupt " + pointer);
//        }

        locks.get(pointer.value).lock();
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Lock " + pointer);
    }

    private void unlock(BLinkPtr pointer) {
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Unlock " + pointer);

        try {
        locks.get(pointer.value).unlock();
        } catch (Exception e) {
            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " --------------> UNLOCK FAIL " + pointer);

            System.out.println("T" + Thread.currentThread().getId() + " TD" + Thread.currentThread().getId() + " UNLOCK FAIL -> " + e);
            System.out.println("T" + Thread.currentThread().getId() + " TD" + Thread.currentThread().getId() + " UNLOCK FAIL -> " + e.getMessage());
            e.printStackTrace(System.out);
            for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                System.out.println("T" + Thread.currentThread().getId() + " TD" + Thread.currentThread().getId() + " UNLOCK FAIL -> " + ste);
            }
        }
    }

    private MoveRightResult<K> moveRight(BLinkNode<K> a, K key, BLinkPtr current) {

        BLinkPtr t;
        /* Move right if necessary */
        while( ((t = a.scanNode(key)).isLink()) ) {
            /* Note left-to-right locking */
            lock(t);
            unlock(current);
            current = t;
            a = get(current);
        }

        return new MoveRightResult<>(t, current, a);
    }

    private static final class MoveRightResult<K extends Comparable<K> & SizeAwareObject> {
        final BLinkPtr t;
        final BLinkPtr current;
        final BLinkNode<K> a;

        public MoveRightResult(BLinkPtr t, BLinkPtr current, BLinkNode<K> a) {
            super();
            this.t = t;
            this.current = current;
            this.a = a;
        }
    }

    @Override
    public String toString() {
        return "BLink [nodeSize=" + nodeSize + ", leafSize=" + leafSize + ", size=" + size + ", nextNodeId=" + nextNodeId + "]";
    }

//    public void deepPrint(int maxstack) {
//
//        System.out.println(this);
//
//        Set<Long> seen = new HashSet<>();
//        Deque<Object[]> stack = new LinkedList<>();
//        stack.push(new Object[] {1, root});
//
//        int count = 0;
//        while (!stack.isEmpty())  {
//
//            if (++count == maxstack) {
//                System.out.println("Max Stack " + count + " loops?");
//                return;
//            }
//
//            Object[] o = stack.pop();
//
//            int indents = (int) o[0];
//
//            BLinkPtr current = (BLinkPtr) o[1];
//
//            /* Read node into memory */
//            BLinkNode<K> node = get(current);
//
//            StringBuilder builder = new StringBuilder();
//            for(int i = 0; i < indents; ++i) {
//                builder.append("-");
//            }
//            builder.append(node);
//            System.out.println(builder);
//
//
//            if (!seen.add(node.getPageId())) {
//                System.out.println("Page " + node.getPageId() + " already seen");
//                return;
//            }
//
//            if (!node.isLeaf()) {
//
//                BLinkInner<K> inner = (BLinkInner<K>) node;
//
//                Element<K> e = inner.root;
//
//                Deque<Object[]> ministack = new LinkedList<>();
//
//                while(e != null) {
//                    ministack.push(new Object[] {indents + 1, BLinkPtr.page(e.page)});
//                    e = e.next;
//                }
//
//                /* Reverse to match child ordering! */
//                while(!ministack.isEmpty()) {
//                    stack.push(ministack.pop());
//                }
//            }
//
//        }
//
//    }

    /**
     * Iterate through leaf values, looking for right leaves if needed.
     *
     * @author diego.salvi
     *
     * @param <K>
     */
    private static final class LeafValueIterator<K extends Comparable<K> & SizeAwareObject> implements Iterator<Entry<K, Long>> {

        private final BLink<K> tree;
        private final K end;

        private boolean nextChecked;

        private BLinkPtr right;
        private K highKey;

        private Iterator<Entry<K, Long>> current;

        public LeafValueIterator(BLink<K> tree, BLinkLeaf<K> leaf, K start, K end) {
            super();

            this.tree = tree;
            this.right = leaf.getRight();

            this.current = leaf.getValues(start, end);
            this.highKey = leaf.getHighKey();

            this.end = end;

            this.nextChecked = false;
        }


        @Override
        public boolean hasNext() {

            nextChecked = true;

            while (current != null) {

                /* If remains data in currently checked node use it */
                if (current.hasNext()) {

                    return true;

                } else {

                    /* Otherwise try to move right */
                    if (!right.isEmpty()) {

                        /* If current highkey is less than or equal to target end we can move right */
                        if (end == null || highKey.compareTo(end) <= 0) {

                            /* Move right */
                            BLinkLeaf<K> leaf = (BLinkLeaf<K>) tree.get(right);
                            current = leaf.getValues(null, end);
                            highKey = leaf.getHighKey();
                            right   = leaf.getRight();

                        } else {

                            /* Oterwise there is no interesting data at right */
                            current = null;
                            return false;
                        }

                    } else {

                        /* Oterwise there is no data at right */
                        current = null;
                        return false;

                    }
                }
            }

            return false;
        }

        @Override
        public Entry<K, Long> next() {

            if (nextChecked) {

                if (current == null) {
                    throw new NoSuchElementException();
                }

                /*
                 * If next has been checked we don't need to replay all next checking... it's already
                 * prepared!
                 */

                nextChecked = false;
                return current.next();

            } else {

                while (current != null) {

                    /* If remains data in currently checked node use it */
                    if (current.hasNext()) {

                        nextChecked = false;
                        return current.next();

                    } else {

                        /* Otherwise try to move right */
                        if (!right.isEmpty()) {

                            /* If current highkey is greater than end we can move right */
                            if (end == null || highKey.compareTo(end) > 0) {

                                /* Move right */
                                BLinkLeaf<K> leaf = (BLinkLeaf<K>) tree.get(right);
                                current = leaf.getValues(null, end);
                                highKey = leaf.getHighKey();
                                right   = leaf.getRight();

                            } else {

                                /* Oterwise there is no interesting data at right */
                                current = null;
                                throw new NoSuchElementException();
                            }

                        } else {

                            /* Oterwise there is no data at right */
                            current = null;
                            throw new NoSuchElementException();

                        }
                    }
                }

                throw new NoSuchElementException();

            }

        }

    }
}
