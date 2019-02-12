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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.utils.Holder;

/**
 * Java implementation of b-link tree derived from Vladimir Lanin and Dennis
 * Shasha work: <i>A symmetric concurrent b-tree algorithm</i>
 *
 * <p>
 * This implementations add variable sized nodes, range scans, truncation and a
 * way to store data pages.
 * </p>
 *
 * <p>
 * For original work see:
 *
 * <pre>
 * LANIN, Vladimir; SHASHA, Dennis. A symmetric concurrent b-tree algorithm.
 * In: Proceedings of 1986 ACM Fall joint computer conference.
 * IEEE Computer Society Press, 1986. p. 380-389
 * </pre>
 *
 * @author diego.salvi
 */
public class BLink<K extends Comparable<K>, V> implements AutoCloseable, Page.Owner {

    /**
     * Debug flag to remove some logs and checks during normal operations
     */
    private static final boolean DEBUG = false;

    private static final Logger LOGGER = Logger.getLogger(BLink.class.getName());

    /**
     * Signal that a node size is unknown and need to be recalculated (if a node
     * has really size 0 recalculation will trigger too but isn't a problem:
     * nothing to recalculate)
     */
    static final long UNKNOWN_SIZE = 0L;

//    type
//        locktype = (readlock, writelock);
//        nodeptr = "node;
//        height = 1 .. maxint;
//        task = (add, remove);
    private static final int READ_LOCK = 1;
    private static final int WRITE_LOCK = 2;

    private static final int ADD_TASK = 1;
    private static final int REMOVE_TASK = 2;

    /**
     * Minimum number of children to keep a node as root.
     * <p>
     * In original algorithm was children > 3 but testing this facility too much
     * time was expended into move_right at higher levels. It's better to keep
     * it at a minimum of 2 children to reduce move_right invocations.
     * </p>
     */
    private static final int CRITIC_MIN_CHILDREN = 2;

    private final Anchor<K, V> anchor;

    private final ConcurrentMap<Long, Node<K, V>> nodes;

    private final AtomicLong nextID;

    private final K positiveInfinity;
    private final long maxSize;
    private final long minSize;

    private final SizeEvaluator<K, V> evaluator;

    private final BLinkIndexDataStorage<K, V> storage;
    private final PageReplacementPolicy policy;

    private final AtomicBoolean closed;

    private final LongAdder size;

    /**
     * Support structure to evaluates memory byte size occupancy of keys and
     * values
     *
     * @author diego.salvi
     */
    public static interface SizeEvaluator<X, Y> {

        /**
         * Evaluate the key size only
         */
        public long evaluateKey(X key);

        /**
         * Evaluate the value size only
         */
        public long evaluateValue(Y value);

        /**
         * Evaluate both key and value size
         */
        public long evaluateAll(X key, Y value);

        /**
         * Returns a value which is greater than all of the other values.
         * Code will check using '==', so this value must be a singleton.
         *
         * @return a value which is greater than every other value
         */
        public X getPosiviveInfinityKey();

    }

    public BLink(long maxSize, SizeEvaluator<K, V> evaluator,
            PageReplacementPolicy policy, BLinkIndexDataStorage<K, V> storage) {
        this.positiveInfinity = evaluator.getPosiviveInfinityKey();
        if (this.positiveInfinity != evaluator.getPosiviveInfinityKey()) {
            throw new IllegalStateException("getPosiviveInfinityKey must always return the same value");
        }
        this.maxSize = maxSize;
        this.minSize = maxSize / 2;

        this.evaluator = evaluator;

        this.storage = storage;
        this.policy = policy;

        this.nextID = new AtomicLong(1L);
        this.closed = new AtomicBoolean(false);
        this.size = new LongAdder();

        this.nodes = new ConcurrentHashMap<>();

        final Node<K, V> root = allocate_node(true);
        this.anchor = new Anchor<>(root);

        /* Nothing to load locked now (we are creating a new tree) */
        final Metadata meta = policy.add(root);
        if (meta != null) {
            meta.owner.unload(meta.pageId);
        }
    }

    public BLink(long maxSize, SizeEvaluator<K, V> evaluator,
            PageReplacementPolicy policy, BLinkIndexDataStorage<K, V> storage,
            BLinkMetadata<K> metadata) {
        this.positiveInfinity = evaluator.getPosiviveInfinityKey();
        if (this.positiveInfinity != evaluator.getPosiviveInfinityKey()) {
            throw new IllegalStateException("getPosiviveInfinityKey must always return the same value");
        }

        this.maxSize = maxSize;
        this.minSize = maxSize / 2;

        this.evaluator = evaluator;

        this.storage = storage;
        this.policy = policy;

        this.nextID = new AtomicLong(metadata.nextID);
        this.closed = new AtomicBoolean(false);
        this.size = new LongAdder();
        size.add(metadata.values);

        this.nodes = new ConcurrentHashMap<>();

        convertNodeMetadata(metadata.nodes, nodes);

        this.anchor = new Anchor<>(
                nodes.get(metadata.fast), metadata.fastheight,
                nodes.get(metadata.top), metadata.topheight,
                nodes.get(metadata.first));
    }

    /**
     * Convert given node metadatas in real nodes and push them into given map
     */
    private void convertNodeMetadata(List<BLinkNodeMetadata<K>> nodes, Map<Long, Node<K, V>> map) {

        /* First loop: create every node without links (no rightlink nor outlink) */
        for (BLinkNodeMetadata<K> metadata : nodes) {
            map.put(metadata.id, new Node<>(metadata, this));
        }

        /* Second loop: add missing links (rightlink or outlink) */
        for (BLinkNodeMetadata<K> metadata : nodes) {

            final Node<K, V> node = map.get(metadata.id);

            if (metadata.rightlink != BLinkNodeMetadata.NO_LINK) {
                node.rightlink = map.get(metadata.rightlink);
            }

            if (metadata.outlink != BLinkNodeMetadata.NO_LINK) {
                node.outlink = map.get(metadata.outlink);
            }
        }
    }

    /**
     * Fully close the facility
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            final Iterator<Node<K, V>> iterator = nodes.values().iterator();
            while (iterator.hasNext()) {
                Node<K, V> node = iterator.next();
                /* If the node has been unloaded removes it from policy */
                if (node.unload(false, false)) {
                    policy.remove(node);
                }

                /* linked nodes dereferencing */
                node.outlink = null;
                node.rightlink = null;
            }

            /* Anchor nodes dereferencing */
            anchor.fast = null;
            anchor.top = null;

            nodes.clear();
            size.reset();
        }
    }

    /**
     * Truncate any tree data. Invokers must ensure to call this method in a not
     * concurrent way.
     */
    public void truncate() {

        final Iterator<Node<K, V>> iterator = nodes.values().iterator();
        while (iterator.hasNext()) {
            Node<K, V> node = iterator.next();
            /* If the node has been unloaded removes it from policy */
            if (node.unload(false, false)) {
                policy.remove(node);
            }

            /* linked nodes dereferencing */
            node.outlink = null;
            node.rightlink = null;
        }

        nodes.clear();
        size.reset();

        final Node<K, V> root = allocate_node(true);
        this.anchor.reset(root);

        /* Nothing to load locked now */
        final Metadata meta = policy.add(root);
        if (meta != null) {
            meta.owner.unload(meta.pageId);
        }

    }

    /**
     * Returns the number of key/value pairs stored into this tree.
     *
     * @return current size of the tree
     */
    public long size() {
        return size.sum();
    }

    /**
     * Returns the current nodes count.
     *
     * @return current tree nodes count
     */
    public int nodes() {
        return nodes.size();
    }

    /* ******************** */
 /* *** PAGE LOADING *** */
 /* ******************** */
    @Override
    public void unload(long pageId) {
        nodes.get(pageId).unload(true, false);
    }

    /**
     * Handles page unloading, using special try & unload if given metadata
     * represent a page owned by current BLink tree.
     *
     * @param unload metadata to unload
     * @return {@code true} if unloaded
     */
    private boolean attemptUnload(Metadata unload) {

        if (unload.owner == this) {

            /*
             * Page owned by current BLink tree, use try -> unload to avoid deadlock on
             * loadLock acquisition. If not unloaded here invoking code will have to unload
             * the page later after releasing his load lock
             */

 /* Attempt to unload metadata if a lock can be acquired */
            return nodes.get(unload.pageId).unload(true, true);

        } else {

            /* Directly unload metatada */
            unload.owner.unload(unload.pageId);

            return true;
        }

    }

    /**
     * Executes a complete tree checkpoint.
     * <p>
     * Invoking method must ensure that there isn't any concurrent update, read
     * operations could be executed concurrently with checkpoint.
     * </p>
     *
     * @return tree checkpoint metadata
     * @throws IOException
     */
    public BLinkMetadata<K> checkpoint() throws IOException {

        final List<BLinkNodeMetadata<K>> metadatas = new LinkedList<>();
        for (Node<K, V> node : nodes.values()) {

            /*
             * Lock shouldn't be really needed because checkpoint must invoked when no thread are modifying
             * the index but to ensure that latest value of "empty" flag is read we must read it from RAM. Any
             * memory breaking operation would suffice but tacking a read lock on the node is "cleaner"
             */
            lock(node, READ_LOCK);
            try {
                /*
                 * Do not checkpoint empty nodes. They aren't needed at all and because no modification
                 * operations is occurring currently seen empty node aren't referenced by anyone.
                 */
                if (node.empty) {
                    continue;
                }
            } finally {
                unlock(node, READ_LOCK);
            }

            BLinkNodeMetadata<K> metadata = node.checkpoint();

            metadatas.add(metadata);

            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("node " + metadata.id + " has " + metadata.keys + " keys at checkpoint");
            }
        }

        lock_anchor(READ_LOCK);

        long fast = anchor.fast.pageId;
        int fastheight = anchor.fastheight;
        long top = anchor.top.pageId;
        int topheight = anchor.topheight;
        long first = anchor.first.pageId;

        unlock_anchor(READ_LOCK);

        return new BLinkMetadata<>(nextID.get(), fast, fastheight, top, topheight, first, size.sum(), metadatas);
    }

    /* ******************** */
 /* *** TREE METHODS *** */
 /* ******************** */
//    function search(v: value); boolean;
//    var
//        n: nodeptr;
//        descent: stack;
//    begin
//        n := locate-leaf(v, readlock, descent); {v € coverset(n), n read-locked}
//        search := check-key(v, n); {decisive}
//        unlock(n, readlock)
//    end;
    public V search(K v) {

        Node<K, V> n;
        @SuppressWarnings("unchecked")
        Deque<ResultCouple<K, V>> descent = DummyDeque.INSTANCE;

        try {

            n = locate_leaf(v, READ_LOCK, descent); // v € coverset(n), n read-locked

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to search for " + v, ex);
        }

        try {

            V search = n.check_key(v); // decisive;

            return search;

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to search for " + v, ex);

        } finally {

            unlock(n, READ_LOCK);
        }
    }

    /**
     * Supports both from and to empty.
     *
     * @param from inclusive (if not empty)
     * @param to exclusive
     * @return
     */
    public Stream<Entry<K, V>> scan(K from, K to) {

        Node<K, V> n;

        @SuppressWarnings("unchecked")
        Deque<ResultCouple<K, V>> descent = DummyDeque.INSTANCE;

        if (from == null) {
            lock_anchor(READ_LOCK);
            n = anchor.first;
            unlock_anchor(READ_LOCK);

            /*
             * We have to lock the first node, scan iterator require a read locked node (as produced from
             * locate_leaf too)
             */
            lock(n, READ_LOCK);
        } else {
            try {

                n = locate_leaf(from, READ_LOCK, descent); // v € coverset(n), n read-locked

            } catch (IOException ex) {

                throw new UncheckedIOException("failed to scan from " + from + " to " + to, ex);
            }
        }

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new ScanIterator(n, from, from != null, to, false),
                        /* No characteristics */ 0),
                /* No parallel */ false);
    }

    public Stream<Entry<K, V>> scan(K from, K to, boolean toInclusive) {

        Node<K, V> n;

        @SuppressWarnings("unchecked")
        Deque<ResultCouple<K, V>> descent = DummyDeque.INSTANCE;

        if (from == null) {
            lock_anchor(READ_LOCK);
            n = anchor.first;
            unlock_anchor(READ_LOCK);

            /*
             * We have to lock the first node, scan iterator require a read locked node (as produced from
             * locate_leaf too)
             */
            lock(n, READ_LOCK);
        } else {

            try {

                n = locate_leaf(from, READ_LOCK, descent); // v € coverset(n), n read-locked

            } catch (IOException ex) {

                throw new UncheckedIOException("failed to scan from " + from + " to " + to, ex);
            }

        }

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new ScanIterator(n, from, from != null, to, toInclusive),
                        /* No characteristics */ 0),
                /* No parallel */ false);
    }

//    function insert(v: value): boolean;
//    var
//        n: nodeptr;
//        descent: stack;
//    begin
//        n := locate-leaf(v, writelock, descent); {v € coverset(n), n write-locked}
//        insert := add-key(v, n); {decisive}
//        normalize(n, descent, 1);
//        unlock(n, writelock)
//    end;
    public boolean insert(K v, V e, V expected) {

        Node<K, V> n;
        Deque<ResultCouple<K, V>> descent = new LinkedList<>();
        Queue<CriticJob> maintenance = new LinkedList<>();

        try {

            n = locate_leaf(v, WRITE_LOCK, descent);  // v € coverset(n), n write-locked

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to insert " + v, ex);
        }

        boolean added;
        try {

            added = n.add_key_if(v, e, expected); // decisive
            normalize(n, descent, 1, maintenance);

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to insert " + v, ex);

        } finally {

            unlock(n, WRITE_LOCK);
        }

        if (added == true && expected == null) {
            size.increment();
        }

        handleMainenance(maintenance);

        return added;
    }

    public V insert(K v, V e) {

        Node<K, V> n;
        Deque<ResultCouple<K, V>> descent = new LinkedList<>();
        Queue<CriticJob> maintenance = new LinkedList<>();

        try {

            n = locate_leaf(v, WRITE_LOCK, descent);  // v € coverset(n), n write-locked

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to insert " + v, ex);
        }

        V replaced;
        try {

            replaced = n.add_key(v, e); // decisive
            normalize(n, descent, 1, maintenance);

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to insert " + v, ex);

        } finally {

            unlock(n, WRITE_LOCK);
        }

        if (replaced == null) {
            size.increment();
        }

        handleMainenance(maintenance);

        return replaced;
    }

//    function delete(v: value): boolean;
//    var
//        n: nodeptr;
//        descent: stack;
//    begin
//        n := locate-leaf(v, writelock, descent); {v € coverset(n) , n write-locked}
//        delete := remove-key(v, n); {decisive}
//        nornialize(n, descent, 1); unlock(n, writelock)
//    end;
    public V delete(K v) {

        Node<K, V> n;
        Deque<ResultCouple<K, V>> descent = new LinkedList<>();
        Queue<CriticJob> maintenance = new LinkedList<>();

        try {

            n = locate_leaf(v, WRITE_LOCK, descent); // v € coverset(n), n write-locked

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to delete " + v, ex);
        }

        V delete;
        try {

            delete = n.remove_key(v); // decisive
            normalize(n, descent, 1, maintenance);

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to delete " + v, ex);

        } finally {

            unlock(n, WRITE_LOCK);
        }

        if (delete != null) {
            size.decrement();
        }

        handleMainenance(maintenance);

        return delete;
    }

//    function locate-leaf(v: value; lastlock: locktype; var descent: stack): nodeptr;
//    { locate-leaf descends from the anchor to the leaf whose coverset
//    includes v, places a lock of kind specified in lastlock on that leaf,
//    and returns a pointer to it. It records its path in the stack descent. }
//    var
//        n,m: nodeptr;
//        h,enterheight: height;
//        ubleftsep: value;
//        { ubleftsep stands for "upper bound on the leftsep of the current node".
//        This value is recorded for each node on the descent stack so that an ascending process can tell if it's too far to the right. }
//    begin
//        lock-anchor(readlock);
//        n := anchor.fast; enterheight := anchor.fastheight; ubleftsep := +inf";
//        unlock-anchor(readlock);
//        set-to-empty(descent);
//        for h := enterheight downto 2 do begin { v > leftsep (n)}
//            move-right(v, n, ubleftsep, readlock);{ v € coverset(n) }
//            push(n, ubleftsep, descent);
//            (m, ubleftsep) := find(v, n, ubleftsep); { v > leftsep (m) }
//            unlock(n, readlock); n := m
//        end;
//        move-right(v, n, ubleftsep, lastlock); {v € coverset(n) }
//        locate-leaf := n
//    end;
    /**
     * locate-leaf descends from the anchor to the leaf whose coverset includes
     * v, places a lock of kind specified in lastlock on that leaf, and returns
     * a pointer to it. It records its path in the stack descent.
     *
     * @param v
     * @param lastlock
     * @param descent
     * @return
     */
    private Node<K, V> locate_leaf(K v, int lastlock, Deque<ResultCouple<K, V>> descent) throws IOException {

        Node<K, V> n, m;
        int h, enterheight;

        /*
         * ubleftsep stands for "upper bound on the leftsep of the current node". This value is recorded for
         * each node on the descent stack so that an ascending process can tell if it's too far to the right.
         */
        K ubleftsep;

        lock_anchor(READ_LOCK);
        n = anchor.fast;
        enterheight = anchor.fastheight;
        ubleftsep = positiveInfinity;
        unlock_anchor(READ_LOCK);

        descent.clear();

        for (h = enterheight; h > 1; --h) { // v > leftsep (n)
            ResultCouple<K, V> move_right = move_right(v, n, ubleftsep, READ_LOCK); // v € coverset(n)
            n = move_right.node;
            ubleftsep = move_right.ubleftsep;
            descent.push(move_right);
            try {

                final ResultCouple<K, V> find = n.find(v, ubleftsep); // v > leftsep (m)
                m = find.node;
                ubleftsep = find.ubleftsep;

            } catch (IOException e) {

                throw new IOException("failed to find key " + v + " on leaf " + n.pageId, e);

            } finally {

                unlock(n, READ_LOCK);
            }

            n = m;
        }

        ResultCouple<K, V> move_right = move_right(v, n, ubleftsep, lastlock); // v € coverset(n)
        n = move_right.node;
        ubleftsep = move_right.ubleftsep;

        return n;

    }

//    procedure move-right(v: value; var n: nodeptr; var ubleftsep: value; rw: locktype);
//    { move-right scans along a level starting with node n until it comes to a node into whose coverset v falls (trivially, n itself).
//    It assumes that no lock is held on n initially, and leaves a lock
//    of the kind specified in rw on the final node. }
//    var
//        m: nodeptr;
//    begin {assume v > leftsep (n)}
//        lock(n, rw);
//        while empty(n) or (rightsep(n) < v) do begin { v > leftsep (n) }
//        if empty(n) then m := outlink(n) { v > leftsep (n) = leftsep (m) }
//        else begin
//            m := rightlink(n); {v > rightsep(n) = leftsep(m) }
//            ubleftsep := rightsep(n);
//        end;
//        unlock(n, rw);
//        lock(m, rw)
//        n := m;
//        end;
//    end;
    /**
     * move-right scans along a level starting with node n until it comes to a
     * node into whose coverset v falls (trivially, n itself). It assumes that
     * no lock is held on n initially, and leaves a lock of the kind specified
     * in rw on the final node.
     *
     * @param v
     * @param n
     * @param ubleftsep
     * @param rw
     * @return
     */
    private ResultCouple<K, V> move_right(K v, Node<K, V> n, K ubleftsep, int rw) {

        Node<K, V> m;

        // assume v > leftsep (n)
        lock(n, rw);

        while (n.empty() || n.rightsep().compareTo(v) < 0) { // v > leftsep (n)

            if (n.empty()) {
                m = n.outlink(); // v > leftsep (n) = leftsep (m)
            } else {
                m = n.rightlink(); // v > rightsep(n) = leftsep(m)
                ubleftsep = n.rightsep();
            }

            unlock(n, rw);
            lock(m, rw);
            n = m;
        }

        return new ResultCouple<>(n, ubleftsep);
    }

//    procedure normalize(n: nodeptr; descent: stack; atheight: height);
//    { normalize makes sure that node n is not too crowded
//    or sparse by performing a split or merge as needed.
//    A split may be necessary after a merge, n is assumed to be write-locked.
//    descent and atheight are needed to ascend to the level above to complete a split or merge. }
//    var
//        sib, newsib: nodeptr;
//        sep, newsep: value;
//    begin
//        if too-sparse(n) and (rightlink(n) <> nil) then begin
//            sib := rightlink(n);
//            lock(sib, writelock);
//            sep := half-merge(n, sib);
//            unlock(sib, writelock);
//            spawn(ascend(remove, sep, sib, atheight+1, descent))
//        end;
//        if too-crowded(n) then begin
//            allocate-node(newsib);
//            newsep := half-split(n, newsib);
//            spawn(ascend(add, newsep, newsib, atheight+1, descent))
//        end
//    end;
    /**
     * normalize makes sure that node n is not too crowded or sparse by
     * performing a split or merge as needed. A split may be necessary after a
     * merge, n is assumed to be write-locked. descent and at height are needed
     * to ascend to the level above to complete a split or merge.
     *
     * @param n
     * @param descent
     */
    private void normalize(Node<K, V> n, Deque<ResultCouple<K, V>> descent, int atheight, Queue<CriticJob> maintenance) throws IOException {

        Node<K, V> sib, newsib;
        K sep, newsep;

        if (n.too_sparse() && (n.rightlink() != null)) {

            sib = n.rightlink();

            lock(sib, WRITE_LOCK);
            try {
                sep = n.half_merge(sib);
            } finally {
                unlock(sib, WRITE_LOCK);
            }

            spawn(() -> ascend(REMOVE_TASK, sep, sib, atheight + 1, clone(descent), maintenance), maintenance);

            /* Having merged a node we could potentially lower the root or shrink it too much to be effective, run a
             * critic check */

 /*
             * TODO: improve the critic execution heuristic. Actual heuristic is very conservative but it
             * could be run many fewer times! (run critic every x merge of node size?)
             */
            spawn(() -> run_critic(), maintenance);
        }

        if (n.too_crowded()) {

            newsib = allocate_node(n.leaf);
            newsep = n.half_split(newsib);

            /* Nothing to load locked now */
            final Metadata meta = policy.add(newsib);
            if (meta != null) {
                meta.owner.unload(meta.pageId);
            }

            spawn(() -> ascend(ADD_TASK, newsep, newsib, atheight + 1, clone(descent), maintenance), maintenance);
        }

    }

//    procedure ascend(t: task; sep: value; child: nodeptr; toheight: height; descent: stack);
//    { adds or removes separator sep and downlink to child at height toheight,
//    using the descent stack to ascend to it. }
//    var
//        n: nodeptr;
//        ubleftsep: value;
//    begin n := locate-internal(sep, toheight, descent)
//        while not add-or-remove-link(task, sep, child, n, toheight, descent) do begin
//            { wait and try again, very rare }
//            unlock(n, writelock);
//            delay; { sep > teftsep(n) }
//            move-right(sep, n, ubleftsep, writelock) { sep € coverset(n) }
//        end;
//        normalize(n, descent, toheight);
//        unlock(n, writelock)
//    end;
    /**
     * adds or removes separator sep and downlink to child at height toheight,
     * using the descent stack to ascend to it.
     *
     * @param task
     * @param sep
     * @param child
     * @param toheight
     * @param descent
     */
    private void ascend(int t, K sep, Node<K, V> child, int toheight, Deque<ResultCouple<K, V>> descent, Queue<CriticJob> maintenance) throws IOException {
        Node<K, V> n;
        K ubleftsep;

        ResultCouple<K, V> locate_internal = locate_internal(sep, toheight, descent, maintenance);
        n = locate_internal.node;
        ubleftsep = locate_internal.ubleftsep;

        try {

            while (!add_or_remove_link(t, sep, child, n, toheight, descent, maintenance)) {
                // wait and try again, very rare
                unlock(n, WRITE_LOCK);
                delay(1L); // sep > teftsep(n)
                ResultCouple<K, V> move_right = move_right(sep, n, ubleftsep, WRITE_LOCK); // sep € coverset(n)
                n = move_right.node;
                ubleftsep = move_right.ubleftsep;
            }

            normalize(n, descent, toheight, maintenance);

        } finally {

            unlock(n, WRITE_LOCK);

        }
    }

//    function add-or-remove-link(t: task; sep: value; child: nodeptr;
//    n: nodeptr; atheight: height; descent: stack): boolean;
//    { tries to add or removes sep and downlink to child from
//    node n and returns true if succeeded, if removing,
//    and sep is rightmost in n, merges n with its right neighbor first,
//    (if the resulting node is too large, it will be split by the upcoming normalization.). A solution that avoids this merge exists,
//    but we present this for the sake of simplicity. }
//    var
//        sib: nodeptr;
//        newsep: value;
//    begin
//        if t=add then add-or-remove-link := add-link(sep, child, n)
//        else begin {t= remove}
//            if rightsep(n) = sep then begin
//                { the downlink to be removed is in n's right neighbor. }
//                sib := rightlink(n); {rightsep(n) = sep < +inf, thus rightlink(n)<>nil
//                lock(sib, writelock);
//                newsep := half-merge(n, sib); {newsep = sep}
//                unlock(sib, writelock);
//                spawn(ascend(remove, newsep, sib, atheight+1, descent))
//            end;
//            add-or-remove-link := remove-link(sep, child, n)
//        end
//    end;
    /**
     * tries to add or removes sep and downlink to child from node n and returns
     * true if succeeded, if removing, and sep is rightmost in n, merges n with
     * its right neighbor first, (if the resulting node is too large, it will be
     * split by the upcoming normalization.). A solution that avoids this merge
     * exists, but we present this for the sake of simplicity.
     *
     * @param t
     * @param sep
     * @param child
     * @param n
     * @param atheight
     * @param descent
     * @return
     */
    private boolean add_or_remove_link(int t, K sep, Node<K, V> child, Node<K, V> n, int atheight, Deque<ResultCouple<K, V>> descent, Queue<CriticJob> maintenance) throws IOException {

        Node<K, V> sib;
        K newsep;

        if (t == ADD_TASK) {
            return n.add_link(sep, child);
        } else {
            if (n.rightsep().equals(sep)) {
                // the downlink to be removed is in n's right neighbor.
                sib = n.rightlink(); // rightsep(n) = sep < +inf, thus rightlink(n)<>nil
                lock(sib, WRITE_LOCK);
                try {
                    newsep = n.half_merge(sib); // newsep = sep
                } catch (IOException ex) {
                    throw new UncheckedIOException("failed to remove link from " + n.pageId + " to " + sib.pageId, ex);
                } finally {
                    unlock(sib, WRITE_LOCK);
                }
                spawn(() -> ascend(REMOVE_TASK, newsep, sib, atheight + 1, clone(descent), maintenance), maintenance);
            }
            return n.remove_link(sep, child);
        }
    }

//    function locate-internal(v: value; toheight: height; var descent: stack): nodeptr;
//    { a modified locate phase; instead of finding a leaf whose coverset includes
//    V, finds a node at height toheight whose coverset includes v.
//    if possible, uses the descent stack (whose top points at toheight) }
//    var
//        n, m, newroot: nodeptr;
//        h, enterheight: height;
//        ubleftsep: value;
//    begin
//        if empty-stack(descent) then ubleftsep := +inf { force new descent }
//        else pop(n, ubleftsep, descent);
//        if v < = ubleftsep then begin
//            { a new descent from the top must be made}
//            lock-anchor(readlock);
//            if anchor. topheight < toheight then begin
//                unlock-anchor(readlock); lock-anchor(writelock);
//                if anchor. topheight < toheight then begin
//                    allocate-node(newroot);
//                    grow(newroot)
//                end;
//                unlock-anchor(writelock); lock-anchor(readlock)
//            end;
//            if anchor. fastheight > = toheight then begin
//                n := anchor. fast; enterheight := anchor. fastheight
//            end
//            else begin
//                n := anchor. top; enterheight := anchor. topheight
//            end;
//            ubleftsep := +inf; { v > leftsep(n) }
//            unlock-anchor(readlock);
//            set-to-empty (descent);
//            for h := enterheight downto toheight+1 do begin { v > leftsep(n) }
//                move-right(v, n, ubleftsep, readlock);{ v € coverset(n) }
//                push(n, ubleftsep, descent);
//                (m, ubleftsep) := find(v, n, ubleftsep); { v > leftsep(m) }
//                unlock(n, readlock);
//                n := m
//            end
//        end;
//        { v > leftsep(n), height of n = toheight }
//        move-right(v, n, ubleftsep, writelock); { v € coverset(n) }
//        locate-internal := n
//    end;
    /**
     * a modified locate phase; instead of finding a leaf whose coverset
     * includes v, finds a node at height toheight whose coverset includes v. if
     * possible, uses the descent stack (whose top points at toheight)
     *
     * @param v
     * @param toheight
     * @param descent
     * @return
     */
    private ResultCouple<K, V> locate_internal(K v, int toheight, Deque<ResultCouple<K, V>> descent, Queue<CriticJob> maintenance) throws IOException {

        Node<K, V> n, m, newroot;
        int h, enterheight;
        K ubleftsep;

        if (descent.isEmpty()) {
            /*
             * Just to avoid "The local variable n may not have been initialized" at last move_right.
             *
             * If there isn't descent ubleftsep is +inf then the check ubleftsep.comparteTo(v) > 0 will always
             * be true and a root will be retrieved
             */
            n = null;

            ubleftsep = positiveInfinity; // force new descent
        } else {
            ResultCouple<K, V> pop = descent.pop();
            n = pop.node;
            ubleftsep = pop.ubleftsep;
        }

        if (ubleftsep.compareTo(v) > 0) { // invert the check ubleftsep isn't always a K
            // a new descent from the top must be made
            lock_anchor(READ_LOCK);
            if (anchor.topheight < toheight) {
                unlock_anchor(READ_LOCK);
                lock_anchor(WRITE_LOCK);
                if (anchor.topheight < toheight) {
                    newroot = allocate_node(false);
                    grow(newroot);
                    /* Nothing to load locked now */
                    final Metadata meta = policy.add(newroot);
                    if (meta != null) {
                        meta.owner.unload(meta.pageId);
                    }
                    spawn(() -> run_critic(), maintenance);
                }
                unlock_anchor(WRITE_LOCK);
                lock_anchor(READ_LOCK);
            }
            if (anchor.fastheight >= toheight) {
                n = anchor.fast;
                enterheight = anchor.fastheight;
            } else {
                n = anchor.top;
                enterheight = anchor.topheight;
            }

            ubleftsep = positiveInfinity; // v > leftsep(n)
            unlock_anchor(READ_LOCK);
            descent.clear();
            for (h = enterheight; h > toheight; --h) { // v > leftsep(n)
                ResultCouple<K, V> move_right = move_right(v, n, ubleftsep, READ_LOCK); // v € coverset(n)
                try {

                    n = move_right.node;
                    ubleftsep = move_right.ubleftsep;
                    descent.push(move_right);
                    ResultCouple<K, V> find = n.find(v, ubleftsep); // v > leftsep(m)
                    m = find.node;
                    ubleftsep = find.ubleftsep;

                } finally {

                    unlock(n, READ_LOCK);

                }
                n = m;
            }
        }

        // v > leftsep(n), height of n = toheight
        ResultCouple<K, V> move_right = move_right(v, n, ubleftsep, WRITE_LOCK); // v € coverset(n)
        n = move_right.node;
        ubleftsep = move_right.ubleftsep;

        return move_right;
    }

//    procedure critic;
//    { the critic runs continuously; its function is to keep the target
//    of the fast pointer in the anchor close to the highest level containing more than one downlink. }
//    var
//        n, m: nodeptr;
//        h: height;
//    begin
//        while true do begin
//            lock-anchor(readlock);
//            n := anchor. top; h := anchor. topheight;
//            unlock-anchor(readlock);
//            lock(n, readlock);
//            while numberofchildren(n)< = 3 and rightlink(n) = nil and h> 1 do begin
//                m := leftmostchild(n);
//                unlock(n, readlock);
//                n := m;
//                lock(n, readlock);
//                h := h - 1
//            end;
//            unlock(n, readlock):
//            lock-anchor(readlock);
//            if anchor. fastheight = h then
//                unlock-anchor(readlock)
//            else begin
//                unlock-anchor(readlock);
//                lock-anchor(writelock);
//                anchor.fastheight := h; anchor.fast := n;
//                unlock-anchor(writelock)
//            end;
//            delay
//        end
//    end;
    /**
     * Custom critic version to be ran from querying threads. Avoid to run more
     * than a critic a time
     *
     * <p>
     * original comment: the critic runs continuously; its function is to keep
     * the target of the fast pointer in the anchor close to the highest level
     * containing more than one downlink.
     * </p>
     *
     */
    private final AtomicBoolean criticRunning = new AtomicBoolean(false);

    private void run_critic() throws IOException {

        if (!criticRunning.compareAndSet(false, true)) {
            /* Someone else is already running a critic, we don't need to run it twice */
            return;
        }

        Node<K, V> n, m;
        int h;

        lock_anchor(READ_LOCK);
        n = anchor.top;
        h = anchor.topheight;
        unlock_anchor(READ_LOCK);
        lock(n, READ_LOCK);

        try {

            while (n.number_of_children() < CRITIC_MIN_CHILDREN && n.rightlink() == null && h > 1) {

                try {

                    m = n.leftmost_child();

                } catch (IOException e) {

                    throw new IOException("failed to find leftmost child on node " + n.pageId, e);

                } finally {

                    unlock(n, READ_LOCK);
                }

                n = m;
                lock(n, READ_LOCK);
                --h;
            }

        } catch (IOException e) {

            throw new IOException("failed to evaluate anchor fast height", e);

        } finally {

            unlock(n, READ_LOCK);
        }

        lock_anchor(READ_LOCK);

        if (anchor.fastheight == h) {
            unlock_anchor(READ_LOCK);
        } else {
            unlock_anchor(READ_LOCK);
            lock_anchor(WRITE_LOCK);
            anchor.fastheight = h;
            anchor.fast = n;
            unlock_anchor(WRITE_LOCK);
        }

        criticRunning.set(false);
    }

    /**
     * Differently from original algorithm <i>spawning</i> means just enqueuing
     * maintenance work at for execution at the end of insert/update/delete
     */
    private void spawn(CriticJob runnable, Queue<CriticJob> maintenance) {
        maintenance.offer(runnable);
    }

    private void handleMainenance(Queue<CriticJob> maintenance) {
        try {
            while (!maintenance.isEmpty()) {
                maintenance.poll().execute();
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("failed to handle Blink maintenance", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Deque<ResultCouple<K, V>> clone(Deque<ResultCouple<K, V>> descent) {
        return (Deque<ResultCouple<K, V>>) ((LinkedList<ResultCouple<K, V>>) descent).clone();
    }

    /*
     * The search structure operations. Locking ensures that they are atomic.
     */
    private Node<K, V> allocate_node(boolean leaf) {
        final Long nodeID = nextID.getAndIncrement();
        final Node<K, V> node = new Node<>(nodeID, leaf, this, positiveInfinity);
        nodes.put(nodeID, node);

        return node;
    }

    /**
     * n is made an internal node containing only a downlink to the current
     * target of the anchor's top pointer and the separator +inf to its right.
     * The anchor's top pointer is then set to point to n, and its height
     * indicator is incremented.
     *
     * @param n
     */
    private void grow(Node<K, V> n) {
        n.grow(anchor.top);

        anchor.top = n;
        anchor.topheight++;
    }

    private void lock_anchor(int locktype) {
        lock(anchor.lock, locktype);
    }

    private void unlock_anchor(int locktype) {
        unlock(anchor.lock, locktype);
    }

    private void lock(Node<K, V> n, int locktype) {
//
//      System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Locking  " + n + " " + (locktype == READ_LOCK ? "r" : "w"));
//
//      try {
//          Lock lock;
//          if (locktype == READ_LOCK) {
//              lock = locks.get(n).readLock();
//          } else {
//              lock = locks.get(n).writeLock();
//          }
//          if (!lock.tryLock(3, TimeUnit.SECONDS)) {
//              System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " --------------> Deadlock " + n);
//
//              Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
//
//              for( Thread thread : threadSet )
//                  for (StackTraceElement ste : thread.getStackTrace()) {
//                      System.out.println("T" + Thread.currentThread().getId() + " TD" + thread.getId() + " -> " + ste);
//              }
//
//              throw new InternalError("Deadlock " + n);
//          }
//      } catch (InterruptedException e) {
//          throw new InternalError("interrupt " + n);
//      }
//
//      System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Lock     " + n + " " + (locktype == READ_LOCK ? "r" : "w"));

        lock(n.lock, locktype);
    }

    private void unlock(Node<K, V> n, int locktype) {
//
//        try {
//
//            Lock lock;
//            if (locktype == READ_LOCK) {
//                lock = locks.get(n).readLock();
//            } else {
//                lock = locks.get(n).writeLock();
//            }
//
//            lock.unlock();
//
//        } catch (Exception e) {
//            System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " --------------> UNLOCK FAIL " + n + " " + (locktype == READ_LOCK ? "r" : "w"));
//
//            System.out.println("T" + Thread.currentThread().getId() + " TD" + Thread.currentThread().getId() + " UNLOCK FAIL -> " + e);
//            System.out.println("T" + Thread.currentThread().getId() + " TD" + Thread.currentThread().getId() + " UNLOCK FAIL -> " + e.getMessage());
//            e.printStackTrace(System.out);
//            for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
//                System.out.println("T" + Thread.currentThread().getId() + " TD" + Thread.currentThread().getId() + " UNLOCK FAIL -> " + ste);
//            }
//        }
//
//        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " Unlocked " + n + " " + (locktype == READ_LOCK ? "r" : "w"));

        unlock(n.lock, locktype);
    }

    private void lock(ReadWriteLock lock, int locktype) {
        if (locktype == READ_LOCK) {
            lock.readLock().lock();
        } else {
            lock.writeLock().lock();
        }
    }

    private void unlock(ReadWriteLock lock, int locktype) {
        if (locktype == READ_LOCK) {
            lock.readLock().unlock();
        } else {
            lock.writeLock().unlock();
        }
    }

    private void delay(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException soaked) {/* SOAK */
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "BLink [anchor=" + anchor
                + ", nextID=" + nextID
                + ", keys=" + size()
                + ", maxSize=" + maxSize
                + ", minSize=" + minSize
                + ", closed=" + closed
                + "]";
    }

    /**
     * Build a full string representation of this tree.
     * <p>
     * <b>Pay attention:</b> it will load/unload nodes potentially polluting the
     * page replacement policy. Use this method just for test and analysis
     * purposes.
     * </p>
     *
     * @return full tree string representation
     */
    @SuppressWarnings("unchecked")
    public String toStringFull() {

        Node<K, V> top = anchor.top;

        Deque<Object[]> stack = new LinkedList<>();
        Set<Node<K, V>> seen = new HashSet<>();

        stack.push(new Object[]{top, 0});

        int indents;

        try {

            StringBuilder builder = new StringBuilder();
            while (!stack.isEmpty()) {

                Object[] el = stack.pop();
                Node<K, V> node = (Node<K, V>) el[0];
                indents = (int) el[1];

                for (int i = 0; i < indents; ++i) {
                    builder.append("-");
                }

                builder.append("> ");

                if (seen.contains(node)) {
                    builder.append("Seen: ").append(node.pageId).append('\n');

                } else {
                    seen.add(node);
                    builder.append(node).append(' ');

                    if (node.leaf) {

                        Deque<Object[]> cstack = new LinkedList<>();

                        /* No other nodes currently loaded and can't require to unload itself */
                        final LockAndUnload<K, V> loadLock = node.loadAndLock(true);
                        try {
                            for (Entry<K, V> child
                                    : (Collection<Entry<K, V>>) (Collection<?>) node.map.entrySet()) {

                                builder.append(child.getValue()).append(" <- ").append(child.getKey()).append(" | ");
                            }

                            builder.setLength(builder.length() - 3);

                        } finally {
                            loadLock.unlock();
                            if (loadLock.unload != null) {
                                loadLock.unload();
                            }
                        }

                        while (!cstack.isEmpty()) {
                            stack.push(cstack.pop());
                        }

                    } else {
                        Deque<Object[]> cstack = new LinkedList<>();

                        /* No other nodes currently loaded and can't require to unload itself */
                        final LockAndUnload<K, V> loadLock = node.loadAndLock(true);
                        try {
                            for (Entry<K, Node<K, V>> child
                                    : (Collection<Entry<K, Node<K, V>>>) (Collection<?>) node.map.entrySet()) {

                                builder.append(child.getValue().pageId).append(" <- ").append(child.getKey()).append(" | ");
                                cstack.push(new Object[]{child.getValue(), indents + 1});
                            }

                            builder.setLength(builder.length() - 3);
                        } finally {
                            loadLock.unlock();
                            if (loadLock.unload != null) {
                                loadLock.unload();
                            }
                        }

                        while (!cstack.isEmpty()) {
                            stack.push(cstack.pop());
                        }
                    }

                    builder.append('\n');
                }
            }

            return builder.toString();

        } catch (IOException ex) {

            throw new UncheckedIOException("failed to generate full string representation", ex);

        }

    }

    private static interface CriticJob {

        public void execute() throws IOException;

    }

//  var
//  anchor: record
//    fast: nodeptr; fastheight: height;
//    top: nodeptr; topheight: height;
//  end;
    private static final class Anchor<X extends Comparable<X>, Y> {

        final ReadWriteLock lock;

        /*
         * Next fields won't need to be volatile. They are written only during write lock AND no other thread
         * will have an opportunity do read this field until the lock is released.
         */
        Node<X, Y> fast;
        int fastheight;

        Node<X, Y> top;
        int topheight;

        /**
         * The first leaf
         */
        Node<X, Y> first;

        public Anchor(Node<X, Y> root) {
            this(root, 1, root, 1, root);
        }

        public Anchor(Node<X, Y> fast, int fastheight, Node<X, Y> top, int topheight, Node<X, Y> first) {
            super();

            this.fast = fast;
            this.fastheight = fastheight;
            this.top = top;
            this.topheight = topheight;
            this.first = first;

            lock = new ReentrantReadWriteLock(false);
        }

        public void reset(Node<X, Y> root) {
            this.fast = root;
            this.fastheight = 1;
            this.top = root;
            this.topheight = 1;
            this.first = root;
        }

        @Override
        public String toString() {
            return "Anchor [fast=" + fast.pageId
                    + ", fastheight=" + fastheight
                    + ", top=" + top.pageId
                    + ", topheight=" + topheight
                    + ", first=" + first
                    + "]";
        }
    }

    private static final class Node<X extends Comparable<X>, Y> extends BLinkPage<X, Y> {

        /**
         * <pre>
         * herddb.index.blink.BLink$Node object internals:
         *  OFFSET  SIZE                                         TYPE DESCRIPTION                               VALUE
         *       0    12                                              (object header)                           N/A
         *      12     4                       herddb.core.Page.Owner Page.owner                                N/A
         *      16     8                                         long Page.pageId                               N/A
         *      24     4                    herddb.core.Page.Metadata Page.metadata                             N/A
         *      28     4                                          int Node.keys                                 N/A
         *      32     8                                         long Node.storeId                              N/A
         *      40     8                                         long Node.flushId                              N/A
         *      48     8                                         long Node.size                                 N/A
         *      56     1                                      boolean Node.leaf                                 N/A
         *      57     1                                      boolean Node.empty                                N/A
         *      58     1                                      boolean Node.loaded                               N/A
         *      59     1                                      boolean Node.dirty                                N/A
         *      60     4     java.util.concurrent.locks.ReadWriteLock Node.lock                                 N/A
         *      64     4     java.util.concurrent.locks.ReadWriteLock Node.loadLock                             N/A
         *      68     4   java.util.concurrent.ConcurrentSkipListMap Node.map                                  N/A
         *      72     4                         java.lang.Comparable Node.rightsep                             N/A
         *      76     4             herddb.index.blink.nn.BLink.Node Node.outlink                              N/A
         *      80     4             herddb.index.blink.nn.BLink.Node Node.rightlink                            N/A
         *      84     4                                              (loss due to the next object alignment)
         * Instance size: 88 bytes
         * Space losses: 0 bytes internal + 4 bytes external = 4 bytes total
         * </pre>
         *
         * And still adding one of each:
         * <pre>
         * COUNT       AVG       SUM   DESCRIPTION
         *   272        32      8704   java.util.concurrent.ConcurrentHashMap$Node
         *   141        48      6768   java.util.concurrent.ConcurrentSkipListMap
         *     8        16       128   java.util.concurrent.ConcurrentSkipListMap$EntrySet
         *   209        32      6688   java.util.concurrent.ConcurrentSkipListMap$HeadIndex
         *  3464        24     83136   java.util.concurrent.ConcurrentSkipListMap$Index
         *     2        16        32   java.util.concurrent.locks.ReentrantLock
         *     2        32        64   java.util.concurrent.locks.ReentrantLock$NonfairSync
         *   283        24      6792   java.util.concurrent.locks.ReentrantReadWriteLock
         *   283        48     13584   java.util.concurrent.locks.ReentrantReadWriteLock$NonfairSync
         *   283        16      4528   java.util.concurrent.locks.ReentrantReadWriteLock$ReadLock
         *   283        16      4528   java.util.concurrent.locks.ReentrantReadWriteLock$Sync$ThreadLocalHoldCounter
         *   283        16      4528   java.util.concurrent.locks.ReentrantReadWriteLock$WriteLock
         *
         * One of each: 320 bytes
         * </pre>
         */
        static final long NODE_CONSTANT_SIZE = 408L;
        static final long ENTRY_CONSTANT_SIZE = /* ConcurrentSkipListMap$Node */ 42L;

        long storeId;

        /**
         * Flush page id, used to reduce space consumption recycling stored
         * pages. They will be used for storeId during checkpoint if no changes
         * were done after last flush
         */
        long flushId;

        final boolean leaf;

        /**
         * Node access lock
         */
        final ReadWriteLock lock;

        /**
         * Node data load/unload lock ({@link #map})
         */
        final ReadWriteLock loadLock;

        /**
         * Inner nodes will have Long values, leaves Y values
         */
        NavigableMap<X, Object> map;

        /*
         * Next fields won't need to be volatile. They are written only during write lock AND no other thread
         * will have an opportunity do read this field until the lock is released.
         */
        /**
         * Managed key set size, {@link NavigableMap} size could not be a a O(1) operation.
         */
        int keys;

        /**
         * Managed byte size node occupancy
         */
        long size;

        X rightsep;

        Node<X, Y> outlink;

        Node<X, Y> rightlink;

        boolean empty;

        boolean loaded;

        volatile boolean dirty;

        Node(long id, boolean leaf, BLink<X, Y> tree, X rightsep) {

            super(tree, id);

            this.storeId = BLinkIndexDataStorage.NEW_PAGE;
            this.flushId = BLinkIndexDataStorage.NEW_PAGE;

            this.leaf = leaf;

            this.empty = false;

            this.rightsep = rightsep;

            this.lock = new ReentrantReadWriteLock(false);
            this.loadLock = new ReentrantReadWriteLock(false);

            this.map = newNodeMap();

            this.keys = 0;
            this.size = NODE_CONSTANT_SIZE;

            /* New live node, loaded and dirty by default */
            this.loaded = true;
            this.dirty = true;
        }

        private static final <A,B> NavigableMap<A, B> newNodeMap() {
            return new TreeMap<>();
        }

        /**
         * Create a node from his metadata.
         * <p>
         * Doesn't set {@link #outlink} and {@link #rightlink} because they
         * could still not exist in a starting up tree.
         * </p>
         *
         * @param metadata
         * @param tree
         */
        Node(BLinkNodeMetadata<X> metadata, BLink<X, Y> tree) {

            super(tree, metadata.id);

            this.storeId = metadata.storeId;
            this.flushId = BLinkIndexDataStorage.NEW_PAGE;

            this.leaf = metadata.leaf;

            this.empty = metadata.empty;

            this.rightsep = metadata.rightsep;

            this.lock = new ReentrantReadWriteLock(false);
            this.loadLock = new ReentrantReadWriteLock(false);

            this.map = newNodeMap();

            this.keys = metadata.keys;
            this.size = metadata.bytes;

            /* Old stored node, unloaded and clean by default */
            this.loaded = false;
            this.dirty = false;
        }

        /* ********************************* */
 /* *** FOR BOTH LEAVES AND NODES *** */
 /* ********************************* */
        boolean empty() {
            return empty;
        }

        boolean too_sparse() {
            return size < owner.minSize;
        }

        boolean too_crowded() {
            return size > owner.maxSize;
        }

        X rightsep() {
            /*
             * Could be an positiveInfinity... pay real attention how you check it (positiveInfinity can compare to
             * any key but no otherwise)
             */
            return rightsep;
        }

        Node<X, Y> outlink() {
            return outlink;
        }

        Node<X, Y> rightlink() {
            return rightlink;
        }

        /**
         * The sequence in r is transferred to the end of the sequence in l. The
         * rightlink of l is directed to the target of the rightlink in r, r is
         * marked empty, its outlink pointing to l. If l is a leaf, its
         * separator is set to the largest key in it. The previous value of the
         * rightmost separator in l is returned.
         */
        X half_merge(Node<X, Y> right) throws IOException {

            // Cast to K, is K for sure because it has a right sibling
            final X half_merge = rightsep;

            LockAndUnload<X, Y> thisLoadLock = null;
            LockAndUnload<X, Y> rightLoadLock = null;

            boolean thisUnloaded = false;
            boolean rightUnloaded = false;

            try {
                try {

                    /* Could require to unload right but we need the data */
                    thisLoadLock = this.loadAndLock(false);

                    /* Not unloaded if needed or if we can't acquire a lock */
                    thisUnloaded = thisLoadLock.unloadIfNot(right, owner);

                    /* Could require to unload this but we need the data */
                    rightLoadLock = right.loadAndLock(false);

                    /* Not unloaded if needed or if we can't acquire a lock */
                    rightUnloaded = rightLoadLock.unloadIfNot(this, owner);

                    // the sequence in r is transferred to the end of the sequence in l
                    map.putAll(right.map);

                    dirty = true;

                    // r is marked empty
                    right.empty = true;
                    right.map.clear();
                    right.map = newNodeMap();

                } finally {

                    /*
                     * Unload need a write lock: if a lock (actually read) is held no lock upgrade
                     * is possible so we need first to read unlock then write lock (lock not
                     * usable). So we load unlock first and then we unload to avoid deadlocking
                     */

 /* Unlock pages */
                    if (thisLoadLock != null) {
                        thisLoadLock.unlock();
                    }

                    if (rightLoadLock != null) {
                        rightLoadLock.unlock();
                    }

                    /* Unload pages outside locks */
                    if (thisLoadLock != null && !thisUnloaded) {
                        thisLoadLock.unload();
                    }

                    if (rightLoadLock != null && !rightUnloaded) {
                        rightLoadLock.unload();
                    }
                }
            } catch (IOException e) {

                throw new IOException("failed to half merge " + right.pageId + " into " + pageId, e);

            }
            // add copied keys and size
            keys += right.keys;
            size += right.size - NODE_CONSTANT_SIZE; // just one node!

            right.keys = 0;
            right.size = 0;

            // the rightlink of l is directed to the target of the rightlink in r
            rightlink = right.rightlink;

            // its outlink pointing to l
            right.outlink = this;

            // If l is a leaf, its separator is set to the largest key in it
            rightsep = right.rightsep;

            // the previous value of the rightmost separator in l is returned.
            return half_merge;

        }

        /**
         * The rightlink of new is directed to the target of the rightlink of n.
         * The rightlink of n is directed to new. The right half of the sequence
         * in n is moved to new. If n and new are leaves, their separators are
         * set equal to the largest keys in them. The return value is the new
         * rightmost separator in n.
         */
        @SuppressWarnings("unchecked")
        X half_split(Node<X, Y> right) throws IOException {

            // the rightlink of new is directed to the target of the rightlink of n
            right.rightlink = rightlink;

            // the rightlink of n is directed to new
            rightlink = right;

            // the right half of the sequence in n is moved to new
            long limit = (size - NODE_CONSTANT_SIZE) / 2L;
            long keeping = 0L;
            int count = 0;

            X lastKey = null;
            /*
             * No other nodes currently loaded (right isn't currently known to page policy) and can't require
             * to unload itself.
             */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {

                boolean toright = false;
                for (Iterator<Entry<X, Object>> entryIt = map.entrySet().iterator();
                        entryIt.hasNext(); ) {
                    Entry<X, Object> entry = entryIt.next();
                    if (toright) {
                        right.map.put(entry.getKey(), entry.getValue());
                        entryIt.remove();
                    } else {
                        ++count;
                        if (leaf) {
                            keeping += owner.evaluator.evaluateAll(entry.getKey(), (Y) entry.getValue()) + ENTRY_CONSTANT_SIZE;
                        } else {
                            keeping += owner.evaluator.evaluateKey(entry.getKey()) + ENTRY_CONSTANT_SIZE;
                        }
                        if (keeping >= limit) {
                            toright = true;
                            lastKey = entry.getKey();
                        }
                    }
                }

                dirty = true;

            } finally {
                loadLock.unlock();
            }

            if (loadLock.unload != null) {
                loadLock.unload();
            }

            right.keys = keys - count;
            keys = count;

            right.size = size - keeping;
            size = keeping + NODE_CONSTANT_SIZE;

            // if n and new are leaves, their separators are set equal to the largest keys in them
            right.rightsep = rightsep;

            rightsep = lastKey;

            // the return value is the new rightmost separator in n
            // Cast to K, is K for sure because it has a right sibling
            return rightsep;
        }

        /* ****************** */
 /* *** FOR LEAVES *** */
 /* ****************** */
        /**
         * Copy ranged values
         */
        @SuppressWarnings("unchecked")
        List<Entry<X, Y>> copyRange(X start, boolean startInclusive, X end, boolean endInclusive) throws IOException {

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);

            final Map<X, Object> sub;
            try {

                /*
                 * In reality ConcurrentSkipListMap.subMap could handle null start/end but it places some null
                 * checks. So we have to check start/end nullity too and route to the right method that will...
                 * route all to the same non visible procedure :( :( :(
                 */
                if (start == null) {

                    if (startInclusive) {
                        throw new NullPointerException("Null inclusive start");
                    }

                    if (end == null) {

                        if (endInclusive) {
                            throw new NullPointerException("Null inclusive end");
                        }

                        sub = map;

                    } else {
                        sub = map.headMap(end, endInclusive);
                    }
                } else {

                    if (end == null) {

                        if (endInclusive) {
                            throw new NullPointerException("Null inclusive end");
                        }

                        sub = map.tailMap(start, startInclusive);

                    } else {

                        sub = map.subMap(start, startInclusive, end, endInclusive);
                    }
                }

                final List<Entry<X, Y>> list = new ArrayList<>();

                /* Cast to Y: is a leaf */
                for (Entry<X, Y> entry : (Set<Entry<X, Y>>) (Set<?>) sub.entrySet()) {
                    /*
                     * Manually copy the entry set, using costructor with a collection would copy an
                     * array of elements taken from the map entry set, such array is build iterating
                     * all entry set elements to build an array list and then copying his internal
                     * array! Iterating directly we avoid such multiple copy overhead.
                     */
                    list.add(entry);
                }

                return list;

            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }
        }

        /**
         * Check if the leaf contains the key.
         */
        @SuppressWarnings("unchecked")
        Y check_key(X key) throws IOException {
            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {
                /* Cast to Y: is a leaf */
                return (Y) map.get(key);
            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }
        }

        /**
         * If the leaf doen't contains key it is added into the sequence of keys
         * at an appropriate location an returns true otherwise false.
         */
        @SuppressWarnings("unchecked")
        Y add_key(X key, Y value) throws IOException {
            final Y old;

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {
                /* Cast to Y: is a leaf */
                old = (Y) map.put(key, value);

                dirty = true;
            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }

            if (old == null) {
                ++keys;
                size += owner.evaluator.evaluateAll(key, value) + ENTRY_CONSTANT_SIZE;
                return null;
            } else {
                /* TODO: this could be avoided if we can ensure that every value will have the same size */
                size += owner.evaluator.evaluateValue(value) - owner.evaluator.evaluateValue(old);
                return old;
            }
        }

        /**
         * add a key only if current mapping match with expected one, returns {@code true} if added/replaced
         */
        boolean add_key_if(X key, Y value, Y expected) throws IOException {
            final Y old;

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {

                if (expected == null) {
                    /* Cast to Y: is a leaf */
                    old = (Y) map.putIfAbsent(key, value);

                    /* If there was a value and we didn't expect something abort replacement */
                    if (old != null) {
                        return false;
                    }
                } else {
                    /*
                     * We need to keep track if the update was really done. Reading computeIfPresent result won't
                     * suffice, it can be equal to newPage even if no replacement was done (the map contained already
                     * newPage mapping and expectedPage was different)
                     */
                    Holder<Boolean> replaced = new Holder<>(Boolean.FALSE);
                    Holder<Y> hold = new Holder<>();
                    final boolean o;
                    map.computeIfPresent(key, (skey, svalue) -> {
                        if (svalue.equals(expected)) {
                            replaced.value = Boolean.TRUE;
                            /* Cast to Y: is a leaf */
                            hold.value = (Y) svalue;
                            return value;
                        }

                        return svalue;
                    });

                    /* If we didn't find expected mapping abort replacement */
                    if (!replaced.value.booleanValue()) {
                        return false;
                    }

                    old = hold.value;
                }

                dirty = true;
            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }

            if (old == null) {
                ++keys;
                size += owner.evaluator.evaluateAll(key, value) + ENTRY_CONSTANT_SIZE;
            } else {
                /* TODO: this could be avoided if we can ensure that every value will have the same size */
                size += owner.evaluator.evaluateValue(value) - owner.evaluator.evaluateValue(old);
            }

            return true;
        }

        /**
         * If the leaf contains key it is removed and true is returned,
         * otherwise returns false.
         */
        @SuppressWarnings("unchecked")
        Y remove_key(X key) throws IOException {
            final Y old;

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {
                /* Cast to Y: is a leaf */
                old = (Y) map.remove(key);

                if (old != null) {
                    dirty = true;
                }

            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }

            if (old == null) {
                return null;
            } else {
                --keys;
                size -= owner.evaluator.evaluateAll(key, old) + ENTRY_CONSTANT_SIZE;
                return old;
            }
        }

        /* ***************** */
 /* *** FOR NODES *** */
 /* ***************** */
        @SuppressWarnings("unchecked")
        Node<X, Y> leftmost_child() throws IOException {

            // TODO: move the knowledge on metadata to avoid page loading during critic?

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {
                return (Node<X, Y>) map.firstEntry().getValue();
            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }
        }

        int number_of_children() {
            return keys;
        }

        void grow(Node<X, Y> downlink) {
            /* No load checks... a "growing" root isn't actually known at page policy */

            // it assumes that node is empty!
            map.put(owner.positiveInfinity, downlink);
            ++keys;

            /* positiveInfinity being singleton is practically considered 0 size */
            size += ENTRY_CONSTANT_SIZE;
        }

        /**
         * The smallest si in the node such that v <= si is identified. If i > 1
         * returns (pi,si-1) otherwise (pi,ubleftsep).
         */
        @SuppressWarnings("unchecked")
        ResultCouple<X, Y> find(X v, X ubleftsep) throws IOException {

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {

                /*
                 * ...,(pi-1,si-1),(pi,si),(pi+1,si+1)...
                 */
                final Entry<X, Object> first = map.firstEntry();

                /* Check if is the first */
                if (first.getKey().compareTo(v) >= 0) {
                    /* First: i == 1 -> return (pi,ubleftsep) */
                    return new ResultCouple<>((Node<X, Y>) first.getValue(), ubleftsep);
                }

                final X key = map.lowerKey(v);

                final Entry<X, Object> ceiling = map.ceilingEntry(v);

                /* Not the first: i > 1 return (pi,si-1) */
                return new ResultCouple<>((Node<X, Y>) ceiling.getValue(), key);

            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }

        }

        /**
         * The smallest index = i such that si >= s identified. If si = s, the
         * operation returns false. Otherwise, it changes the sequence in parent
         * to (.., pi,s,child,si,...) and returns true.
         */
        boolean add_link(X s, Node<X, Y> child) throws IOException {

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {
                final Entry<X, Object> ceiling = map.ceilingEntry(s);

                if (ceiling.getKey().compareTo(s) == 0) {
                    return false;
                }

                /* First add new */
                map.put(s, ceiling.getValue());

                /* Then overwrite old */
                map.put(ceiling.getKey(), child);

                dirty = true;
            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }

            ++keys;
            size += owner.evaluator.evaluateKey(s) + ENTRY_CONSTANT_SIZE;
            return true;
        }

        /**
         * If the sequence in node includes a separator s on the immediate left
         * of a downlink to child, the two are removed and true is returned;
         * otherwise the return value is false
         *
         * @param s
         * @param child
         * @return
         */
        boolean remove_link(X s, Node<X, Y> child) throws IOException {

            /* No other nodes currently loaded and can't require to unload itself */
            final LockAndUnload<X, Y> loadLock = loadAndLock(true);
            try {

                /*
                 * ...,(pi-1,si-1),(pi,si),(pi+1,si+1)...
                 *
                 * s := si
                 * pi+1 == child?
                 */
                @SuppressWarnings("unchecked")
                final Node<X, Y> pi = (Node<X, Y>) map.get(s);
                if (pi == null) {
                    return false;
                }

                final Entry<X, Object> eip1 = map.higherEntry(s);
                if (eip1.getValue().equals(child)) {

                    /*
                     * the two are removed...
                     *
                     * from: ...,(pi-1,si-1),(pi,si),(pi+1,si+1)...
                     * to: ...,(pi-1,si-1),(pi,si+1)...
                     */
                    map.put(eip1.getKey(), pi);
                    map.remove(s);

                    dirty = true;

                    --keys;
                    size -= owner.evaluator.evaluateKey(s) + ENTRY_CONSTANT_SIZE;

                    return true;

                }

                return false;

            } finally {
                loadLock.unlock();

                if (loadLock.unload != null) {
                    loadLock.unload();
                }
            }
        }


        /* ******************** */
 /* *** PAGE LOADING *** */
 /* ******************** */
        /**
         * With {@code doUload} parameter to {@code true} will attempt to unload
         * eventual pages before exit from this method.
         */
        final LockAndUnload<X, Y> loadAndLock(boolean doUnload) throws IOException {

            Metadata unload = null;
            final Lock read = loadLock.readLock();

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " read lock requested " + pageId + " loadAndLock");
            }

            read.lock();

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " read lock taken " + pageId + " loadAndLock");
            }

            if (!loaded) {

                /*
                 * We need an upgrade from read to write, with ReentrantReadWriteLock isn't possible thus we
                 * release current read lock and retrieve a write lock before recheck the condition.
                 */
                read.unlock();

                if (DEBUG) {
                    LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " read lock released " + pageId + " loadAndLock");
                }

                final Lock write = loadLock.writeLock();

                if (DEBUG) {
                    LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock requested " + pageId + " loadAndLock");
                }

                write.lock();

                if (DEBUG) {
                    LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock taken " + pageId + " loadAndLock");
                }

                try {
                    /* Recheck condition (Another thread just loaded the node?) */
                    if (!loaded) {

                        /* load */
                        readPage(flushId == BLinkIndexDataStorage.NEW_PAGE ? storeId : flushId);
                        loaded = true;

                        unload = owner.policy.add(this);

                    } else {

                        owner.policy.pageHit(this);
                    }

                    if (DEBUG) {
                        LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " read lock requested " + pageId + " loadAndLock");
                    }

                    /* Downgrade the lock (permitted) */
                    read.lock();

                    if (DEBUG) {
                        LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " read lock taken " + pageId + " loadAndLock");
                    }

                } catch (RuntimeException | IOException err) {

                    throw new IOException("failed to read node " + pageId, err);

                } finally {
                    write.unlock();

                    if (DEBUG) {
                        LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock released " + pageId + " loadAndLock");
                    }
                }

                if (doUnload && unload != null) {
                    /* Attempt to unload metatada */
                    if (owner.attemptUnload(unload)) {
                        unload = null;
                    }
                }

            } else {
                owner.policy.pageHit(this);
            }

            return new LockAndUnload<>(read, unload, pageId);
        }

        boolean unload(boolean flush, boolean justTry) {

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock requested " + pageId + " unload");
            }

            /* No data cannot change during checkpoint! */
            final Lock lock = loadLock.writeLock();

            if (justTry) {
                boolean acquired = lock.tryLock();
                if (!acquired) {
                    if (DEBUG) {
                        LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock taken " + pageId + " tryunload");
                        LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock released " + pageId + " tryunload");
                    }

                    return false;
                }
            } else {
                lock.lock();
            }

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock taken " + pageId + " unload");
            }

            try {

                if (!loaded) {
                    return false;
                }

                if (flush && dirty) {
                    try {
                        flush();
                    } catch (IOException e) {

                        /* Avoid any reuse of a possible broken/half-flushed page */
                        flushId = BLinkIndexDataStorage.NEW_PAGE;

                        throw new UncheckedIOException("failed to flush node " + pageId, e);
                    }
                }

                map.clear();
                map = null;
                loaded = false;

                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("unloaded node " + pageId);
                }

                return true;

            } finally {
                lock.unlock();

                if (DEBUG) {
                    LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock released " + pageId + " unload");
                }
            }
        }

        /**
         * Flush node changes to disk
         * <p>
         * <b>Must</b> be invoked when already holding {@link loadLock} write
         * lock.
         * </p>
         *
         * @throws IOException
         */
        void flush() throws IOException {

            if (DEBUG) {
                if (!((ReentrantReadWriteLock) loadLock).isWriteLockedByCurrentThread()) {
                    throw new AssertionError("Write lock for " + pageId + " not held during flush!");
                }
            }

            if (loaded && dirty) {
                /* Overwrite/NewPage */
                flushId = writePage(flushId);
                dirty = false;

                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.log(Level.FINE, "flush node " + pageId + ": page -> " + flushId + " with " + keys + " keys x " + size + " bytes");
                }
            }
        }

        BLinkMetadata.BLinkNodeMetadata<X> checkpoint() throws IOException {

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock requested " + pageId + " checkpoint");
            }

            /* No data cannot change during checkpoint! */
            final Lock lock = loadLock.writeLock();
            lock.lock();

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock taken " + pageId + " checkpoint");
            }

            try {

                if (loaded && dirty) {

                    /* New page */
                    storeId = writePage(BLinkIndexDataStorage.NEW_PAGE);

                    /*
                     * "Reset" flush status: if not reset the next load would fetch an older flush and not the
                     * right checkpoint.
                     */
                    flushId = BLinkIndexDataStorage.NEW_PAGE;

                    dirty = false;

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.log(Level.FINE, "checkpoint node " + pageId + ": newpage -> " + storeId + " with " + keys + " keys x " + size + " bytes");
                    }
                } else {

                    /*
                     * No changes on data page from last flush, we can use directly last flush (obviously only
                     * if last flush occurred). No last flush could happen with:
                     *
                     * a) old nodes never changed since last flush id reuse
                     *
                     * b) old nodes from previous boot and untouched since then (so no flushId)
                     *
                     * It cannot happen with new nodes never flushed because they should be dirty too (and
                     * thus they fall in the other if branch).
                     */
                    if (flushId != BLinkIndexDataStorage.NEW_PAGE) {

                        /* Flush recycle */
                        storeId = flushId;

                        /*
                         * "Reset" flush status: we reused flush page as checkpoint one thus we need to force a
                         * new flush page when needed.
                         */
                        flushId = BLinkIndexDataStorage.NEW_PAGE;

                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.log(Level.FINE, "checkpoint node " + pageId + ": from existing flush -> " + storeId + " with " + keys + " keys x " + size + " bytes");
                        }
                    }

                }

                return new BLinkNodeMetadata<>(
                        leaf,
                        pageId,
                        storeId,
                        empty,
                        keys,
                        size,
                        outlink == null ? BLinkNodeMetadata.NO_LINK : outlink.pageId,
                        rightlink == null ? BLinkNodeMetadata.NO_LINK : rightlink.pageId,
                        rightsep);

            } finally {
                lock.unlock();

                if (DEBUG) {
                    LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " write lock released " + pageId + " checkpoint");
                }
            }
        }

        /**
         * Write/overwrite a data page. If no page id is given
         * {@link BLinkIndexDataStorage#NEW_PAGE} a new page is created,
         * otherwise old one is overwritten.
         *
         * @param pageId
         * @return
         * @throws IOException
         */
        private long writePage(long pageId) throws IOException {
            if (leaf) {
                return writeLeafPage(pageId);
            } else {
                return writeNodePage(pageId);
            }
        }

        @SuppressWarnings("unchecked")
        private long writeNodePage(long pageId) throws IOException {
            final Map<X, Long> pointers = new HashMap<>(keys);
            map.forEach((x, y) -> {
                pointers.put(x, ((Node<X, Y>) y).pageId);
            });

            if (pageId == BLinkIndexDataStorage.NEW_PAGE) {
                return owner.storage.createNodePage(pointers);
            }

            owner.storage.overwriteNodePage(pageId, pointers);
            return pageId;
        }

        @SuppressWarnings("unchecked")
        private long writeLeafPage(long pageId) throws IOException {

            if (pageId == BLinkIndexDataStorage.NEW_PAGE) {
                return owner.storage.createLeafPage((Map<X, Y>) (Map<?, ?>) map);
            }

            owner.storage.overwriteLeafPage(pageId, (Map<X, Y>) (Map<?, ?>) map);
            return pageId;
        }

        private void readPage(long pageId) throws IOException {
            if (leaf) {
                readLeafPage(pageId);
            } else {
                readNodePage(pageId);
            }
        }

        private void readNodePage(long pageId) throws IOException {
            final Map<X, Long> data = new HashMap<>();
            owner.storage.loadNodePage(pageId, data);
            map = newNodeMap();

            if (size == UNKNOWN_SIZE) {
                size = NODE_CONSTANT_SIZE;
                data.forEach((x, y) -> {
                    Node<X, Y> node = owner.nodes.get(y);
                    map.put(x, node);

                    long entrySize;
                    if (x == owner.positiveInfinity) {
                        /* positiveInfinity being singleton is practically considered 0 size */
                        entrySize = ENTRY_CONSTANT_SIZE;
                    } else {
                        entrySize = owner.evaluator.evaluateKey(x) + ENTRY_CONSTANT_SIZE;
                    }
                    size += entrySize;
                });
            } else {
                /* No need to recalculate page size */
                data.forEach((x, y) -> {
                    Node<X, Y> node = owner.nodes.get(y);
                    map.put(x, node);
                });
            }
        }

        private void readLeafPage(long pageId) throws IOException {
            map = newNodeMap();
            owner.storage.loadLeafPage(pageId, (Map<X,Y>) map);

            /* Recalculate size if needed */
            if (size == UNKNOWN_SIZE) {
                size = NODE_CONSTANT_SIZE;
                /* Avoid a double entries loop and both put and evaluate in one loop */
                map.forEach((x, y) -> {
                    size += owner.evaluator.evaluateAll(x, (Y) y) + ENTRY_CONSTANT_SIZE;
                });
            }
        }

        @Override
        public String toString() {
            return "Node [id=" + pageId
                    + ", leaf=" + leaf
                    + ", empty=" + empty
                    + ", nkeys=" + keys
                    + ", size=" + size
                    + ", outlink=" + (outlink == null ? null : outlink.pageId)
                    + ", rightlink=" + (rightlink == null ? null : rightlink.pageId)
                    + ", rightsep=" + rightsep
                    + "]";
        }
    }

    private static class BLinkPage<X extends Comparable<X>, Y> extends Page<BLink<X, Y>> {

        public BLinkPage(BLink<X, Y> owner, long pageId) {
            super(owner, pageId);
        }

    }

    private static final class ResultCouple<X extends Comparable<X>, Y> {

        final Node<X, Y> node;
        final X ubleftsep;

        ResultCouple(Node<X, Y> node, X ubleftsep) {
            super();
            this.node = node;
            this.ubleftsep = ubleftsep;
        }

        @Override
        public String toString() {
            return "[node=" + node + ", ubleftsep=" + ubleftsep + "]";
        }
    }

    private static final class LockAndUnload<X extends Comparable<X>, Y> {

        final Lock lock;
        final Metadata unload;
        final long pageId;

        public LockAndUnload(Lock lock, Metadata unload, long pageId) {
            super();
            this.lock = lock;
            this.unload = unload;
            this.pageId = pageId;
        }

        public boolean unloadIfNot(Node<X, Y> node, BLink<X, Y> tree) throws IOException {

            /* If nothing to unload is a success */
            if (unload == null) {
                return true;
            }

            /* If is requested node (right page + right owner) do not unload */
            if (node.pageId == unload.pageId && unload.owner == tree) {
                return false;
            }

            /* Do real unload */
            return tree.attemptUnload(unload);
        }

        public void unload() throws IOException {
            try {
                unload.owner.unload(unload.pageId);
            } catch (RuntimeException e) {
                throw new IOException("failed to unload " + unload.pageId);
            }
        }

        public void unlock() {
            lock.unlock();

            if (DEBUG) {
                LOGGER.fine(System.nanoTime() + " " + Thread.currentThread().getId() + " read lock released " + pageId + " LockAndUnload.unlock");
            }
        }

        @Override
        public String toString() {
            return "LockAndUnload [unload=" + unload + "]";
        }

    }

    /**
     * A dummy ever empty, discarding {@link Deque}, useful when queue isn't
     * really needed
     *
     * @author diego.salvi
     */
    private static final class DummyDeque<E> implements Deque<E> {

        private static final Object[] EMPTY_ARRAY = new Object[0];

        @SuppressWarnings("rawtypes")
        public static final Deque INSTANCE = new DummyDeque();

        /**
         * No instances! Use {@link #getInstance()}
         */
        private DummyDeque() {
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Object[] toArray() {
            return EMPTY_ARRAY;
        }

        @Override
        public <T> T[] toArray(T[] a) {
            Arrays.fill(a, null);
            return a;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {
        }

        @Override
        public void addFirst(E e) {
        }

        @Override
        public void addLast(E e) {
        }

        @Override
        public boolean offerFirst(E e) {
            return false;
        }

        @Override
        public boolean offerLast(E e) {
            return false;
        }

        @Override
        public E removeFirst() {
            return null;
        }

        @Override
        public E removeLast() {
            throw new NoSuchElementException();
        }

        @Override
        public E pollFirst() {
            return null;
        }

        @Override
        public E pollLast() {
            return null;
        }

        @Override
        public E getFirst() {
            throw new NoSuchElementException();
        }

        @Override
        public E getLast() {
            throw new NoSuchElementException();
        }

        @Override
        public E peekFirst() {
            return null;
        }

        @Override
        public E peekLast() {
            return null;
        }

        @Override
        public boolean removeFirstOccurrence(Object o) {
            return false;
        }

        @Override
        public boolean removeLastOccurrence(Object o) {
            return false;
        }

        @Override
        public boolean add(E e) {
            return false;
        }

        @Override
        public boolean offer(E e) {
            return false;
        }

        @Override
        public E remove() {
            throw new NoSuchElementException();
        }

        @Override
        public E poll() {
            return null;
        }

        @Override
        public E element() {
            throw new NoSuchElementException();
        }

        @Override
        public E peek() {
            return null;
        }

        @Override
        public void push(E e) {
        }

        @Override
        public E pop() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterator<E> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<E> descendingIterator() {
            return Collections.emptyIterator();
        }

    }

    private final class ScanIterator implements Iterator<Entry<K, V>> {

        private boolean nextChecked;

        private Iterator<Entry<K, V>> current;
        private Node<K, V> node;

        private K lastRead;

        private K rightsep;

        private final K end;
        private final boolean inclusive;

        public ScanIterator(Node<K, V> node, K start, boolean sinclusive, K end, boolean einclusive) throws UncheckedIOException {

            this.end = end;
            this.inclusive = einclusive;

            this.lastRead = start;

            this.node = node;

            try {
                /* Copy values to quicly release read lock */
                final List<Entry<K, V>> list = node.copyRange(start, sinclusive, end, einclusive);

                current = list.iterator();

                rightsep = node.rightsep;

            } catch (IOException e) {

                throw new UncheckedIOException("failed to copy data from node " + node.pageId, e);

            } finally {
                unlock(node, READ_LOCK);
            }

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

                    if (rightsep != positiveInfinity) {

                        /* Check if new data was added to the node due to merge operations */
                        ResultCouple<K, V> move_right = move_right(lastRead, node, rightsep, READ_LOCK);
                        node = move_right.node;

                        /* Use custom comparator, both node.rightsep and rightsep could be a positiveInfinity instance */
                        if (node.rightsep.compareTo(rightsep) > 0) {

                            /* rightsep changed from last separator... there could be other data to read in the node. */
                            rightsep = node.rightsep;

                            try {
                                /* Copy values to quicly release read lock */
                                final List<Entry<K, V>> list = node.copyRange(lastRead, false, end, inclusive);

                                current = list.iterator();

                            } catch (IOException e) {

                                throw new UncheckedIOException("failed to copy data from node " + node.pageId, e);

                            } finally {
                                unlock(node, READ_LOCK);
                            }

                            if (current.hasNext()) {
                                return true;
                            }

                        }

                        node = jump_right(node, rightsep);

                        rightsep = node.rightsep;

                        try {
                            /* Copy values to quicly release read lock */
                            final List<Entry<K, V>> list = node.copyRange(lastRead, false, end, inclusive);

                            current = list.iterator();

                        } catch (IOException e) {

                            throw new UncheckedIOException("failed to copy data from node " + node.pageId, e);

                        } finally {
                            unlock(node, READ_LOCK);
                        }

                        if (current.hasNext()) {
                            return true;
                        }

                    }

                    /* We couln't get any more data do cleanup */

                    /* Cleanup: there is no interesting data at right */
                    rightsep = null;
                    lastRead = null;
                    node = null;
                    current = null;

                    return false;

                }
            }

            return false;
        }

        @Override
        public Entry<K, V> next() {

            if (current == null) {
                throw new NoSuchElementException();
            }

            if (!nextChecked && !hasNext()) {
                throw new NoSuchElementException();
            }

            nextChecked = false;

            final Entry<K, V> next = current.next();

            lastRead = next.getKey();

            return next;

        }

        /**
         * Differently from move_right given node <b>must be</b> locked
         */
        private Node<K, V> jump_right(Node<K, V> n, K rightsep) {

            Node<K, V> m;

            /*
             * Jump until we found a node not empty and with a right separator greater than given
             */
            while (n.empty() || n.rightsep().compareTo(rightsep) <= 0) {

                if (n.empty()) {
                    m = n.outlink(); // v > leftsep (n) = leftsep (m)
                } else {
                    m = n.rightlink(); // v > rightsep(n) = leftsep(m)
                }

                unlock(n, READ_LOCK);
                lock(m, READ_LOCK);
                n = m;
            }

            return n;
        }

    }
}
