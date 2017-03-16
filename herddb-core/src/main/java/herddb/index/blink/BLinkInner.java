package herddb.index.blink;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;

public final class BLinkInner<K extends Comparable<K>> implements BLinkNode<K> {

    private static final Logger LOGGER = Logger.getLogger(BLinkInner.class.getName());

    private final BLinkPage page;
    private volatile long storeId;

    private BLinkInner<K> substitute;

    private final BLinkIndexDataStorage<K> storage;
    private final PageReplacementPolicy policy;

    private final ReadWriteLock loadLock = new ReentrantReadWriteLock();

    private volatile boolean loaded;
    private volatile boolean dirty;

//    private Element<K> root;
    Element<K> root; // per string

    private final long maxElements;
    private final long minElements;
    private long elements;

    private final K highKey;
    private final BLinkPtr right;

    public BLinkInner(BLinkNodeMetadata<K> metadata, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {

        this.storage = storage;
        this.policy = policy;

        this.storeId = metadata.storeId;

        this.page = new BLinkPage(metadata.nodeId, this);

        this.maxElements = metadata.maxKeys;
        this.minElements = maxElements / 2;

        this.elements = metadata.keys;

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
        this.root = new Element<>(key1, value1);
        this.root.next = new Element<>(null, value2);

        this.highKey = null;

        this.right = BLinkPtr.empty();

        /* Dirty by default */
        this.dirty  = true;
        this.loaded = true;
    }

    private BLinkInner(long storeId, BLinkPage page, long maxElements, long elements, Element<K> root, K highKey, BLinkPtr right, BLinkIndexDataStorage<K> storage, PageReplacementPolicy policy) {
        super();

        this.storage = storage;
        this.policy = policy;

        this.storeId = storeId;

        this.page = page;

        this.maxElements = maxElements;
        this.minElements = maxElements / 2;

        this.elements = elements;
        this.root = root;

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
        return root.key;
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

                root = null;
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

                    Element<K> root = storage.loadPage(storeId);

                    this.root = root;

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

            Element<K> root = this.root;

            if (root == null) {
                return BLinkPtr.empty();
            }

            return BLinkPtr.page(root.page);

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

        final Lock lock = loadAndLock();
        try {

            /*
             * This is the sole procedure that can invoked on replaced nodes because isn't retrieved a write lock
             * for the node itself
             */
            Element<K> current = root;
            do {

                if (current.key == null) {

                    if (highKey != null && key.compareTo(highKey) >= 0) {
                        return right;
                    }

                    return BLinkPtr.page(current.page);

                } else {

                    int cmp = key.compareTo(current.key);

                    if (cmp < 0) {
                        return BLinkPtr.page(current.page);
                    }

                }

            } while ((current = current.next) != null);

        } finally {
            lock.unlock();
        }

        return BLinkPtr.empty();

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

        final Lock lock = loadAndLock();

        try {
            /* Lock already held for modifications */

            Element<K> current = root;
            Element<K> previous = null;
            do {

                if (current.key == null) {
                    /* Insert after last */
                    break;
                }

                final int cmp = current.key.compareTo(key);

                if (cmp < 0) {
                    previous = current;
                } else if ( cmp == 0 ) {
                    throw new InternalError("Update Key NOT expected!");

                } else {

                    /* Got the first element greater than we must insert between this and previous */
                    break;
                }

            } while ((current = current.next) != null);

            /* Proceed to insertion */
            final Element<K> n1 = new Element<>(current.key, pointer, current.next);
            final Element<K> n2 = new Element<>(key, current.page, n1);

            /* Link to previous chain, the element already point to "current" node! */
            if (previous != null) {
                previous.next = n2;
            } else {
                /* Linking before root */
                root = n2;
            }

        } finally {
            lock.unlock();
        }

        ++elements;

        dirty = true;

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

        Element<K> chainroot = null;
        Element<K> chaincurrent = null;
        Element<K> current = null;

        Element<K> aroot = null;
        K push = null;

        int count = 0;
        boolean insert = true;

        /* Retrieve lock to avoid concurrent page unloads */
        final Lock lock = loadAndLock();

        try {

            /*
             * TODO: scanning the whole sequence isn't really needed just scan till split point has been reached
             * AND the new element has been inserted then append the remaining sequence
             */
            current = root;

            Element<K> next;
            do {

                /* If still needs to insert */
                if (insert) {

                    /* First of all check if it needs to interleave the new key/value */

                    /* Force insertion if is the last element */
                    final int cmp = current.key == null ? /* Insert after last */ 1 : current.key.compareTo(key);

                    if (cmp > 0) {

                        /* Need to interleave the key/value */

                        if (count++ == splitpoint) {

                            /* Save the key as the "push" key */
                            push = key;

                            /* Attach the last element to current root */
                            next = new Element<>(null, current.page);

                            /* Attention! Split point cannot be the first element! (There cannot be a node with no key!)*/
                            chaincurrent.next = next;

                            /* Save old chain root as the new a root */
                            aroot = chainroot;

                            /* Reset the chain root */
                            chainroot = null;

                        } else {

                            /* Otherwise just append the element to the current chain  */
                            next = new Element<>(key, current.page);

                            if (chainroot == null) {
                                chainroot = next;
                            } else {
                                chaincurrent.next = next;
                            }

                            chaincurrent = next;
                        }

                        if (count++ == splitpoint) {

                            /* Save the key as the "push" key */
                            push = current.key;

                            /* Attach the last element to current root */
                            next = new Element<>(null, pointer);

                            /* Attention! Split point cannot be the first element! (There cannot be a node with no key!)*/
                            chaincurrent.next = next;

                            /* Save old chain root as the new a root */
                            aroot = chainroot;

                            /* Reset the chain root */
                            chainroot = null;

                        } else {

                            /* Otherwise just append the element to the current chain  */
                            next = new Element<>(current.key, pointer);

                            if (chainroot == null) {
                                chainroot = next;
                            } else {
                                chaincurrent.next = next;
                            }

                            chaincurrent = next;
                        }

                        /* Signal that the element has been inserted */
                        insert = false;

                        /* Need to advance because we have handled current element too */
                        if ((current = current.next) == null) {
                            break;
                        }

                    }

                }

                if (count++ == splitpoint) {

                    /* Save the key as the "push" key */
                    push = current.key;

                    /* Attach the last element to current root */
                    next = new Element<>(null, current.page);

                    /* Attention! Split point cannot be the first element! (There cannot be a node with no key!)*/
                    chaincurrent.next = next;

                    /* Save old chain root as the new a root */
                    aroot = chainroot;

                    /* Reset the chain root */
                    chainroot = null;

                } else {

                    /* Otherwise just append the element to the current chain  */
                    next = new Element<>(current.key, current.page);

                    if (chainroot == null) {
                        chainroot = next;
                    } else {
                        chaincurrent.next = next;
                    }

                    chaincurrent = next;
                }

            } while((current = current.next) != null);


            if (insert) {
                throw new InternalError(
                        "We should have inserted the node");
            }

            if (aroot == null) {
                throw new InternalError(
                        "We should have split the node");
            }

            /* Sets the root of chain b, chain a has already been set */
            Element<K> broot = chainroot;

    //      make high key of A' equal y;
            //      make right-link of A' point to B';
            BLinkInner<K> aprime = new BLinkInner<>(storeId, page, maxElements, splitpoint, aroot, push, BLinkPtr.link(newPage), storage, policy);

            /*
             * Replace page loading management owner... If we are to unload during this procedure the thread will
             * wait and then will see a new substitute owner pointing to the right owner!
             */
            substitute = aprime;
            page.owner.setOwner(aprime);

            BLinkPage bpage = new BLinkPage(newPage);
            //      make high key of B' equal old high key of A';
            //      make right-link of B' equal old right-link of A';
            BLinkInner<K> bprime = new BLinkInner<>(BLinkIndexDataStorage.NEW_PAGE, bpage,   maxElements, elements - splitpoint, broot, highKey, right, storage, policy);

            /* Set page owner after construction */
            bpage.owner.setOwner(bprime);

    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " push " + push );
    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " A " + aprime );
    //        System.out.println("T" + Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " B " + bprime );

            unload = policy.add(bpage);

            @SuppressWarnings("unchecked")
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

            Element<K> current = root;
            Element<K> previous = null;
            do {

                final int cmp = current.key.compareTo(key);

                if (cmp < 0) {
                    previous = current;
                } else if ( cmp == 0 ) {

                    /* Delete! */

                    if (previous == null) {
                        /* Delete root */
                        root = current.next;
                    } else {
                        /* Shortcut */
                        previous.next = current.next;
                    }

                    --elements;

                    dirty = true;

                    return this;

                } else {
                    break;
                }

            } while ((current = current.next) != null);

            throw new InternalError("An element to delete was expected!");

        } finally {
            lock.unlock();
        }

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

        Element<K> current = root;

        while(current != null) {

            if (current.key == null) {
                builder.setLength(builder.length() - 2);

                builder
                    .append(" -> ")
                    .append(current.page);

            } else {
                builder
                    .append(current.page)
                    .append(" <- ")
                    .append(current.key)
                    .append(", ");
            }

            current = current.next;
        }

        builder.append("]");

        return builder.toString();
    }

}