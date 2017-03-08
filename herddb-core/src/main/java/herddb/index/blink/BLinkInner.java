package herddb.index.blink;

public class BLinkInner<K extends Comparable<K>> implements BLinkNode<K> {

    private final long page;

//    private Element<K> root;
    Element<K> root; // per string

    private final long maxElements;
    private long elements;

    private K highKey;
    private long right;

    public BLinkInner(long page, long maxElements, K key1, long value1, long value2) {
        super();
        this.page = page;
        this.maxElements = maxElements;

        this.elements = 1;
        this.root = new Element<>(key1, value1);
        root.next = new Element<>(null, value2);

        highKey = null;

        this.right = BLink.NO_PAGE;
    }

//    public BLinkInner(long page, long maxElements, K key, long value) {
//        super();
//        this.page = page;
//        this.maxElements = maxElements;
//
//        this.elements = 1;
//        this.root = new Element<>(key, value);
//
//        highKey = key;
//
//        this.right = BLink.NO_PAGE;
//    }

    private BLinkInner(long page, long maxElements, long elements, Element<K> root, K highKey) {
        super();

        this.page = page;
        this.root = root;

        this.maxElements = maxElements;
        this.elements = elements;

        this.highKey = highKey;

        this.right = BLink.NO_PAGE;
    }

    @Override
    public long getPage() {
        return page;
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
    public boolean isLeaf() {
        return false;
    }

    @Override
    public boolean isSafe() {
        return elements < maxElements;
    }

    @Override
    public BlinkPtr scanNode(K key) {
        Element<K> current = root;
        do {

            if (current.key == null) {

                if (highKey != null && key.compareTo(highKey) >= 0) {
                    return BlinkPtr.link(right);
                }

                return BlinkPtr.page(current.page);

            } else {

                int cmp = key.compareTo(current.key);

                if (cmp < 0) {
                    return BlinkPtr.page(current.page);
                }

            }

        } while ((current = current.next) != null);

        return BlinkPtr.empty();
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

        System.out.println(Thread.currentThread().getId() + " INSERT page " + page + " orig " + this + " K " + key + " ptr " + pointer );

        if (!isSafe()) {
            throw new IllegalStateException("Invoking insert on a unsafe node");
        }

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

        ++elements;

        System.out.println(Thread.currentThread().getId() + " INSERTED page " + page + " modified " + this + " K " + key + " ptr " + pointer );

        return this;
    }


    public static final void main(String[] args) {


//        BLinkInner<Integer> binner = new BLinkInner<>(1, 4, 10, 0, 10);
//
//        int i = 2;
//        while (binner.isSafe()) {
//            binner.insert(i * 10, i * 10);
//            ++i;
//
//            System.out.println(binner);
//        }
//
//        System.out.println(Arrays.toString(binner.split(i * 10, i * 10, 999)));

//        BLinkInner<Integer> binner = new BLinkInner<>(99, 3, 6, 1, 2);
//
//        Element<Integer> last = binner.root.next;
//        Element<Integer> e;
//
//        e = new Element<>(8, 12);
//        binner.root.next = e;
//        e.next = last;
//
//        binner.elements = 3;
//
//        binner.split(3, 13, 98);

        BLinkInner<Integer> binner = new BLinkInner<>(99, 3, 6, 1, 2);

        Element<Integer> last = binner.root.next;
        Element<Integer> e;

        e = new Element<>(8, 12);
        binner.root.next = e;

        e = new Element<>(15, 25);
        binner.root.next.next = e;
        e.next = last;

        binner.elements = 3;

        binner.split(3, 13, 98);

        /*
1 <- 6, 12 <- 8 -> 2   k3 ptr13
1 <- 3 -> 13
12 <- 8 -> 2
p6!!! high? 6
         */

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
     *     A -> B -> C -> n -> r
     *    /    /    /    /
     *   A'   B'   C'   D'
     * </pre>
     *
     * the nodes will became (split isn't done on new key by necessity):
     * <pre>
     *     A -> X -> u    B -> C -> r
     *    /    /         /    /
     *   A'   B'        X'   C'
     * </pre>
     *
     *
     *
     * <pre>
     *     A -> B -> C -> D -> n -> r
     *    /    /    /    /    /
     *   A'   B'   C'   D'   N'
     * </pre>
     *
     * <pre>
     *     A -> X -> B -> C -> D -> n -> r
     *    /    /    /    /    /    /
     *   A'   B'   X'   C'   D'   N'
     * </pre>
     *
     * <pre>
     *     A -> X -> nn -> u    C -> D -> n -> r
     *    /    /    /          /    /    /
     *   A'   B'   X'         C'   D'   N'
     *
     * push B
     * </pre>
     * </p>
     */
    @Override
    public BLinkNode<K>[] split(K key, long value, long newPage) {

        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " orig " + this + " K " + key + " ptr " + value );

        if (isSafe()) {
            throw new IllegalStateException("Invoking rearrange on a safe node");
        }

        /* Lock already held for modifications */

        final long splitpoint = (elements + 1) / 2;



        int count = 0;
        boolean insert = true;

        /*
         * First create a complete chain with inserted element (for simplicity it will be done in a two step
         * process, it could be collapsed to one for performance)
         */

        Element<K> chainroot = null;
        Element<K> chaincurrent = null;
        Element<K> current = null;

        /*
         * Splitting node: his key will become null and pushed up, his next will become null breaking the chain
         * in two segments. Just avoid to scan the whole sequence twice
         */
        Element<K> split = null;

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

                    next = new Element<>(key, current.page);

                    if (chainroot == null) {
                        chainroot = next;
                    } else {
                        chaincurrent.next = next;
                    }
                    chaincurrent = next;

                    if (count++ == splitpoint) {
                        split = chaincurrent;
                    }

                    next = new Element<>(current.key, value);
                    chaincurrent.next = next;
                    chaincurrent = next;

                    if (count++ == splitpoint) {
                        split = chaincurrent;
                    }

                    /* Signal that the element has been inserted */
                    insert = false;

                    /* Need to advance because we have handled current element too */
                    if ((current = current.next) == null) {
                        break;
                    }

                }

            }

            next = new Element<>(current.key, current.page);
            if (chainroot == null) {
                chainroot = next;
            } else {
                chaincurrent.next = next;
            }
            chaincurrent = next;

            if (count++ == splitpoint) {
                split = chaincurrent;
            }

        } while((current = current.next) != null);



        /* Now we need to split the whole chain */

        Element<K> aroot = chainroot;
        Element<K> broot = split.next;


        /* Get the key to push (is the new high key for this node too) */
        K push = split.key;

        /* Break the chain in two segments */
        split.next = null;

        /* Cleanup the key on the last element */
        split.key = null;

        if (insert) {
            throw new InternalError(
                    "We should already have inserted the node");
        }


//      make high key of A' equal y;
        BLinkInner<K> aprime = new BLinkInner<>(this.page,    maxElements, splitpoint,    aroot, push);
//        BLinkInner<K> aprime = new BLinkInner<>(this.page,    maxElements, splitpoint,    aroot, push);
//      make high key of B' equal old high key of A';
//        BLinkInner<K> bprime = new BLinkInner<>(newPage, maxElements, count - splitpoint, broot, bcurrent.key);
        BLinkInner<K> bprime = new BLinkInner<>(newPage, maxElements, elements - splitpoint, broot, highKey);

        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " push " + push );
        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " A " + aprime );
        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " B " + bprime );

//      make right-link of B' equal old right-link of A';
        bprime.right = right;
//      make right-link of A' point to B';
        aprime.right = bprime.page;

        @SuppressWarnings("unchecked")
        final BLinkNode<K>[] result = new BLinkNode[] { aprime, bprime };

        return result;
    }
//    public BLinkNode<K>[] split(K key, long value, long newPage) {
//
//        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " orig " + this + " K " + key + " ptr " + value );
//
//        if (isSafe()) {
//            throw new IllegalStateException("Invoking rearrange on a safe node");
//        }
//
//        /*
//         * TODO: actually adhere strictly to Lehman algorithm and insert and rearrange with a full copy. Being
//         * linked elements it could proceed in two step: a) rearrange without insert tacking lock for b node
//         * too and pusing split nodes to all known memory; b) before releasing locks: detect to which node
//         * will host the new key and release the other; c) standard insert into node and release lock.
//         *
//         * Be aware that a such precedure could generate too small nodes at least temporary (what happen if
//         * node is split into k+1 and k keys and the new inserted key goes to the first block?)
//         */
//
//        /* Lock already held for modifications */
//
//        final long splitpoint = (elements + 1) / 2;
//
//
//
//        int count = 0;
//        boolean insert = true;
//
//
//        /*
//         * First create a complete chain with inserted element (for simplicity it will be done in a two step
//         * process, it could be collapsed to one for performance)
//         */
//
//        Element<K> aroot = null;
//        Element<K> broot = null;
//
//        Element<K> acurrent = null;
//        Element<K> bcurrent = null;
//
//        K push = null;
//
//        Element<K> current = root;
////        Element<K> pointerElement = current;
//
//
//        do {
//
//            /* If still needs to insert */
//            if (insert) {
//
//                /* First of all check if it needs to interleave the new key/value */
//
//                /* Force insertion if is the last element */
//                final int cmp = current.key == null ? /* Insert after last */ 1 : current.key.compareTo(key);
//
//                if (cmp > 0) {
//
//                    /* Need to interleave the key/value */
//
//
//                    if (count == splitpoint) {
//
//                        final Element<K> next = new Element<>(null, current.page);
//                        push = key;
//
//                        if (aroot == null) {
//                            throw new InternalError(
//                                    "Left root should have at least one value!");
//                        } else {
//                            acurrent.next = next;
//                            acurrent = next;
//                        }
//
//                    } else {
//
//                        final Element<K> next = new Element<>(key, current.page);
//
//                        if (count < splitpoint) {
//
//                            if (aroot == null) {
//                                aroot = next;
//                                acurrent = aroot;
//                            } else {
//                                acurrent.next = next;
//                                acurrent = next;
//                            }
//                        } else {
//
//
//                            if (broot == null) {
//                                broot = next;
//                                bcurrent = broot;
//                            } else {
//                                bcurrent.next = next;
//                                bcurrent = next;
//                            }
//                        }
//                    }
//                    ++count;
//
//                    if (count == splitpoint) {
//
//                        final Element<K> next = new Element<>(null, value);
//                        push = key;
//
//                        if (aroot == null) {
//                            throw new InternalError(
//                                    "Left root should have at least one value!");
//                        } else {
//                            acurrent.next = next;
//                            acurrent = next;
//                        }
//
//                    } else {
//
//                        final Element<K> next = new Element<>(current.key, value);
//
//                        if (count < splitpoint) {
//
//                            if (aroot == null) {
//                                aroot = next;
//                                acurrent = aroot;
//                            } else {
//                                acurrent.next = next;
//                                acurrent = next;
//                            }
//                        } else {
//
//
//                            if (broot == null) {
//                                broot = next;
//                                bcurrent = broot;
//                            } else {
//                                bcurrent.next = next;
//                                bcurrent = next;
//                            }
//                        }
//                    }
//                    ++count;
//
//                    /* Signal that the element has been inserted */
//                    insert = false;
//
//                    /* Need to advance because we have handled current element too */
//                    if ((current = current.next) == null) {
//                        break;
//                    }
//
//                }
//
//            }
//
//
//            if (count == splitpoint) {
//
//                final Element<K> next = new Element<>(null, current.page);
//                push = current.key;
//
//                if (aroot == null) {
//                    throw new InternalError(
//                            "Left root should have at least one value!");
//                } else {
//                    acurrent.next = next;
//                    acurrent = next;
//                }
//
//            } else {
//
//                final Element<K> next = new Element<>(current.key, current.page);
//
//                if (count < splitpoint) {
//
//                    if (aroot == null) {
//                        aroot = next;
//                        acurrent = aroot;
//                    } else {
//                        acurrent.next = next;
//                        acurrent = next;
//                    }
//                } else {
//
//
//                    if (broot == null) {
//                        broot = next;
//                        bcurrent = broot;
//                    } else {
//                        bcurrent.next = next;
//                        bcurrent = next;
//                    }
//                }
//            }
//            ++count;
//
//        } while((current = current.next) != null);
//
//
//        if (insert) {
//            throw new InternalError(
//                    "We should already have inserted the node");
//        }
//
//        if (push == null) {
//            throw new InternalError(
//                    "No real split happened!");
//        }
//
//
////      make high key of A' equal y;
//        BLinkInner<K> aprime = new BLinkInner<>(this.page,    maxElements, splitpoint,    aroot, push);
////        BLinkInner<K> aprime = new BLinkInner<>(this.page,    maxElements, splitpoint,    aroot, push);
////      make high key of B' equal old high key of A';
////        BLinkInner<K> bprime = new BLinkInner<>(newPage, maxElements, count - splitpoint, broot, bcurrent.key);
//        BLinkInner<K> bprime = new BLinkInner<>(newPage, maxElements, elements - splitpoint, broot, highKey);
//
//        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " push " + push );
//        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " A " + aprime );
//        System.out.println(Thread.currentThread().getId() + " SPLIT page " + page + " B " + bprime );
//
////      make right-link of B' equal old right-link of A';
//        bprime.right = right;
////      make right-link of A' point to B';
//        aprime.right = bprime.page;
//
//        @SuppressWarnings("unchecked")
//        final BLinkNode<K>[] result = new BLinkNode[] { aprime, bprime };
//
//        return result;
//    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();

        builder
            .append("BLinkInner [size: ").append(elements)
            .append(", page: ").append(page)
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


    static final class Element<K> {
//        private static final class Element<K> {
//        private K key;
//        private long page;

        K key;
        long page;
//        private Element<K> next;
        Element<K> next;

        public Element(K key, long page) {
            this(key,page,null);
        }

        public Element(K key, long page, Element<K> next) {
            super();
            this.key = key;
            this.page = page;
            this.next = next;
        }

        @Override
        public String toString() {
            return "Element [key=" + key + ", page=" + page + "]";
        }
    }

}