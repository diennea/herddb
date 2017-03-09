package herddb.index.blink;

public class BLinkInner<K extends Comparable<K>> implements BLinkNode<K> {



//  public static final void main(String[] args) {
//
//
////      BLinkInner<Integer> binner = new BLinkInner<>(1, 4, 10, 0, 10);
////
////      int i = 2;
////      while (binner.isSafe()) {
////          binner.insert(i * 10, i * 10);
////          ++i;
////
////          System.out.println(binner);
////      }
////
////      System.out.println(Arrays.toString(binner.split(i * 10, i * 10, 999)));
//
////      BLinkInner<Integer> binner = new BLinkInner<>(99, 3, 6, 1, 2);
////
////      Element<Integer> last = binner.root.next;
////      Element<Integer> e;
////
////      e = new Element<>(8, 12);
////      binner.root.next = e;
////      e.next = last;
////
////      binner.elements = 3;
////
////      binner.split(3, 13, 98);
//
//      BLinkInner<Integer> binner = new BLinkInner<>(99, 3, 6, 1, 2);
//
//      Element<Integer> last = binner.root.next;
//      Element<Integer> e;
//
//      e = new Element<>(8, 12);
//      binner.root.next = e;
//
//      e = new Element<>(15, 25);
//      binner.root.next.next = e;
//      e.next = last;
//
//      binner.elements = 3;
//
//      binner.split(3, 13, 98);
//
//      /*
//1 <- 6, 12 <- 8 -> 2   k3 ptr13
//1 <- 3 -> 13
//12 <- 8 -> 2
//p6!!! high? 6
//       */
//
//  }



//    public static final void main(String[] args) {
//
//        // [size: 1, page: 9, high: null, right: -1, data: 3 <- 14 -> 8] K 9 ptr 12
//
//        BLinkInner<Integer> binner = new BLinkInner<>(9, 3, 14, 3, 8);
//
//        System.out.println(binner);
//
//        binner.insert(9, 12);
//
//        System.out.println(binner);
//
//        /*
//         * 1 <- 6, 12 <- 8 -> 2 k3 ptr13 1 <- 3 -> 13 12 <- 8 -> 2 p6!!! high? 6
//         */
//
////        BLinkInner [size: 1, page: 9, high: null, right: -1, data: 3 <- 14 -> 8]
////        1 INSERT page 9 orig BLinkInner [size: 1, page: 9, high: null, right: -1, data: 3 <- 14 -> 8] K 9 ptr 12
////        1 INSERTED page 9 modified BLinkInner [size: 2, page: 9, high: null, right: -1, data: 3 <- 9, 12 <- 14 -> 8] K 9 ptr 12
////        BLinkInner [size: 2, page: 9, high: null, right: -1, data: 3 <- 9, 12 <- 14 -> 8]
//
//    }

    public static final void main(String[] args) {

//        Sequenza errata!
//        10 SPLIT page 3 orig BLinkInner [size: 3, page: 3, high: null, right: -1, data: 1 <- 3, 2 <- 5, 4 <- 7 -> 5] K 9 ptr 6
//        10 SPLIT page 3 push 7
//        10 SPLIT page 3 A BLinkInner [size: 2, page: 3, high: 7, right: -1, data: 1 <- 3, 2 <- 5 -> 6]
//        10 SPLIT page 3 B BLinkInner [size: 1, page: 7, high: null, right: -1, data: 5 <- 9 -> 6]

        BLinkInner<Integer> node = new BLinkInner<>(3, 3, 3, 1, 5);

        Element<Integer> current = node.root;
        Element<Integer> last = current.next;

        Element<Integer> next;

        next = new Element<>(5, 2);
        current.next = next;
        current = next;

        next = new Element<>(7, 4);
        current.next = next;
        current = next;

        current.next = last;

        node.elements = 3;

        System.out.println(node);

        node.split(9, 6, 7);

    }

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

    private void LOTHRUIN_CHECK(K key) {

        Element<K> element = root;

        do {

            if (element.key != null && element.key.equals(key)) {
                throw new InternalError("Checked a key already in the node!!!");
            }

        } while ((element = element.next) != null);

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

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );

//        LOTHRUIN_CHECK(key);

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

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERTED page " + this.page + " modified " + this + " K " + key + " ptr " + pointer );

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

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );

//        LOTHRUIN_CHECK(key);

        if (isSafe()) {
            throw new IllegalStateException("Invoking rearrange on a safe node");
        }

        /* Lock already held for modifications */

        final long splitpoint = (elements + 1) / 2;

        Element<K> chainroot = null;
        Element<K> chaincurrent = null;
        Element<K> current = null;

        Element<K> aroot = null;
        K push = null;

        int count = 0;
        boolean insert = true;

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
        BLinkInner<K> aprime = new BLinkInner<>(this.page, maxElements, splitpoint,            aroot, push);
//      make high key of B' equal old high key of A';
        BLinkInner<K> bprime = new BLinkInner<>(newPage,   maxElements, elements - splitpoint, broot, highKey);

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " push " + push );
//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " A " + aprime );
//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " B " + bprime );

//      make right-link of B' equal old right-link of A';
        bprime.right = right;
//      make right-link of A' point to B';
        aprime.right = bprime.page;

        @SuppressWarnings("unchecked")
        final BLinkNode<K>[] result = new BLinkNode[] { aprime, bprime };

        return result;
    }

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

        final K key;
        final long page;
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