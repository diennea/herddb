package herddb.index.blink;

import java.util.Arrays;

public class BLinkLeaf<K extends Comparable<K>>implements BLinkNode<K> {


    public static final void main(String[] args) {

        BLinkLeaf<Integer> node = new BLinkLeaf<>(99, 3, 1, 1);

        System.out.println(node);

        node.insert(2, 2);

        System.out.println(node);

        node.insert(1, 11);

        System.out.println(node);

        node.insert(4, 4);

        System.out.println(node);

        BLinkNode<?>[] split = node.split(3, 3, 98);

        System.out.println(Arrays.toString(split));

    }

    private final long page;

    private Element<K> root;

    private final long maxElements;
    private long elements;

    private K highKey;
    private long right;

    public BLinkLeaf(long page, long maxElements, K key, long value) {
        super();
        this.page = page;
        this.maxElements = maxElements;

        this.elements = 1;
        this.root = new Element<>(key, value);

        highKey = null;

        this.right = BLink.NO_PAGE;
    }

    private BLinkLeaf(long page, long maxElements, long elements, Element<K> root, K highKey) {
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
        return true;
    }

    @Override
    public boolean isSafe() {
        return elements < maxElements;
    }

    @Override
    public BlinkPtr scanNode(K key) {

        Element<K> current = root;
        do {
            int cmp = key.compareTo(current.key);

            if (cmp == 0) {
                return BlinkPtr.page(current.page);
            }
        } while ((current = current.next) != null);

        if (highKey != null && key.compareTo(highKey) >= 0) {
            return BlinkPtr.link(right);
        }

        return BlinkPtr.empty();

    }

    @Override
    public BLinkLeaf<K> insert(K key, long pointer) {

        try {


//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );

        /* Lock already held for modifications */

        Element<K> current = root;
        Element<K> previous = null;
        do {

            final int cmp = current.key.compareTo(key);

            if (cmp < 0) {
                previous = current;
            } else if ( cmp == 0 ) {

                /* Update! */
                final Element<K> replacement = new Element<>(key, pointer, current.next);

                if (previous == null) {
                    /* Updating root */
                    root = replacement;
                } else {
                    previous.next = replacement;
                }

                return this;

            } else {

                /* Got the first element greater than we must insert between this and previous */
                break;
            }

        } while ((current = current.next) != null);


        if (!isSafe()) {
            throw new IllegalStateException("Invoking a real insert (no update) on a unsafe node");
        }


        /* Proceed to insertion */
        final Element<K> inserted = new Element<>(key, pointer, current);

        /* Link to previous chain, the element already point to "current" node! */
        if (previous == null) {
            /* Linking before root */
            root = inserted;
        } else {
            previous.next = inserted;
        }

        ++elements;

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " INSERTED page " + this.page + " modified " + this + " K " + key + " ptr " + pointer );

        return this;

        } catch (Throwable t  ) {
//            System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " THROW page " + this.page + " modified " + this + " K " + key + " ptr " + pointer + " t " + t);
            t.printStackTrace();
            throw t;
        }
    }

    @Override
    public BLinkNode<K>[] split(K key, long pointer, long newPage) {

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " orig " + this + " K " + key + " ptr " + pointer );

        if (isSafe()) {
            throw new IllegalStateException("Invoking rearrange on a safe node");
        }

        /* Lock already held for modifications */

        final long splitpoint = (elements + 1) / 2;

        Element<K> current = root;

        Element<K> aroot = null;
        Element<K> broot = null;

        Element<K> acurrent = null;
        Element<K> bcurrent = null;

        int count = 0;
        boolean insert = true;

        do {

            if (insert) {
                final int cmp = current.key.compareTo(key);

                if (cmp > 0) {

                    /* Insert here! */
                    Element<K> next = new Element<>(key, pointer);

                    /* Check and force count increment */
                    if (count++ < splitpoint) {


                        if (acurrent == null) {
                            aroot = next;
                            acurrent = next;
                        } else {
                            acurrent.next = next;
                            acurrent = next;
                        }

                    } else {

                        if (bcurrent == null) {
                            broot = next;
                            bcurrent = next;
                        } else {
                            bcurrent.next = next;
                            bcurrent = next;
                        }

                    }

                    /* Signal that the element has been inserted */
                    insert = false;

                    /* Continue to append */

                } else if (cmp == 0) {
                    throw new InternalError("Replacement inside a split!!!");

                }

            }

            /* Append */
            Element<K> next = new Element<>(current.key, current.page);
            if (count++ < splitpoint) {


                if (acurrent == null) {
                    aroot = next;
                    acurrent = next;
                } else {
                    acurrent.next = next;
                    acurrent = next;
                }

            } else {

                if (bcurrent == null) {
                    broot = next;
                    bcurrent = next;
                } else {
                    bcurrent.next = next;
                    bcurrent = next;
                }

            }

        } while((current = current.next) != null);


        if (insert) {

            /* Insert here! */
            Element<K> next = new Element<>(key, pointer);
            if (count++ < splitpoint) {

                if (acurrent == null) {
                    aroot = next;
                    acurrent = next;
                } else {
                    acurrent.next = next;
                    acurrent = next;
                }

            } else {

                if (bcurrent == null) {
                    broot = next;
                    bcurrent = next;
                } else {
                    bcurrent.next = next;
                    bcurrent = next;
                }

            }
        }

//      make high key of A' equal y;
        BLinkLeaf<K> aprime = new BLinkLeaf<>(this.page, maxElements, splitpoint,         aroot, broot.key);
//      make high key of B' equal old high key of A';
        BLinkLeaf<K> bprime = new BLinkLeaf<>(newPage,   maxElements, count - splitpoint, broot, this.highKey);

//      make right-link of B' equal old right-link of A';
        bprime.right = right;
//      make right-link of A' point to B';
        aprime.right = bprime.page;

        @SuppressWarnings("unchecked")
        final BLinkNode<K>[] result = new BLinkNode[] { aprime, bprime };

//        System.out.println("SPLIT: " + this.page + " count " + splitpoint + " high " + acurrent.key);
//        System.out.println("NEW SPLIT: " + newPage + " count " + (count - splitpoint) + " high " + highKey);

//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " A " + aprime );
//        System.out.println(Thread.currentThread().getId() + " " + System.currentTimeMillis() + " SPLIT page " + this.page + " B " + bprime );

        return result;
    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();

        builder
            .append("BLinkLeaf [size: ").append(elements)
            .append(", page: ").append(page)
            .append(", high: ").append(highKey)
            .append(", right: ").append(right)
            .append(", data: ");

        Element<K> current = root;

        while(current != null) {

            builder
                .append("(")
                .append(current.key)
                .append(",")
                .append(current.page)
                .append("), ");

            current = current.next;
        }

        builder.setLength(builder.length() - 2);

        builder.append("]");

        return builder.toString();
    }

    private static final class Element<K> {
        private final K key;
        private final long page;

        private Element<K> next;

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