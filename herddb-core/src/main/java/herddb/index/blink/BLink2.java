package herddb.index.blink;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import herddb.index.blink.BLinkLeaf.BLinkLeafPtr;
import herddb.index.bp.mine.Sized;

public class BLink2<K extends Comparable<K>> {


//    public static void main(String[] args) {
//        BLink<Sized<Long>> tree = new BLink<>(3,5);
//
////        insert(10,tree);
////        insert(20,tree);
////        insert(30,tree);
////        insert(40,tree);
////        insert(50,tree);
////        insert(60,tree);
////
////
////        for(long i = 0; i < 100; ++i) {
////            System.out.println(i);
////            tree.insert(Sized.valueOf(i),i);
////        }
//
//
//        insert(5,tree);
//
//
//        long[] values = new long[1000];
//        for(int i = 0; i < 1000; ++i) {
//            values[i] = i;
//        }
//
//        Collections.shuffle(Arrays.asList(values));
//
//        for( long l : values ) {
//            tree.insert(Sized.valueOf(l),l);
//        }
//
//        for(long i = 0; i < 1000; ++i) {
////          System.out.println(i);
//          long r = tree.search(Sized.valueOf(i));
//
//          if (r != i) {
//              System.out.println(i);
//              throw new RuntimeException("errore nella ricerca!");
//          }
//      }
////
////        for(long i = 0; i < 100; ++i) {
//////            System.out.println(i);
////            tree.insert(Sized.valueOf(i),i);
////        }
////
////
////        for(long i = 0; i < 100; ++i) {
//////            System.out.println(i);
////            long r = tree.search(Sized.valueOf(i));
////
////            if (r != i) {
////                System.out.println(i);
////                throw new RuntimeException("errore nella ricerca!");
////            }
////        }
//
//        System.out.println(tree);
//
//
//
//    }

    public static void main(String[] args) {
        BLink2<Sized<Long>> tree = new BLink2<>(3,5);

        int threads = 18;
//        int threads = 1;
        long maxID = 100;
        long minID = 1;
        ExecutorService ex = Executors.newFixedThreadPool(threads);

        CyclicBarrier barrier = new CyclicBarrier(threads);

        AtomicLong gen = new AtomicLong(minID);

        for (int i = 0; i < threads; ++i) {
            ex.submit(() -> {

                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }

                while(true) {

                    long id = gen.getAndIncrement();

                    if (id > maxID)
                         break;

                    tree.insert(Sized.valueOf(id), id);
                }
            } );
        }

        ex.shutdown();

        try {
            ex.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        tree.booh(tree.root, 0);

        for(long i = minID; i < maxID; ++i) {
//          System.out.println(i);
          long r = tree.search(Sized.valueOf(i));

          if (i % 100 == 0)
              System.out.println(i);

          if (r != i) {
              System.out.println(i);


              r = tree.search(Sized.valueOf(i));
              if (r != i) {
                  System.out.println(i);
                  throw new RuntimeException("errore nella ricerca! 2 " + i);
              }

              throw new RuntimeException("errore nella ricerca! 1 " + i);
          }
      }

      System.out.println(tree);





    }

    private static void insert(long element, BLink2<Sized<Long>> tree) {
        final Sized<Long> sized = Sized.valueOf(element);
        tree.insert(sized,element);
        System.out.println("insert " + element + ":" + tree);
    }

    public static final long NO_RESULT = -1;
    public static final long NO_PAGE = -1;


    public BLink2(long nodeSize, long leafSize) {

        this.nodeSize = nodeSize;
        this.leafSize = leafSize;

        this.root = NO_PAGE;
    }

    private final long nodeSize;
    private final long leafSize;


    volatile long root;

    public BLinkLeaf<K> scannode(K v) {

        /* Get ptr to root node */
        BlinkPtr current = BlinkPtr.page(root);

        /* Read node into memory */
        BLinkNode<K> a = get(current);

        /* Missing root, no data in current index */
        if (a == null) {
            return null;
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
        BlinkPtr t;
        while( ((t = a.scanNode(v)).isLink()) ) {
            current = t;
            /* Get node */
            a = get(t);
        }

        /* Now we have the leaf node in which u should exist. */

        return (BLinkLeaf<K>) a;

    }

    public long search(K v) {

        /* Get ptr to root node */
        BlinkPtr current = BlinkPtr.page(root);

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
        BlinkPtr t;
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



    public long insert(K v, long z) {

        /* For remembering ancestors */
        Deque<BlinkPtr> stack = new LinkedList<>();

        /* Get ptr to root node */
        BlinkPtr current = BlinkPtr.page(root);



        BLinkNode<K> a = get(current);

        /* Missing root, no data in current index */
        if (a == null) {

            synchronized (this) {
                /* Root initialization */

                /* Check if already created */
                if ( root == NO_PAGE ) {

                    final long root = createNewPage();

                    BLinkNode<K> node = new BLinkLeaf<>(root, leafSize, v, z);

                    put(node, BlinkPtr.page(root));

                    this.root = root;

                    return NO_RESULT;
                }

            }

            /* Get ptr to root node (now should exist!) */
            current = BlinkPtr.page(root);

            a = get(current);
        }

        BlinkPtr oldRoot = current;

        /* Scan down tree */
        BlinkPtr t;
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

            @SuppressWarnings("unchecked")
            BLinkLeafPtr<K> ptr = (BLinkLeafPtr<K>) t;
            // LOTHRUIN.... SPORCARE LE PAGINE E VERIFICARE SE L'UPDATE È NECESSARIO!!
            /* stop */
            return ptr.update(z);
        }

        /* w <- pointer to pages allocated for record associated with v; */
        long w = z;

        /* Do insertion */

        while(!a.isSafe()) {

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
            put(bprime,BlinkPtr.page(u));

            /* Instantaneous change of 2 nodes */
            put(aprime,current);

            /* Now insert pointer in parent */
            BlinkPtr oldnode = current;

            v = y;
            w = u;

            /* Backtrack */
            current = stack.poll();

//            if( stack.is_empty() ) {  // at the root - must increase the depth of the tree
//                write_lock_acquire();
//                A.write_lock_release();
//                A.get_min_key( addme );
//
//                NodeBLink r = the_cache.createNode( policy, addme ); // make a new btree node
//                r.insert( NodeBLink.INVALID_ID, addme, current_id );
//                B.get_min_key( addme );
//                r.insert( current_id, addme, B.get_id() );
//                r.insertLink( addme.get_max_cell(), NodeBLink.ALWAYS_MAX_ID ); // the high node
//
//                root_id = r.get_id();
//                r.write_lock_release();
//
//
//                write_lock_release();
//                return root_id;
//              }

            if (current == null) {

                synchronized (this) {

                    if (oldRoot.value == root) {

                        /* We are exiting from root! */
                        long r = createNewPage();

                        System.out.println("ROOT CREATION page " + r + " A " + aprime.getPage() + " B " + bprime.getPage() + " cr " + root );

                        System.out.println("ROOT CREATION A " + aprime );
                        System.out.println("ROOT CREATION B " + bprime );

                        BLinkNode<K> newRoot = new BLinkInner<>(r, nodeSize, v, aprime.getPage(), bprime.getPage());

                        put(newRoot, BlinkPtr.page(r));
                        root = r;

                        /* Success-done backtracking */
                        unlock(oldnode);

                        return NO_RESULT;

                    } else {

                        System.out.println("ROOT CHANGE page A " + aprime.getPage() + " B " + bprime.getPage() );
                        System.out.println("ROOT CHANGE page or " + oldRoot.value  + " cr " + root );

                        /* La root è cambiaaataaa!!!! */
                        unlock(oldnode);
                        throw new RuntimeException("rooot cambiataaaa che fo'?");

                    }

                }

            }

            /* Well ordered */
            lock(current);

            a = get(current);

            /* If necessary */
            moveRight = moveRight(a, v, current);

            t = moveRight.t;
            current = moveRight.current;
            a = moveRight.a;

            unlock(oldnode);

            /* And repeat procedure for parent */

        }

        /* Original "if is safe = true" branch */
        /* Exact manner depends if current is a leaf */

        a = a.insert(v,w);
        put(a,current);

        /* Success-done backtracking */
        unlock(current);


        return NO_RESULT;

//        if (a.isSafe()) {
//            /* Exact manner depends if current is a leaf */
//            a = nodeInsert(a,w,v);
//            put(a,current);
//
//            /* Success-done backtracking */
//            unlock(current);
//        } else {
//            /* Must split node */
//
//            long u = "allocate 1 new page for B";
//
//
//            /*
//             * A, B <- rearrange old A, adding v and w, to make 2 nodes,
//             * where (link ptr of A, link ptr of B) <- (u, link ptr of old A);
//             */
//            BLinkNode<K,V> b;
//
//            /* For insertion into parent */
//            K y = "max value stored in new A";
//
//            /* Insert B before A */
//            put(b,u);
//
//            /* Instantaneous change of 2 nodes */
//            put(a,current);
//
//            /* Now insert pointer in parent */
//            BlinkPtr oldnode = current;
//
//            v = y;
//            w = u;
//
//            /* Backtrack */
//            current = stack.pop();
//
//            /* Well ordered */
//            lock(current);
//
//            a = get(current);
//
//            /* If necessary */
//            moveRight(a, v, current);
//
//            t = moveRight.t;
//            current = moveRight.current;
//            a = moveRight.a;
//
//            unlock(oldnode);
//
//            /* And repeat procedure for parent */
//            break ins;
//        }


    }

    private final AtomicLong idGenerator = new AtomicLong();
    private final ConcurrentMap<Long,BLinkNode<K>> nodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long,Lock> locks = new ConcurrentHashMap<>();

    private BLinkNode<K> get(BlinkPtr pointer) {
        return nodes.get(pointer.value);
    }

    private void put(BLinkNode<K> node, BlinkPtr pointer) {
        /* Crea il lock se non creato... questo perché attualmente il lock è creato a parte, meglio
         * sarebbe crearlo sulla pagina (fouri dai dati "scaricabili") e shararlo tra le versioni */
        locks.putIfAbsent(pointer.value, new ReentrantLock());
//        locks.put(pointer.value, new ReentrantLock());
        nodes.put(pointer.value,node);
    }


    private long createNewPage() {
        return idGenerator.incrementAndGet();
    }

    private void lock(BlinkPtr pointer) {
//        System.out.println("Lock " + pointer);
        locks.get(pointer.value).lock();
    }

    private void unlock(BlinkPtr pointer) {
//        System.out.println("Unlock " + pointer);
        locks.get(pointer.value).unlock();
    }

    private MoveRightResult<K> moveRight(BLinkNode<K> a, K key, BlinkPtr current) {

        BlinkPtr t;
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

    private static final class MoveRightResult<K extends Comparable<K>> {
        final BlinkPtr t;
        final BlinkPtr current;
        final BLinkNode<K> a;

        public MoveRightResult(BlinkPtr t, BlinkPtr current, BLinkNode<K> a) {
            super();
            this.t = t;
            this.current = current;
            this.a = a;
        }
    }

    @Override
    public String toString() {

        /* Get ptr to root node */
        BlinkPtr current = BlinkPtr.page(root);

        /* Read node into memory */
        BLinkNode<K> root = get(current);

        return "BLink [" + root + "]";
    }

    public void booh(long page, int indents) {

        BlinkPtr current = BlinkPtr.page(page);

        /* Read node into memory */
        BLinkNode<K> node = get(current);

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < indents; ++i) {
            builder.append("-");
        }
        builder.append(node);
        System.out.println(builder);

        if (!node.isLeaf()) {

            BLinkInner<K> inner = (BLinkInner<K>) node;

            BLinkInner.Element<K> e = inner.root;

            while(e != null) {
                booh(e.page, indents + 1);
                e = e.next;
            }
        }

    }

    public static final <X> int le(X search, X[] array, int size) {
        int idx = Arrays.binarySearch(array, 0, size, search);

        if (idx > -1) {
            /* key found */
            return idx;
        } else {
            return -(idx + 1) - 1 /* less than */;
        }
    }

    public static final <X> int lt(X search, X[] array, int size) {
        int idx = Arrays.binarySearch(array, 0, size, search);

        if (idx > -1) {
            /* key found */
            return idx - 1 /* less than */;
        } else {
            return -(idx + 1) - 1 /* less than */;
        }
    }
}
