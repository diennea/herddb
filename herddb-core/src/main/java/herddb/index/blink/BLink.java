package herddb.index.blink;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import herddb.index.blink.BLinkLeaf.BLinkLeafPtr;
import herddb.index.bp.mine.Sized;

public class BLink<K extends Comparable<K>> {


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

//    public static void main(String[] args) {
//
//        ByteArrayOutputStream os = new ByteArrayOutputStream();
//        PrintStream ps = new PrintStream(new ByteArrayOutputStream());
//
//
//
//        System.setOut(out);
//
//        BLink<Sized<Long>> tree = new BLink<>(3,3);
//
//        int threads = 25;
////        int threads = 1;
//        long maxID = 1000;
//        long minID = 1;
//        ExecutorService ex = Executors.newFixedThreadPool(threads);
//
//        CyclicBarrier barrier = new CyclicBarrier(threads);
//
//        AtomicLong gen = new AtomicLong(minID);
//
//        for (int i = 0; i < threads; ++i) {
//            ex.submit(() -> {
//
//                try {
//                    barrier.await();
//                } catch (InterruptedException | BrokenBarrierException e) {
//                    e.printStackTrace();
//                }
//
//                while(true) {
//
//                    long id = gen.getAndIncrement();
//
//                    if (id > maxID)
//                         break;
//
//                    tree.insert(Sized.valueOf(id), id);
//                }
//            } );
//        }
//
//        ex.shutdown();
//
//        try {
//            ex.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//
//        tree.booh(tree.root, 0);
//
//        for(long i = minID; i < maxID; ++i) {
////          System.out.println(i);
//          long r = tree.search(Sized.valueOf(i));
//
//          if (i % 100 == 0)
//              System.out.println(i);
//
//          if (r != i) {
//              System.out.println(i);
//
//
//              r = tree.search(Sized.valueOf(i));
//              if (r != i) {
//                  System.out.println(i);
//                  throw new RuntimeException("errore nella ricerca! 2 " + i);
//              }
//
//              throw new RuntimeException("errore nella ricerca! 1 " + i);
//          }
//      }
//
//      System.out.println(tree);
//
//
//
//
//
//    }

    public static void main(String[] args) {

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        PrintStream original = System.out;


        boolean deadlock = false;

        int count = 0;

        try {
            while(!deadlock) {

                os = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(os);
                System.setOut(ps);


                BLink<Sized<Long>> tree = new BLink<>(3,3);

    //            int threads = 5;
    //            long maxID = 8;

    //            int threads = 6;
    //            long maxID = 10;

                int threads = 16;
//                long maxID = 10000;
                long maxID = 20;

                long minID = 1;
                ExecutorService ex = Executors.newFixedThreadPool(threads);

                CyclicBarrier barrier = new CyclicBarrier(threads);

                AtomicLong gen = new AtomicLong(minID);

                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < threads; ++i) {
                    Future<?> f = ex.submit(() -> {

                        try {
                            barrier.await();
                        } catch (InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace(System.out);
                        }

                        while(true) {

                            long id = gen.getAndIncrement();

                            if (id > maxID)
                                 break;

                            tree.insert(Sized.valueOf(id), id);
                        }
                    } );

                    futures.add(f);
                }

                ex.shutdown();

                try {
                    ex.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.out);
                }



                tree.booh(tree.root);


                for(Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace(System.out);
                        deadlock = true;
                    }
                }

                if (!deadlock) {

                    for (long i = minID; i < maxID; ++i) {
                        // System.out.println(i);
                        long r = tree.search(Sized.valueOf(i));

                        if (i % 100 == 0)
                            System.out.println(i);

                        if (r != i) {
                            System.out.println(i);

                            r = tree.search(Sized.valueOf(i));

                            System.err.println("errore nella ricerca! 2 " + i);
                            if (r != i) {
                                System.out.println(i);

                                throw new RuntimeException("errore nella ricerca! 2 " + i);
                            }

                            throw new RuntimeException("errore nella ricerca! 1 " + i);
                        }
                    }
                }

              System.out.println(tree);

              original.println(++count);
              original.flush();

              System.err.println( "OS S " + os.size() );

            }

        } finally {
            System.out.flush();
            System.setOut(original);

            try {
                System.out.println( "OS S " + os.size() );
                String s = os.toString("UTF-8");
                System.out.println( s );
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    private static void insert(long element, BLink<Sized<Long>> tree) {
        final Sized<Long> sized = Sized.valueOf(element);
        tree.insert(sized,element);
        System.out.println("insert " + element + ":" + tree);
    }

    public static final long NO_RESULT = -1;
    public static final long NO_PAGE = -1;


    public BLink(long nodeSize, long leafSize) {

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

                    publish(node, BlinkPtr.page(root));

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

            long result = ptr.update(z);

            unlock(current);

            return result;
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
            BlinkPtr bptr = BlinkPtr.page(u);
            publish(bprime,bptr,true);

            /* Instantaneous change of 2 nodes */
            republish(aprime,current);

            /* Now insert pointer in parent */
            BlinkPtr oldnode = current;

            v = y;
            w = u;

            if (stack.isEmpty()) {

                synchronized (this) {

                    BlinkPtr currentRoot = BlinkPtr.page(root);
                    if (oldRoot.value == currentRoot.value) {

                        /* We are exiting from root! */
                        long r = createNewPage();

                        System.out.println(Thread.currentThread().getId() + " ROOT CREATION page " + r + " cr " + currentRoot + " or " + oldRoot);

                        System.out.println(Thread.currentThread().getId() + " ROOT CREATION A " + aprime );
                        System.out.println(Thread.currentThread().getId() + " ROOT CREATION B " + bprime );

                        BLinkNode<K> newRoot = new BLinkInner<>(r, nodeSize, v, aprime.getPage(), bprime.getPage());

                        System.out.println(Thread.currentThread().getId() + " ROOT CREATION root " + newRoot);

                        publish(newRoot, BlinkPtr.page(r));
                        root = r;

                        /* Success-done backtracking */
                        unlock(oldnode);
                        unlock(bptr);

                        return NO_RESULT;

                    } else {

                        /* La root è cambiaaataaa!!!! */

                        System.out.println(Thread.currentThread().getId() + " ROOT CHANGE cr " + currentRoot + " or " + oldRoot);

                        System.out.println(Thread.currentThread().getId() + " ROOT CHANGE A " + aprime );
                        System.out.println(Thread.currentThread().getId() + " ROOT CHANGE B " + bprime );


                        /* Get ptr to root node */
                        current = BlinkPtr.page(root);

                        /* Save it as the new "old root"*/
                        oldRoot = current;

                        a = get(current);

                        /* Scan down tree */
                        /* è un po' diverso dallo scan normale poiché dobbiamo essenzialmente trovare
                         * il nodo che è diventato il padre della vecchia root per inserire i dati
                         * In linea teorica potrebbe anche interessarci un solo nodo e rifare il giro se
                         * è cambiato di nuovo! */

//                        K searchKey = v;
                        K searchKey = aprime.getLowKey();


                        /* Scan down tree */
                        while(!a.isLeaf()) {
                            t = current;
                            current = a.scanNode(searchKey);

                            if (!current.isLink()) {
                                /* Remember node at that level */
                                stack.push(t);
                            }
                            System.out.println(Thread.currentThread().getId() + " Searching roots: a " + a + " current " + current + " key " + searchKey);

                            if (current.value == aprime.getPage())
                                break;

                            a = get(current);
                        }

                        /* Ora dovrei avere nuovamente il path nello stack */

                        System.out.println(Thread.currentThread().getId() + " stack " + stack );


                    }
                }

            }

            /* Backtrack */
            current = stack.poll();

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

    private void republish(BLinkNode<K> node, BlinkPtr pointer) {
        /* Il lock e una enty di nodo devono già esistere! */
        if (!locks.containsKey(pointer.value)){
            throw new InternalError("Lock expected");
        }

        nodes.put(pointer.value,node);
    }

//    private void put(BLinkNode<K> node, BlinkPtr pointer) {
//        /* Crea il lock se non creato... questo perché attualmente il lock è creato a parte, meglio
//         * sarebbe crearlo sulla pagina (fouri dai dati "scaricabili") e shararlo tra le versioni */
//        locks.putIfAbsent(pointer.value, new ReentrantLock());
////        locks.put(pointer.value, new ReentrantLock());
//        nodes.put(pointer.value,node);
//    }

    private void publish(BLinkNode<K> node, BlinkPtr pointer) {
        publish(node, pointer, false);
    }

    private void publish(BLinkNode<K> node, BlinkPtr pointer, boolean locked) {
        Lock lock = new ReentrantLock();
        if (locked) lock.lock();

        locks.put(pointer.value, lock);
        nodes.put(pointer.value,node);
    }



    private long createNewPage() {
        return idGenerator.incrementAndGet();
    }

    private void lock(BlinkPtr pointer) {

        try {
            if (!locks.get(pointer.value).tryLock(3, TimeUnit.SECONDS)) {
                System.out.println(Thread.currentThread().getId() + " --------------> Deadlock " + pointer);
                throw new InternalError("Deadlock " + pointer);
            }
        } catch (InterruptedException e) {
            throw new InternalError("interrupt " + pointer);
        }


//        locks.get(pointer.value).lock();
        System.out.println(Thread.currentThread().getId() + " Lock " + pointer);
    }

    private void unlock(BlinkPtr pointer) {
        System.out.println(Thread.currentThread().getId() + " Unlock " + pointer);
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

    public void booh(long root) {

        Set<Long> seen = new HashSet<>();
        Deque<Object[]> stack = new LinkedList<>();
        stack.push(new Object[] {0, root});

        while (!stack.isEmpty())  {

            Object[] o = stack.pop();

            int indents = (int) o[0];
            long page = (long) o[1];



            BlinkPtr current = BlinkPtr.page(page);

            /* Read node into memory */
            BLinkNode<K> node = get(current);

            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < indents; ++i) {
                builder.append("-");
            }
            builder.append(node);
            System.out.println(builder);


            if (!seen.add(page)) {
                System.out.println("Page " + page + " already seen");
                return;
            }

            if (!node.isLeaf()) {

                BLinkInner<K> inner = (BLinkInner<K>) node;

                BLinkInner.Element<K> e = inner.root;

                while(e != null) {
                    stack.push(new Object[] {indents + 1, e.page});
                    e = e.next;
                }
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
