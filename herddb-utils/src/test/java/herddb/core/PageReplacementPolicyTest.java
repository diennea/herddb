package herddb.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import herddb.core.Page.Metadata;

@RunWith(Parameterized.class)
public class PageReplacementPolicyTest {

    private static final int DEFAULT_CAPACITY = 300;
    private static final int DEFAULT_PAGES = 500;
    private static final int DEFAULT_ITERATIONS = 100000;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 {new ClockProPolicy(DEFAULT_CAPACITY),              DEFAULT_PAGES, DEFAULT_ITERATIONS, false},
                 {new ClockAdaptiveReplacement(DEFAULT_CAPACITY),    DEFAULT_PAGES, DEFAULT_ITERATIONS, false},
                 {new RandomPageReplacementPolicy(DEFAULT_CAPACITY), DEFAULT_PAGES, DEFAULT_ITERATIONS, false}
           });
    }

    private final PageReplacementPolicy policy;
    private final int pages;
    private final int iterations;
    private final boolean stable;

    public PageReplacementPolicyTest(PageReplacementPolicy policy, int pages, int iterations, boolean stable) {
        super();
        this.policy = policy;
        this.pages = pages;
        this.iterations = iterations;
        this.stable = stable;
    }

    @Test
    public void test() {
        test(pages, iterations, policy, stable);
    }

    private void test(int pages, int iterations, PageReplacementPolicy policy, boolean stable) {
//        Assert.assertTrue(policy.capacity() < pages);

        MyOwner owner = new MyOwner(pages, policy, stable ? new Random(0) : new Random(System.nanoTime()));
        for(int i = 0; i < iterations; ++i){
            owner.loadRandom();
        }
    }

    private static final class MyOwner implements Page.Owner {

        Map<Long,MyPage> exPolicy;
        Map<Long,MyPage> inPolicy;
        Map<Long,MyPage> existing;

        PageReplacementPolicy policy;
        Random random;

        int nextId;
        int pages;


        public MyOwner(int pages, PageReplacementPolicy policy, Random random) {
            super();
            this.exPolicy = new HashMap<>();
            this.inPolicy = new HashMap<>();

            this.existing = new HashMap<>();

            this.policy = policy;
            this.random = random;

            nextId = 0;
            this.pages = pages;
        }

        @Override
        public void unload(long pageId) {
            throw new RuntimeException();
        }


        private MyPage getRandom(Map<Long,MyPage> map, Random random) {

            /* molto poco performante ma è solo per test */

            Iterator<MyPage> ter = map.values().iterator();

            int pos = random.nextInt(map.size());
            int count = 0;
            MyPage page = null;
            while(count++ <= pos) {
                page = ter.next();
            }

            if (page == null)
                throw new IllegalStateException();

            return page;

        }

        public void loadRandom() {

            int maxhits = inPolicy.size() / 10;
            if (maxhits > 0) {
                int hits = random.nextInt(maxhits);
                /* Aggiunge qualche page/hit casuale */
                for(int i = 0; i < hits; ++i) {
                    MyPage page = getRandom(inPolicy, random);
                    policy.pageHit(page);
                }
            }

            if (inPolicy.size() < policy.capacity()) {

                if (existing.size() < pages) {

                    MyPage page = new MyPage(this, ++nextId);

                    existing.put(page.pageId, page);

                    Metadata meta = policy.add(page);

                    inPolicy.put(page.pageId, page);

                    if (meta != null) {
                        throw new IllegalStateException();
                    }

                } else {

                    /* Pagine massime minori della capacità */

                    MyPage page = getRandom(inPolicy, random);

                    if (inPolicy.remove(page.pageId) == null)
                        throw new IllegalStateException();

                    if (existing.remove(page.pageId) == null)
                        throw new IllegalStateException();

                    if (!policy.remove(page)) {
                        throw new IllegalStateException();
                    }
                }

            } else {

                if (existing.size() < pages) {

                    MyPage page = new MyPage(this, ++nextId);

                    existing.put(page.pageId, page);

                    Metadata meta = policy.add(page);

                    inPolicy.put(page.pageId, page);

                    if (meta == null) {
                        throw new IllegalStateException();
                    }

                    MyPage unload = inPolicy.remove(meta.pageId);

                    if (unload == null) {
                        throw new IllegalStateException();
                    }

                    exPolicy.put(unload.pageId, unload);

                } else {

                    boolean remove = random.nextBoolean() && random.nextBoolean() && random.nextBoolean();

                    if (remove) {

//                        int maxrem = inPolicy.size() / 5;
                        int maxrem = inPolicy.size();
                        if ( maxrem > 0) {
                            int rems = random.nextInt(maxrem);
                            /* Aggiunge qualche page/hit casuale */
                            for(int i = 0; i < rems; ++i) {
                                MyPage page = getRandom(inPolicy, random);

                                if (inPolicy.remove(page.pageId) == null)
                                    throw new IllegalStateException();

                                if (existing.remove(page.pageId) == null)
                                    throw new IllegalStateException();

                                if (!policy.remove(page)) {
                                    throw new IllegalStateException();
                                }
                            }
                        }

                    } else {

                        MyPage page = getRandom(exPolicy, random);

                        if (exPolicy.remove(page.pageId) == null)
                            throw new IllegalStateException();

                        Metadata meta = policy.add(page);

                        inPolicy.put(page.pageId, page);

                        if (meta == null) {
                            throw new IllegalStateException();
                        }

                        MyPage unload = inPolicy.remove(meta.pageId);

                        if (unload == null) {
                            throw new IllegalStateException();
                        }

                        exPolicy.put(unload.pageId, unload);

                    }

                }

            }
        }

        @Override
        public String toString() {
            return "MyOwner";
        }

    }

    private static final class MyPage extends Page<MyOwner> implements Comparable<MyPage> {
        public MyPage(MyOwner owner, long pageId) {
            super(owner, pageId);
        }

        @Override
        public int compareTo(MyPage o) {
            return Long.compare(pageId, o.pageId);
        }

        @Override
        public String toString() {
            return "MyPage [pageId=" + pageId + "]";
        }
    }

}
