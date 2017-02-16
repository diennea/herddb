package herddb.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RandomPageReplacementPolicy implements PageReplacementPolicy {

    private final DataPage[] pages;
    private final Map<DataPage,Integer> positions;
    private final Random random = new Random();

    /** Lock di modifica */
    private final Lock lock = new ReentrantLock();

    public RandomPageReplacementPolicy (int size) {
        pages = new DataPage[size];

        positions = new HashMap<>(size);
    }


    @Override
    public DataPage add(DataPage page) {
        lock.lock();
        try {
            int count = positions.size();
            if (count < pages.length) {
                pages[count] = page;
                positions.put(page,count);

                return null;
            } else {
                int position = random.nextInt(count);

                DataPage old = pages[position];
                positions.remove(old);

                pages[position] = page;
                positions.put(page, position);

                return old;
            }
        } finally {
            lock.unlock();
        }
    }


    public DataPage pop() {
        lock.lock();
        try {
            int count = positions.size();
            int position = random.nextInt(count);

            DataPage old = pages[position];
            positions.remove(old);

            if (count > 0) {
                DataPage moving = pages[count -1];
                pages[position] = moving;
                positions.put(moving, position);
            }

            return old;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        return positions.size();
    }

    @Override
    public int capacity() {
        return pages.length;
    }


    @Override
    public void remove(Collection<DataPage> pages) {
        for(DataPage page : pages) {
            remove(page);
        }
    }



    @Override
    public boolean remove(DataPage page) {
        lock.lock();
        try {
            Integer position = positions.get(page);

            if (position == null) return false;

            int count = positions.size();
            if (count > 0) {
                DataPage moving = pages[count -1];
                pages[position] = moving;
                positions.put(moving, position);
            }

            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            positions.clear();

            for(int i = 0; i < pages.length; ++i) {
                pages[i] = null;
            }
        } finally {
            lock.unlock();
        }
    }
}
