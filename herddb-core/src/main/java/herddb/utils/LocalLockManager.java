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
package herddb.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle locks by key
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class LocalLockManager {

    private static final Logger LOGGER = Logger.getLogger(LocalLockManager.class.getName());

    private StampedLock makeLock() {
        return new StampedLock();
    }

    private final ConcurrentMap<Bytes, LockInstance> locks = new ConcurrentHashMap<Bytes, LockInstance>();

    @SuppressWarnings("serial")
    private static final class LockInstance extends ReentrantLock {

        private final StampedLock lock;
        private int count;

        public LockInstance(StampedLock lock, int count) {
            super();
            this.lock = lock;
            this.count = count;
        }
    }

    private StampedLock makeLockForKey(Bytes key) {

        LockInstance instance = locks.computeIfAbsent(key, (k) -> {

            /* No existing instance, inserting an already locked instance */
            final LockInstance li = new LockInstance(makeLock(), 1);
            li.lock();

            return li;

        });

        try {
            /* If held by current thread all work has been already done! */
            if (!instance.isHeldByCurrentThread()) {
                instance.lock();

                /*
                 * The lock wasn't created by this thread. We should check if it was released from another thread
                 * between instance retrieval from map and instance lock.
                 */
                if (instance.count < 1) {
                    /* Worst concurrent case: released by another thread, retry */

 /*
                     * Do not release current lock before doing another attemp. Other threads checking the
                     * same instance will have to wait here untill a live lock is created (trying to avoid
                     * spinning and contention between threads). The lock will released in finally block upon
                     * method exit.
                     */
                    return makeLockForKey(key);
                }

                ++instance.count;
            }
        } finally {
            instance.unlock();
        }

        return instance.lock;
    }

    private StampedLock returnLockForKey(Bytes key) throws IllegalStateException {

        /* Retrieve the instance... other threads could have this pointer too */
        LockInstance instance = locks.get(key);

        /* If there was no instance fail */
        if (instance == null) {
            LOGGER.log(Level.SEVERE, "no lock object exists for key {0}", key);
            throw new IllegalStateException("no lock object exists for key " + key);
        }

        instance.lock();

        try {

            if (--instance.count < 1) {

                /*
                 * If was already released too much times fail (multiple concurrent releases, if they weren't
                 * really concurrent the map would have returned a null instance)
                 */
                if (instance.count < 0) {
                    LOGGER.log(Level.SEVERE, "too much lock releases for key {0}", key);
                    throw new IllegalStateException("too much lock releases for key " + key);
                } else {
                    boolean ok = locks.remove(key, instance);
                    if (!ok) {
                        throw new IllegalStateException("illegal lock releases for key " + key);
                    }
                }

            }
        } finally {
            instance.unlock();
        }

        return instance.lock;
    }

    public LockHandle acquireWriteLockForKey(Bytes key) {
        StampedLock lock = makeLockForKey(key);
        return new LockHandle(lock.writeLock(), key, true);
    }

    public void releaseWriteLockForKey(Bytes key, LockHandle lockStamp) {
        StampedLock lock = returnLockForKey(key);
        lock.unlockWrite(lockStamp.stamp);
    }

    public LockHandle acquireReadLockForKey(Bytes key) {
        StampedLock lock = makeLockForKey(key);
        return new LockHandle(lock.readLock(), key, false);
    }

    public void releaseReadLockForKey(Bytes key, LockHandle lockStamp) {
        StampedLock lock = returnLockForKey(key);
        lock.unlockRead(lockStamp.stamp);
    }

    public void releaseLock(LockHandle l) {
        if (l.write) {
            releaseWriteLockForKey(l.key, l);
        } else {
            releaseReadLockForKey(l.key, l);
        }
    }

    public void clear() {
        this.locks.clear();
    }

}
