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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

/**
 * Handle locks by key
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class LocalLockManager implements ILocalLockManager {

    private int writeLockTimeout = 60 * 30;

    private int readLockTimeout = 60 * 30;

    private StampedLock makeLock() {
        return new StampedLock();
    }

    private final ConcurrentMap<Bytes, LockInstance> locks = new ConcurrentHashMap<>();

    private static class LockInstance {

        private final StampedLock lock;
        private volatile int count;
        private volatile Thread locker;

        public LockInstance(StampedLock lock, int count) {
            super();
            this.lock = lock;
            this.count = count;
        }

        @Override
        public String toString() {
            return "LockInstance{" + "lock=" + lock + ", count=" + count + '}';
        }

    }

    private LockInstance makeLockForKey(Bytes key) {
        LockInstance instance = locks.compute(key, (k, existing) -> {
            if (existing != null) {
                existing.count++;
                return existing;
            } else {
                LockInstance lock = new LockInstance(makeLock(), 1);
                return lock;
            }
        });
        return instance;
    }

    private void returnLockForKey(LockInstance instance, Bytes key) throws IllegalStateException {
        locks.compute(key, (Bytes t, LockInstance u) -> {
            if (instance != u) {
                throw new IllegalStateException("trying to release un-owned lock");
            }
            if (--u.count == 0) {
                return null;
            } else {
                return u;
            }
        });
    }

    public int getWriteLockTimeout() {
        return writeLockTimeout;
    }

    public void setWriteLockTimeout(int writeLockTimeout) {
        this.writeLockTimeout = writeLockTimeout;
    }

    public int getReadLockTimeout() {
        return readLockTimeout;
    }

    public void setReadLockTimeout(int readLockTimeout) {
        this.readLockTimeout = readLockTimeout;
    }

    @Override
    public LockHandle acquireWriteLockForKey(Bytes key) {
        LockInstance lock = makeLockForKey(key);
        try {
            long tryWriteLock = lock.lock.tryWriteLock(writeLockTimeout, TimeUnit.SECONDS);
            if (tryWriteLock == 0) {                
                StackTraceElement[] stackTrace = lock.locker.getStackTrace();
                StringBuilder st = new StringBuilder();
                for (StackTraceElement s : stackTrace) {
                    st.append(lock.locker.getName()+" "+s+"\n");
                }
                throw new RuntimeException(Thread.currentThread().getName()+" timed out acquiring lock for write on key "+key+", lock state "+lock.lock+" locker "+lock.locker+": \n"+st);
            }
            System.out.println("lock "+key+" "+lock.lock+" "+Thread.currentThread().getName());
            lock.locker = Thread.currentThread();
            return new LockHandle(tryWriteLock, key, true, lock);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(err);
        }
    }

    @Override
    public void releaseWriteLock(LockHandle handle) {
        /* Retrieve the instance... other threads could have this pointer too */
        LockInstance instance = (LockInstance) handle.handle;
        System.out.println("rel "+handle.key+" "+instance.lock+" "+Thread.currentThread().getName()+" locked "+instance.locker.getName());
        instance.locker = null;
        instance.lock.unlockWrite(handle.stamp);
        returnLockForKey(instance, handle.key);
    }

    @Override
    public LockHandle acquireReadLockForKey(Bytes key) {
        LockInstance lock = makeLockForKey(key);
        try {
            long tryReadLock = lock.lock.tryReadLock(readLockTimeout, TimeUnit.SECONDS);
            if (tryReadLock == 0) {
                throw new RuntimeException("timedout trying to read lock");
            }
            return new LockHandle(tryReadLock, key, false, lock);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(err);
        }
    }

    @Override
    public void releaseReadLock(LockHandle handle) {
        LockInstance instance = (LockInstance) handle.handle;
        instance.lock.unlockRead(handle.stamp);
        returnLockForKey(instance, handle.key);
    }

    @Override
    public void releaseLock(LockHandle handle) {
        if (handle == null) {
            return;
        }
        if (handle.write) {
            releaseWriteLock(handle);
        } else {
            releaseReadLock(handle);
        }
    }

    @Override
    public void clear() {
        this.locks.clear();
    }

    @Override
    public int getNumKeys() {
        return locks.size();
    }

    public LockInstance getLockForKey(Bytes key) {
        return locks.get(key);
    }

}
