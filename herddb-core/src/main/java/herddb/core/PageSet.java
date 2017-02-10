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
package herddb.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Status of the set of pages of a Table
 *
 * @author enrico.olivelli
 */
public final class PageSet {

    private static final Logger LOGGER = Logger.getLogger(PageSet.class.getName());

    private final Set<Long> dirtyPages = new HashSet<>();

    private final Set<Long> activePages = new HashSet<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    void setActivePagesAtBoot(Set<Long> activePagesAtBoot) {
        lock.writeLock().lock();
        try {
            this.activePages.clear();
            this.activePages.addAll(activePagesAtBoot);
        } finally {
            lock.writeLock().unlock();
        }
    }

    static Set<Long> selectPagesToUnload(int max, Collection<Long> loadedPages) {
        int count = max;
        Set<Long> pagesToUnload = new HashSet<>();
        List<Long> loadedShuffled = new ArrayList<>(loadedPages);
        loadedShuffled.remove(TableManager.NEW_PAGE);
        Collections.shuffle(loadedShuffled);
        for (Long loadedPage : loadedShuffled) {
            pagesToUnload.add(loadedPage);
            if (--count <= 0) {
                break;
            }
        }
        if (pagesToUnload.isEmpty()) {
            return Collections.emptySet();
        }
        return pagesToUnload;
    }

    Set<Long> getActivePages() {
        lock.readLock().lock();
        try {
            return new HashSet<>(activePages);
        } finally {
            lock.readLock().unlock();
        }
    }

    void truncate() {
        lock.writeLock().lock();
        try {
            dirtyPages.clear();
            activePages.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    void setPageDirty(Long pageId) {
        lock.writeLock().lock();
        try {
            boolean wasNotDirty = dirtyPages.add(pageId);
            if (wasNotDirty) {
                LOGGER.log(Level.SEVERE, "now page " + pageId + " is dirty");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "PageSet{" + "dirtyPages=" + dirtyPages + ", activePages=" + activePages + "}";
        } finally {
            lock.readLock().unlock();
        }
    }

    int getDirtyPagesCount() {
        lock.readLock().lock();
        try {
            return dirtyPages.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    Set<Long> getDirtyPages() {
        lock.readLock().lock();
        try {
            return new HashSet<>(dirtyPages);
        } finally {
            lock.readLock().unlock();
        }
    }

    void pageCreated(long pageId) {
        lock.writeLock().lock();
        try {
            activePages.add(pageId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void checkpointDone(Set<Long> dirtyPagesFlushed) {
        lock.writeLock().lock();
        try {
            activePages.removeAll(dirtyPagesFlushed);
            dirtyPages.removeAll(dirtyPagesFlushed);
            LOGGER.log(Level.SEVERE, "checkpointDone " + dirtyPagesFlushed.size() + ", now activePages:" + activePages.size() + " dirty " + dirtyPages.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

}
