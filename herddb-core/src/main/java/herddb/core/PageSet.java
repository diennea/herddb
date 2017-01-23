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

    /**
     * a structure which holds the set of the pages which are loaded in memory (set<long>)
     */
    private final Set<Long> loadedPages = new HashSet<>();

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

    Set<Long> selectPagesToUnload(int max) {
        lock.readLock().lock();
        try {
            int count = max;
            Set<Long> pagesToUnload = new HashSet<>();
            List<Long> loadedShuffled = new ArrayList<>(loadedPages);
            Collections.shuffle(loadedShuffled);
            for (Long loadedPage : loadedShuffled) {
                if (!dirtyPages.contains(loadedPage)) {
                    pagesToUnload.add(loadedPage);
                    if (count-- <= 0) {
                        break;
                    }
                }
            }
            if (pagesToUnload.isEmpty()) {
                return Collections.emptySet();
            }
            return pagesToUnload;
        } finally {
            lock.readLock().unlock();
        }
    }

    void unloadPages(Set<Long> pages, Runnable runnable) {
        lock.writeLock().lock();
        try {
            // ensure that dirty pages are not loaded
            pages.removeAll(dirtyPages);
            runnable.run();
            loadedPages.removeAll(pages);
        } finally {
            lock.writeLock().unlock();
        }
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
            loadedPages.clear();
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

    boolean checkPageUnloadable(Long pageId) {
        lock.readLock().lock();
        try {
            if (loadedPages.contains(pageId)) {
                return false;
            }
            if (!activePages.contains(pageId)) {
                LOGGER.log(Level.SEVERE, "page {2} is no more active. it cannot be loaded from disk", new Object[]{pageId});
                return false;
            }
            if (dirtyPages.contains(pageId)) {
                throw new IllegalStateException("page " + pageId + " is marked as dirty, so should has been already loaded from disk, dirtyPages " + dirtyPages + ", active " + activePages);
            }
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    void setPageLoaded(Long pageId) {
        lock.writeLock().lock();
        try {
            loadedPages.add(pageId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "{" + "loadedPages=" + loadedPages + ", dirtyPages=" + dirtyPages + ", activePages=" + activePages + '}';
        } finally {
            lock.readLock().unlock();
        }
    }

    int getLoadedPagesCount() {
        lock.readLock().lock();
        try {
            return loadedPages.size();
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
            loadedPages.add(pageId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void checkpointDone(Set<Long> dirtyPagesFlushed) {
        lock.writeLock().lock();
        try {
            activePages.removeAll(dirtyPagesFlushed);
            dirtyPages.removeAll(dirtyPagesFlushed);
        } finally {
            lock.writeLock().unlock();
        }
    }

}
