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

import java.util.AbstractMap;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import herddb.utils.SystemProperties;

/**
 * Responsible to handle global memory usage.
 *
 * @author diego.salvi
 */
public class MemoryManager {

    private static final Logger LOGGER = Logger.getLogger(MemoryManager.class.getName());

    private static final String PAGE_REPLACEMENT_POLICY = SystemProperties.getStringSystemProperty(
            TableManager.class.getName() + ".pageReplacementPolicy", "cp" ).toLowerCase(Locale.US);

    private final ConcurrentMap<TableManager, LongAdder> borrowedDirtyAmounts = new ConcurrentHashMap<>();

    private final LongAdder borrowedDirtyMemory = new LongAdder();

    private final long maximumDirtyMemory;

    private final long hiDirtyMemoryLimit;
    private final long lowDirtyMemoryLimit;

    private final long maxPagesUsedMemory;
    private final long maxLogicalPageSize;

    private final PageReplacementPolicy pageReplacementPolicy;

    public MemoryManager(long maximumDirtyMemory, float hiDirtyMemoryLimit, float lowDirtyMemoryLimit,
            long maxPagesUsedMemory, long maxLogicalPageSize ) {

        if (maximumDirtyMemory < 1) {
            throw new IllegalArgumentException("Maximum dirty memory cannot be negative or zero");
        }

        if (hiDirtyMemoryLimit > 100) {
            throw new IllegalArgumentException("Dirty memory high limit cannot be greater than 100%");
        }

        if (hiDirtyMemoryLimit > 100) {
            throw new IllegalArgumentException("Dirty memory high limit cannot be greater than 100%");
        }

        if (lowDirtyMemoryLimit > 100) {
            throw new IllegalArgumentException("Dirty memory low limit cannot be less than 0%");
        }

        if (lowDirtyMemoryLimit > hiDirtyMemoryLimit) {
            throw new IllegalArgumentException("Dirty memory low limit cannot be greater than high limit");
        }

        this.maximumDirtyMemory  = maximumDirtyMemory;
        this.hiDirtyMemoryLimit  = (long) (maximumDirtyMemory * hiDirtyMemoryLimit);
        this.lowDirtyMemoryLimit = (long) (maximumDirtyMemory * lowDirtyMemoryLimit);

        this.maxPagesUsedMemory  = maxPagesUsedMemory;
        this.maxLogicalPageSize  = maxLogicalPageSize;


        final int pages = (int) (maxPagesUsedMemory / maxLogicalPageSize);
        LOGGER.log(Level.INFO, "Maximum number of loaded pages {0}", pages);
        switch(PAGE_REPLACEMENT_POLICY) {
            case "random":
                pageReplacementPolicy = new RandomPageReplacementPolicy(pages);
                break;

            case "cp":
                pageReplacementPolicy = new ClockPro(pages);
                break;

            case "car":
            default:
                pageReplacementPolicy = new ClockAdaptiveReplacement(pages);
        }

    }

    public long getMaximumDirtyMemory() {
        return maximumDirtyMemory;
    }

    public long getHiDirtyMemoryLimit() {
        return hiDirtyMemoryLimit;
    }

    public long getLowDirtyMemoryLimit() {
        return lowDirtyMemoryLimit;
    }

    public long getMaxPagesUsedMemory() {
        return maxPagesUsedMemory;
    }

    public long getMaxLogicalPageSize() {
        return maxLogicalPageSize;
    }

    public PageReplacementPolicy getPageReplacementPolicy() {
        return pageReplacementPolicy;
    }

    public long borrowDirtyMemory(long amount, TableManager manager) {

        final LongAdder borrowed = borrowedDirtyAmounts.get(manager);

        borrowed.add(amount);
        borrowedDirtyMemory.add(amount);

        return amount;
    }

    public long releaseDirtyMemory(long amount, TableManager manager) {

        final LongAdder borrowed = borrowedDirtyAmounts.get(manager);

        borrowed.add(-amount);
        borrowedDirtyMemory.add(-amount);

        return amount;
    }

    public void registerTableManager(TableManager manager) {
        borrowedDirtyAmounts.computeIfAbsent(manager, (m) -> new LongAdder());
    }

    public void deregisterTableManager(TableManager manager) {
        borrowedDirtyAmounts.remove(manager);
    }

    public void check() {

        long borrowedDirty = borrowedDirtyMemory.sum();

        if (borrowedDirty >= hiDirtyMemoryLimit) {

            long toReclaim = borrowedDirty - lowDirtyMemoryLimit;

            LOGGER.log(Level.SEVERE, "Low-Memory {0} used ({1} high limit). To reclaim: {3}",
                    new Object[]{borrowedDirty, maximumDirtyMemory, toReclaim});


            final List<Entry<TableManager,Long>> snapshotDirtyAmounts = borrowedDirtyAmounts.entrySet()
                .stream()
                .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(),e.getValue().sum()))
                .sorted((a,b) -> a.getValue().compareTo(b.getValue()))
                .collect(Collectors.toList());

            for(Entry<TableManager,Long> snapshotDirtyAmount : snapshotDirtyAmounts) {

                final TableManager manager = snapshotDirtyAmount.getKey();
                final Long amount = snapshotDirtyAmount.getValue();

                LOGGER.log(Level.FINE, "Reclaiming {0} memory from {1}.{2})",
                        new Object[]{amount, manager.getTable().tablespace, manager.getTable().name});

                manager.tryReleaseMemory(amount);

                if ((borrowedDirty -= amount) <= lowDirtyMemoryLimit) {
                    break;
                }
            }
        }
    }
}
