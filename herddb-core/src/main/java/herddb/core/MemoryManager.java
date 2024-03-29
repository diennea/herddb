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

import herddb.utils.SystemProperties;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Responsible to handle global memory usage.
 *
 * @author diego.salvi
 */
public class MemoryManager {

    private static final Logger LOGGER = Logger.getLogger(MemoryManager.class.getName());

    private static final String PAGE_REPLACEMENT_POLICY = SystemProperties.getStringSystemProperty(
            MemoryManager.class.getName() + ".pageReplacementPolicy", "cp").toLowerCase(Locale.US);

    private final long maxDataUsedMemory;
    private final long maxIndexUsedMemory;
    private final long maxPKUsedMemory;
    private final long maxLogicalPageSize;

    private final PageReplacementPolicy dataPageReplacementPolicy;
    private final PageReplacementPolicy indexPageReplacementPolicy;
    private final PageReplacementPolicy pkPageReplacementPolicy;

    public MemoryManager(long maxDataUsedMemory, long maxIndexUsedMemory, long maxPKUsedMemory, long maxLogicalPageSize) {

        this.maxDataUsedMemory = maxDataUsedMemory;
        this.maxIndexUsedMemory = maxIndexUsedMemory;
        this.maxPKUsedMemory = maxPKUsedMemory;
        this.maxLogicalPageSize = maxLogicalPageSize;

        if (maxDataUsedMemory < maxLogicalPageSize) {
            throw new IllegalArgumentException("Max memory for data pages (" + maxDataUsedMemory
                    + ") must be greater or equal than page size (" + maxLogicalPageSize + ")");
        }

        // Max index memory 0 is acceptable, will use data memory instead
        if (maxIndexUsedMemory > 0 && maxIndexUsedMemory < maxLogicalPageSize) {
            throw new IllegalArgumentException("Max memory for index pages (" + maxIndexUsedMemory
                    + ") must be greater or equal than page size (" + maxLogicalPageSize + ")");
        }

        if (maxPKUsedMemory < maxLogicalPageSize) {
            throw new IllegalArgumentException("Max memory for primary key index pages (" + maxPKUsedMemory
                    + ") must be greater or equal than page size (" + maxLogicalPageSize + ")");
        }

        final int dataPages = (int) (maxDataUsedMemory / maxLogicalPageSize);
        final int indexPages = (int) (maxIndexUsedMemory / maxLogicalPageSize);
        final int pkPages = (int) (maxPKUsedMemory / maxLogicalPageSize);

        LOGGER.log(Level.INFO, "Maximum amount of memory for primary key indexes {0} ({1} pages)",
                new Object[]{(maxPKUsedMemory / (1024 * 1024)) + " MB", pkPages});

        if (indexPages > 0) {
            LOGGER.log(Level.INFO, "Maximum amount of memory for data {0} ({1} pages)",
                    new Object[]{(maxDataUsedMemory / (1024 * 1024)) + " MB", dataPages});
            LOGGER.log(Level.INFO, "Maximum amount of memory for indexes {0} ({1} pages)",
                    new Object[]{(maxIndexUsedMemory / (1024 * 1024)) + " MB", indexPages});
        } else {
            LOGGER.log(Level.INFO, "Maximum amount of memory for data and indexes {0} ({1} pages)",
                    new Object[]{(maxDataUsedMemory / (1024 * 1024)) + " MB", dataPages});
        }

        switch (PAGE_REPLACEMENT_POLICY) {
            case "random":
                dataPageReplacementPolicy = new RandomPageReplacementPolicy(dataPages);
                indexPageReplacementPolicy = indexPages > 0 ? new RandomPageReplacementPolicy(dataPages) : dataPageReplacementPolicy;
                pkPageReplacementPolicy = new RandomPageReplacementPolicy(pkPages);
                break;

            case "cp":
                dataPageReplacementPolicy = new ClockProPolicy(dataPages);
                indexPageReplacementPolicy = indexPages > 0 ? new ClockProPolicy(dataPages) : dataPageReplacementPolicy;
                pkPageReplacementPolicy = new ClockProPolicy(pkPages);
                break;

            case "car":
            default:
                dataPageReplacementPolicy = new ClockAdaptiveReplacement(dataPages);
                indexPageReplacementPolicy = indexPages > 0 ? new ClockAdaptiveReplacement(dataPages) : dataPageReplacementPolicy;
                pkPageReplacementPolicy = new ClockAdaptiveReplacement(pkPages);
        }

    }

    public long getMaxDataUsedMemory() {
        return maxDataUsedMemory;
    }

    public long getMaxIndexUsedMemory() {
        return maxIndexUsedMemory;
    }

    public long getMaxPKUsedMemory() {
        return maxPKUsedMemory;
    }

    public long getMaxLogicalPageSize() {
        return maxLogicalPageSize;
    }

    public PageReplacementPolicy getDataPageReplacementPolicy() {
        return dataPageReplacementPolicy;
    }

    public PageReplacementPolicy getIndexPageReplacementPolicy() {
        return indexPageReplacementPolicy;
    }

    public PageReplacementPolicy getPKPageReplacementPolicy() {
        return pkPageReplacementPolicy;
    }

}
