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

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.utils.SystemProperties;

/**
 * Responsible to handle global memory usage.
 *
 * @author diego.salvi
 */
public class MemoryManager {

    private static final Logger LOGGER = Logger.getLogger(MemoryManager.class.getName());

    private static final String PAGE_REPLACEMENT_POLICY = SystemProperties.getStringSystemProperty(
        MemoryManager.class.getName() + ".pageReplacementPolicy", "car").toLowerCase(Locale.US);

    private final long maxDataUsedMemory;
    private final long maxIndexUsedMemory;
    private final long maxLogicalPageSize;

    private final PageReplacementPolicy dataPageReplacementPolicy;
    private final PageReplacementPolicy indexPageReplacementPolicy;

    public MemoryManager(long maxDataUsedMemory, long maxIndexUsedMemory, long maxLogicalPageSize) {

        this.maxDataUsedMemory = maxDataUsedMemory;
        this.maxIndexUsedMemory = maxIndexUsedMemory;
        this.maxLogicalPageSize = maxLogicalPageSize;

        if (maxDataUsedMemory < maxLogicalPageSize) {
            throw new IllegalArgumentException("Max memory for data pages (" + maxDataUsedMemory
                + ") must be greater or equal than page size (" + maxLogicalPageSize + ")");
        }

        if (maxIndexUsedMemory < maxLogicalPageSize) {
            throw new IllegalArgumentException("Max memory for index pages (" + maxIndexUsedMemory
                + ") must be greater or equal than page size (" + maxLogicalPageSize + ")");
        }

        final int dataPages = (int) (maxDataUsedMemory / maxLogicalPageSize);
        final int indexPages = (int) (maxIndexUsedMemory / maxLogicalPageSize);

        LOGGER.log(Level.INFO, "Maximum amount of memory for data and indexes {0}", (maxDataUsedMemory / (1024 * 1024)) + " MB");
        LOGGER.log(Level.INFO, "Maximum amount of memory for primary key indexes {0}", (maxIndexUsedMemory / (1024 * 1024)) + " MB");

        LOGGER.log(Level.INFO, "Maximum number of loaded pages for data {0}"
            + ", maximum number of loadedd pages for primary key indexes {1}", new Object[]{dataPages, indexPages});
        switch (PAGE_REPLACEMENT_POLICY) {
            case "random":
                dataPageReplacementPolicy = new RandomPageReplacementPolicy(dataPages);
                indexPageReplacementPolicy = new RandomPageReplacementPolicy(indexPages);
                break;

            case "cp":
                dataPageReplacementPolicy = new ClockProPolicy(dataPages);
                indexPageReplacementPolicy = new ClockProPolicy(indexPages);
                break;

            case "car":
            default:
                dataPageReplacementPolicy = new ClockAdaptiveReplacement(dataPages);
                indexPageReplacementPolicy = new ClockAdaptiveReplacement(indexPages);
        }

    }

    public long getMaxDataUsedMemory() {
        return maxDataUsedMemory;
    }

    public long getMaxIndexUsedMemory() {
        return maxIndexUsedMemory;
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

}
