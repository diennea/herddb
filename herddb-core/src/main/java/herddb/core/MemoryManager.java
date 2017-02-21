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
        TableManager.class.getName() + ".pageReplacementPolicy", "car").toLowerCase(Locale.US);

    private final long maxPagesUsedMemory;
    private final long maxLogicalPageSize;

    private final PageReplacementPolicy pageReplacementPolicy;

    public MemoryManager(long maxPagesUsedMemory, long maxLogicalPageSize) {

        this.maxPagesUsedMemory = maxPagesUsedMemory;
        this.maxLogicalPageSize = maxLogicalPageSize;

        final int pages = (int) (maxPagesUsedMemory / maxLogicalPageSize);
        LOGGER.log(Level.INFO, "Maximum number of loaded pages {0}", pages);
        switch (PAGE_REPLACEMENT_POLICY) {
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

    public long getMaxPagesUsedMemory() {
        return maxPagesUsedMemory;
    }

    public long getMaxLogicalPageSize() {
        return maxLogicalPageSize;
    }

    public PageReplacementPolicy getPageReplacementPolicy() {
        return pageReplacementPolicy;
    }

}
