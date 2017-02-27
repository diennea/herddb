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
package herddb.jmx;

/**
 * Runtime Statistics for a TableSpaceManager
 *
 * @author enrico.olivelli
 */
public interface TableSpaceManagerStatsMXBean {

    public int getLoadedpages();

    public long getLoadedPagesCount();

    public long getUnloadedPagesCount();

    public long getTablesize();

    public int getDirtypages();

    public int getDirtyrecords();

    public long getDirtyUsedMemory();

    public long getMaxLogicalPageSize();

    public long getBuffersUsedMemory();

    public long getKeysUsedMemory();

}
