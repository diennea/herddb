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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Informations about a running statement
 *
 * @author enrico.olivelli
 */
public class RunningStatementInfo {

    private static final AtomicLong IDGENERATOR = new AtomicLong();

    private final long id = IDGENERATOR.incrementAndGet();
    private final String query;
    private final String tablespace;
    private final String info;
    private final int numBatches;
    private final long startTimestamp;

    public RunningStatementInfo(String query, long startTimestamp, String tablespace, String info, int numBatches) {
        this.query = query;
        this.startTimestamp = startTimestamp;
        this.tablespace = tablespace;
        this.info = info;
        this.numBatches = numBatches;
    }

    public int getNumBatches() {
        return numBatches;
    }

    public long getId() {
        return id;
    }

    public String getQuery() {
        return query;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public String getTablespace() {
        return tablespace;
    }

    public String getInfo() {
        return info;
    }

}
