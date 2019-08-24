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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Statistics about current activity
 *
 * @author enrico.olivelli
 */
public class RunningStatementsStats {

    private final ConcurrentHashMap<Long, RunningStatementInfo> runningStatements = new ConcurrentHashMap<>();

    private final StatsLogger mainStatsLogger;

    RunningStatementsStats(StatsLogger mainStatsLogger) {
        this.mainStatsLogger = mainStatsLogger;
        this.mainStatsLogger.registerGauge("runningstatements", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return runningStatements.size();
            }

        });
    }

    public void registerRunningStatement(RunningStatementInfo info) {
        runningStatements.put(info.getId(), info);
    }

    public void unregisterRunningStatement(RunningStatementInfo info) {
        runningStatements.remove(info.getId());
    }

    public ConcurrentHashMap<Long, RunningStatementInfo> getRunningStatements() {
        return runningStatements;
    }
}
