/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql;

import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.model.NotLeaderException;
import herddb.model.StatementExecutionException;
import herddb.server.ServerConfiguration;
import herddb.utils.SystemProperties;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractSQLPlanner {

    protected final long waitForSchemaTimeout;
    protected static final Level DUMP_QUERY_LEVEL = Level.parse(SystemProperties.getStringSystemProperty("herddb.planner.dumpqueryloglevel", Level.FINE.toString()));
    private static final Logger LOG = Logger.getLogger(AbstractSQLPlanner.class.getName());

    protected final DBManager manager;

    public AbstractSQLPlanner(DBManager manager) {
        this.manager = manager;
        this.waitForSchemaTimeout = manager.getServerConfiguration()
                .getInt(ServerConfiguration.PROPERTY_PLANNER_WAITFORTABLESPACE_TIMEOUT, ServerConfiguration.PROPERTY_PLANNER_WAITFORTABLESPACE_TIMEOUT_DEFAULT);
    }

    public abstract void clearCache();

    public abstract long getCacheHits();

    public abstract long getCacheMisses();

    public abstract long getCacheSize();

    public abstract TranslatedQuery translate(String defaultTableSpace, String query, List<Object> parameters, boolean scan, boolean allowCache, boolean returnValues, int maxRows) throws StatementExecutionException;

    protected final void ensureDefaultTableSpaceBootedLocally(String defaultTableSpace) {        
        TableSpaceManager tableSpaceManager = getTableSpaceManager(defaultTableSpace);
        if (tableSpaceManager == null) {
            throw new NotLeaderException("tablespace " + defaultTableSpace + " not available here (at server " + manager.getNodeId() + ")");
        }
    }

    protected final TableSpaceManager getTableSpaceManager(String tableSpace) {
        long startTs = System.currentTimeMillis();
        while (true) {
            TableSpaceManager result = manager.getTableSpaceManager(tableSpace);
            if (result != null) {
                return result;
            }
            long delta = System.currentTimeMillis() - startTs;
            LOG.log(Level.FINE, "schema {0} not available yet, after waiting {1}/{2} ms",
                    new Object[]{tableSpace, delta, waitForSchemaTimeout});
            if (delta >= waitForSchemaTimeout) {
                return null;
            }
            clearCache();
            try {
                Thread.sleep(100);
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
