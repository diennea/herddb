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

package herddb.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.DBManager;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Context for each statement evaluation. Statements are immutable and cachable
 * objects, and cannot retain state
 *
 * @author enrico.olivelli
 */
public class StatementEvaluationContext {

    private static final Logger LOGGER = Logger.getLogger(StatementEvaluationContext.class.getName());

    private DBManager manager;
    private TransactionContext transactionContext;
    private String defaultTablespace = TableSpace.DEFAULT;
    private volatile long tableSpaceLock;
    private static final ZoneId timezone = ZoneId.systemDefault();
    // REPEATABLE READ
    private final boolean forceRetainReadLock;
    // SELECT ... FOR UPDATE
    private final boolean forceAcquireWriteLock;

    // CHECKSTYLE.OFF: MethodName
    public static StatementEvaluationContext DEFAULT_EVALUATION_CONTEXT() {
        return new StatementEvaluationContext(false, false);
    }
    // CHECKSTYLE.ON: MethodName

    protected StatementEvaluationContext(boolean forceAcquireWriteLock,  boolean forceRetainReadLock) {
        this.forceAcquireWriteLock = forceAcquireWriteLock;
        this.forceRetainReadLock = forceRetainReadLock;
    }

    public String getDefaultTablespace() {
        return defaultTablespace;
    }

    public void setDefaultTablespace(String defaultTablespace) {
        this.defaultTablespace = defaultTablespace;
    }

    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public void setTransactionContext(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public DBManager getManager() {
        return manager;
    }

    public void setManager(DBManager manager) {
        this.manager = manager;
    }

    public List<Object> getJdbcParameters() {
        return Collections.emptyList();
    }

    public Object getJdbcParameter(int index) throws StatementExecutionException {
        try {
            return getJdbcParameters().get(index);
        } catch (IndexOutOfBoundsException err) {
            throw new MissingJDBCParameterException(index + 1);
        }
    }

    private java.sql.Timestamp currentTimestamp;

    @SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
    public java.sql.Timestamp getCurrentTimestamp() {
        if (currentTimestamp == null) {
            currentTimestamp = new java.sql.Timestamp(System.currentTimeMillis());
        }
        return currentTimestamp;
    }

    public long getTableSpaceLock() {
        return tableSpaceLock;
    }

    public void setTableSpaceLock(long tableSpaceLock) {
        this.tableSpaceLock = tableSpaceLock;
    }

    public ZoneId getTimezone() {
        return timezone;
    }

    public boolean isForceAcquireWriteLock() {
        return forceAcquireWriteLock;
    }

    public boolean isForceRetainReadLock() {
        return forceRetainReadLock;
    }

}
