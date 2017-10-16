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

import herddb.core.DBManager;
import herddb.utils.DataAccessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Context for each statement evaluation. Statements are immutable and cachable objects, and cannot retain state
 *
 * @author enrico.olivelli
 */
public class StatementEvaluationContext {

    private static final Logger LOGGER = Logger.getLogger(StatementEvaluationContext.class.getName());

    private DBManager manager;
    private TransactionContext transactionContext;
    private final Map<PlainSelect, List<DataAccessor>> subqueryCache = new IdentityHashMap<>();
    private final Map<PlainSelect, ExecutionPlan> planCache = new IdentityHashMap<>();
    private String defaultTablespace = TableSpace.DEFAULT;

    public static StatementEvaluationContext DEFAULT_EVALUATION_CONTEXT() {
        return new StatementEvaluationContext();
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

    public List<DataAccessor> executeSubquery(PlainSelect select) throws StatementExecutionException {
        List<DataAccessor> cached = subqueryCache.get(select);
        if (cached != null) {
            return cached;
        }
        LOGGER.log(Level.SEVERE, "executing subquery {0}", select);
        ExecutionPlan plan = compileSubplan(select);
        try (ScanResult result = (ScanResult) manager.executePlan(plan, this, transactionContext);) {
            List<DataAccessor> fullResult = result.dataScanner.consume();
            LOGGER.log(Level.SEVERE, "executing subquery " + select + " -> " + fullResult);
            subqueryCache.put(select, fullResult);
            return fullResult;
        } catch (DataScannerException error) {
            throw new StatementExecutionException(error);
        }
    }

    public ExecutionPlan compileSubplan(PlainSelect select) {
        ExecutionPlan plan = planCache.get(select);
        if (plan != null) {
            return plan;
        }

        Select fullSelect = new Select();
        fullSelect.setSelectBody(select);
        plan = manager.getPlanner().plan(defaultTablespace,
            fullSelect, true, false, -1);
        planCache.put(select, plan);
        return plan;
    }

}
