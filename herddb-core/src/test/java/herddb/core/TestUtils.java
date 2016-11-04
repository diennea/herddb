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

import java.util.Collections;
import java.util.List;

import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.sql.TranslatedQuery;

/**
 * Utility
 *
 * @author enrico.olivelli
 */
public class TestUtils {

    public static DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
    }

    public static DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context, transactionContext);
    }

    public static long beginTransaction(DBManager manager, String tableSpace) throws StatementExecutionException {
        return ((TransactionResult) execute(manager, "BEGIN TRANSACTION '" + tableSpace + "'", Collections.emptyList(), TransactionContext.NO_TRANSACTION)).getTransactionId();
    }

    public static void commitTransaction(DBManager manager, String tableSpace, long tx) throws StatementExecutionException {
        execute(manager, "COMMIT TRANSACTION '" + tableSpace + "','" + tx + "'", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
    }

    public static StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        return execute(manager, query, parameters, TransactionContext.NO_TRANSACTION);
    }

    public static StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true);
        return manager.executePlan(translated.plan, translated.context, transactionContext);
    }

    public static DataScanner scan(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner;
    }

    public static DataScanner scan(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, transactionContext)).dataScanner;
    }
}
