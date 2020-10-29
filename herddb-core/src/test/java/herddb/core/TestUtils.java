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

import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility
 *
 * @author enrico.olivelli
 */
public class TestUtils {

    public static ServerConfiguration newServerConfigurationWithAutoPort() {
        return new ServerConfiguration()
                .set(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_AUTODISCOVERY); // automatica ephemeral port
    }
    public static ServerConfiguration newServerConfigurationWithAutoPort(Path baseDir) {
        return new ServerConfiguration(baseDir)
                .set(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_AUTODISCOVERY); // automatica ephemeral port
    }

    public static DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
    }

    public static DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, -1);
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

    public static StatementExecutionResult execute(DBManager manager, String tableSpace, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(tableSpace, query, parameters, true, true, false, -1);
        return manager.executePlan(translated.plan, translated.context, transactionContext);
    }

    public static StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        return execute(manager, TableSpace.DEFAULT, query, parameters, transactionContext);
    }

    public static DataScanner scan(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner;
    }

    public static DataScanner scan(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, transactionContext)).dataScanner;
    }

    private static final Logger LOG = Logger.getLogger(TestUtils.class.getName());
    public static final Consumer<DataAccessor> DEBUG = (da) -> {
        StringBuilder msg = new StringBuilder("Record: " + Arrays.toString(da.getFieldNames()) + " ");
        for (int i = 0; i < da.getNumFields(); i++) {
            if (i > 0) {
                msg.append(',');
            }
            msg.append(da.getFieldNames()[i]);
            msg.append('=');
            msg.append(da.get(i));
        }
        msg.append(" - " + da.getClass().getName());
        LOG.info(msg.toString());
    };

    public static void dump(DBManager manager, String query, List<Object> parameters,
                                                                  TransactionContext transactionContext) throws StatementExecutionException, DataScannerException {
        dump(manager, query, parameters, transactionContext, DEBUG);
    }

    public static void dump(DBManager manager, String query, List<Object> parameters,
                                                                  TransactionContext transactionContext,
                                                                  Consumer<DataAccessor> sink) throws StatementExecutionException, DataScannerException {
        LOG.log(Level.INFO, "Dump {0} parameters: {1}", new Object[]{query, parameters});
        if (parameters == null) {
            parameters = Collections.emptyList();
        }
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        try (DataScanner scanner = ((ScanResult) manager.executePlan(translated.plan, translated.context, transactionContext)).dataScanner) {
            while (scanner.hasNext()) {
                sink.accept(scanner.next());
            }
        }
    }

    public static DataScanner scanKeepReadLocks(DBManager manager, String query, List<Object> parameters, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, -1);
        translated.context.setForceRetainReadLock(true);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, transactionContext)).dataScanner;
    }

    public static DataScanner scan(DBManager manager, String query, List<Object> parameters, int maxRows, TransactionContext transactionContext) throws StatementExecutionException {
        TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, query, parameters, true, true, false, maxRows);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, transactionContext)).dataScanner;
    }
}
