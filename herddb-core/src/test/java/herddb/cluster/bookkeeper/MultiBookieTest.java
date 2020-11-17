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
package herddb.cluster.bookkeeper;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.cluster.BookkeeperCommitLog;
import herddb.codec.RecordSerializer;
import herddb.index.SecondaryIndexSeek;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Test;

public class MultiBookieTest extends BookkeeperFailuresBase {

    @Test
    public void testFollowWithDownBookie() throws Exception {
        // two bookies
        this.testEnv.startNewBookie();

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE, 2);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE, 2);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE, 2);

        List<BookieId> bookies;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_BRIN)
                    .column("s", ColumnTypes.STRING)
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "1")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "s", "2")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "s", "3")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "s", "4")), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = server_1.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_1.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
            BookkeeperCommitLog log = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            bookies = log.getWriter().getOut().getLedgerMetadata().getAllEnsembles().firstEntry().getValue();
        }

        // pause one bookie and boot a new follower server
        // leader is offline, it must read the log
        // we must be able to recover even if one bookie is down
        for (BookieId bAddress : bookies) {
            this.testEnv.pauseBookie(bAddress);

            ServerConfiguration serverconfig_2 = serverconfig_1
                    .copy()
                    .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                    .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                    .set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
            try (Server server_2 = new Server(serverconfig_2)) {

                server_2.getManager().setActivatorPauseStatus(true);
                server_2.start();
                assertTrue(server_2.getManager().isTableSpaceLocallyRecoverable(server_2.getMetadataStorageManager().describeTableSpace(TableSpace.DEFAULT)));
                server_2.getManager().setActivatorPauseStatus(false);
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION).found());
            }

            this.testEnv.resumeBookie(bAddress);
        }
//

        // stop one bookie and boot a new follower server
        // leader is offline, it must read the log
        // we must be able to recover even if one bookie is down
        for (BookieId bAddress : bookies) {
            this.testEnv.stopBookie(bAddress);

            ServerConfiguration serverconfig_2 = serverconfig_1
                    .copy()
                    .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                    .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                    .set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
            try (Server server_2 = new Server(serverconfig_2)) {

                server_2.getManager().setActivatorPauseStatus(true);
                server_2.start();
                assertTrue(server_2.getManager().isTableSpaceLocallyRecoverable(server_2.getMetadataStorageManager().describeTableSpace(TableSpace.DEFAULT)));
                server_2.getManager().setActivatorPauseStatus(false);
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION).found());
            }

            this.testEnv.startStoppedBookie(bAddress);
        }
    }
}
