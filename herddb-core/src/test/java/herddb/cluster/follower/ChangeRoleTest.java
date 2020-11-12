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
package herddb.cluster.follower;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.ActivatorRunRequest;
import herddb.core.MemoryManager;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

/**
 * Tests about changing roles
 *
 * @author enrico.olivelli
 */
public class ChangeRoleTest extends MultiServerBase {

    @Test
    public void testChangeRoleAndReleaseMemory() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
                Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            server_2.start();

            MemoryManager server2MemoryManager = server_2.getManager().getMemoryManager();

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

            // set forcibly server2 as new follower
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            assertEquals(0, server2MemoryManager.getDataPageReplacementPolicy().size());
            assertEquals(0, server2MemoryManager.getPKPageReplacementPolicy().size());

            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, false);

            assertEquals(2, server2MemoryManager.getDataPageReplacementPolicy().size());
            assertEquals(1, server2MemoryManager.getPKPageReplacementPolicy().size());

            // stop tablespace on server2
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_2.getManager().triggerActivator(ActivatorRunRequest.FULL);

            // wait for tablespace manager to be deallocated
            herddb.utils.TestUtils.waitForCondition(() -> {
                TableSpaceManager tableSpaceManager = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                return tableSpaceManager == null;

            }, herddb.utils.TestUtils.NOOP, 100);

            // memory must have been totally released
            assertEquals(0, server2MemoryManager.getDataPageReplacementPolicy().size());
            assertEquals(0, server2MemoryManager.getPKPageReplacementPolicy().size());

            // start tablespace on server2, as let it become leader
            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server2", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, true);

            assertTrue("unexpected value " + server2MemoryManager.getDataPageReplacementPolicy().size(),
                    server2MemoryManager.getDataPageReplacementPolicy().size() >= 1);
            assertEquals(1, server2MemoryManager.getPKPageReplacementPolicy().size());

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // wait for tablespace manager to be deallocated
            herddb.utils.TestUtils.waitForCondition(() -> {
                TableSpaceManager tableSpaceManager = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                return tableSpaceManager == null;

            }, herddb.utils.TestUtils.NOOP, 100);

            // memory must have been totally released again
            assertEquals(0, server2MemoryManager.getDataPageReplacementPolicy().size());
            assertEquals(0, server2MemoryManager.getPKPageReplacementPolicy().size());

        }

    }

}
