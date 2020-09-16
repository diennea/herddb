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
package herddb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.LockHandle;
import java.sql.Connection;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing about setTransactionIsolation
 *
 * @author enrico.olivelli
 */
public class TransactionIsolationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        try (HerdDBEmbeddedDataSource dataSource = new HerdDBEmbeddedDataSource()) {

            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ClientConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            try (Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement()) {
                statement.execute("CREATE TABLE mytable (key string primary key, name string)");

            }
            Server server = dataSource.getServer();
            try (Connection con = dataSource.getConnection();
                    Statement statement = con.createStatement()) {
                assertEquals(1, statement.executeUpdate("INSERT INTO mytable (key,name) values('k1','name1')"));

                assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
                assertEquals(TableSpace.DEFAULT, con.getSchema());
                assertTrue(con.getAutoCommit());

                con.setAutoCommit(false);

                {
                    HerdDBConnection hCon = (HerdDBConnection) con;
                    assertEquals(0, hCon.getTransactionId());

                    statement.executeQuery("SELECT * FROM mytable").close();
                    long tx = hCon.getTransactionId();

                    Transaction transaction = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx);

                    // in TRANSACTION_READ_COMMITTED no lock is to be retained
                    assertTrue(transaction.locks.get("mytable").isEmpty());

                    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    statement.executeQuery("SELECT * FROM mytable").close();

                    LockHandle lock = transaction.lookupLock("mytable", Bytes.from_string("k1"));
                    assertFalse(lock.write);

                    statement.executeQuery("SELECT * FROM mytable FOR UPDATE").close();
                    lock = transaction.lookupLock("mytable", Bytes.from_string("k1"));
                    assertTrue(lock.write);

                    con.rollback();
                    assertNull(server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx));
                }

                // test SELECT ... FOR UPDATE
                {
                    HerdDBConnection hCon = (HerdDBConnection) con;
                    assertEquals(0, hCon.getTransactionId());
                    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

                    statement.executeQuery("SELECT * FROM mytable FOR UPDATE").close();
                    long tx = hCon.getTransactionId();
                    Transaction transaction = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx);
                    LockHandle lock = transaction.lookupLock("mytable", Bytes.from_string("k1"));
                    assertTrue(lock.write);

                    con.rollback();
                    assertNull(server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx));
                }

            }

        }
    }

}
