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
package herddb.checkpoint;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.junit.Test;

/**
 * Checkpoint command test
 *
 * @author lorenzobalzani
 */
public class CheckpointCommandTest {

    private static final Logger LOGGER = Logger.getLogger(CheckpointCommandTest.class.getName());

    @Test
    public void checkpointCommandSuccessTest() throws Exception {
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            execute(manager, "checkpoint tblspace1", Collections.emptyList());
        }
    }

    @Test(expected = herddb.model.TableSpaceDoesNotExistException.class)
    public void checkpointCommandFailureTest() throws Exception {
        final String wrongTableSpace = "THIS_TABLESPACE_DOES_NOT_EXISTS";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            execute(manager, "checkpoint " + wrongTableSpace, Collections.emptyList());
        }
    }

    @Test
    public void checkpointCommandCaseInsensitiveSuccessTest() throws Exception {
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            execute(manager, "checkpoint TBLSPACE1 noWait", Collections.emptyList());
        }
    }

    @Test
    public void checkpointCommandNoWaitSuccessTest() throws Exception {
        AtomicInteger checkpointPerformed = new AtomicInteger(0);
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            Runnable runnable = () -> {
                long returnedValue = execute(manager, "checkpoint tblspace1 noWait", Collections.emptyList()).transactionId;
                if (returnedValue == 0L) {
                    checkpointPerformed.set(checkpointPerformed.get() + 1);
                }
            };
            Thread first = new Thread(runnable);
            Thread second = new Thread(runnable);
            TableSpaceManager myTableSpace = manager.getTableSpaceManager("tblspace1");
            first.start();
            second.start();
            first.join();
            second.join();
            assertEquals("Wrong number of checkpoints", myTableSpace.getCheckpointPerformed(), checkpointPerformed.get());
        }
    }

    @Test
    public void fullCheckpointCommandNoWaitSuccessTest() throws Exception {
        AtomicInteger checkpointPerformed = new AtomicInteger(0);
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            Runnable runnable = () -> {
                long returnedValue = execute(manager, "checkpoint tblspace1 noWait full", Collections.emptyList()).transactionId;
                if (returnedValue == 0L) {
                    checkpointPerformed.set(checkpointPerformed.get() + 1);
                }
            };
            Thread first = new Thread(runnable);
            Thread second = new Thread(runnable);
            TableSpaceManager myTableSpace = manager.getTableSpaceManager("tblspace1");
            first.start();
            second.start();
            first.join();
            second.join();
            assertEquals("Wrong number of checkpoints", myTableSpace.getCheckpointPerformed(), checkpointPerformed.get());
        }
    }
}

