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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;
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
            execute(manager, "CREATE TABLESPACE 'consistence'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE consistence.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO consistence.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            execute(manager, "checkpoint consistence", Collections.emptyList());
        }
    }

    @Test(expected = herddb.model.TableSpaceDoesNotExistException.class)
    public void checkpointCommandFailureTest() throws Exception {
        final String wrongTableSpace = "THIS_TABLESPACE_DOES_NOT_EXISTS";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'consistence'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE consistence.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO consistence.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            execute(manager, "checkpoint " + wrongTableSpace, Collections.emptyList());
        }
    }

    @Test
    public void checkpointCommandNotEnqueueSuccessTest() throws Exception {
        final long[] returnedValues = new long[2];
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'consistence'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE consistence.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO consistence.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            Thread first = new Thread(() -> {
                LOGGER.log(Level.INFO, "First thread has started!");
                returnedValues[0] = execute(manager, "checkpoint consistence -notEnqueue", Collections.emptyList()).transactionId;
                LOGGER.log(Level.INFO, "First thread has finished!");
            });
            Thread second = new Thread(() -> {
                LOGGER.log(Level.INFO, "Second thread has started!");
                returnedValues[1] = execute(manager, "checkpoint consistence -notEnqueue", Collections.emptyList()).transactionId;
                LOGGER.log(Level.INFO, "Second thread has finished!");
            });
            first.start();
            /*
                Force second thread to start after the first in order to verify the simultaneous checkpoints
             */
            Thread.sleep(30);
            second.start();
            first.join();
            second.join();
            assertArrayEquals(new long[]{0, -1}, returnedValues);
        }
    }
}

