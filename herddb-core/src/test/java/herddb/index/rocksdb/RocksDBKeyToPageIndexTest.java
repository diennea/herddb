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
package herddb.index.rocksdb;

import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import java.io.File;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link RocksDBKeyToPageIndex*
 * @author eolivelli
 */
public class RocksDBKeyToPageIndexTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private static final Bytes FOO = Bytes.from_string("foo");
    private static final Bytes FOO2 = Bytes.from_string("foo2");

    @Test
    public void simple() throws Exception {
        try (RocksDBKeyToPageIndex index = new RocksDBKeyToPageIndex(tmpDir.newFolder(), "table")) {
            index.start(LogSequenceNumber.START_OF_TIME);
            index.put(FOO, 10L);
            index.put(FOO, 12L);
            assertEquals(12L, index.get(FOO).longValue());
            assertEquals(12L, index.remove(FOO).longValue());
            assertNull(index.remove(FOO));
            assertNull(index.get(FOO));
            index.put(FOO, 10L);
            assertEquals(10L, index.get(FOO).longValue());
            index.truncate();
            assertNull(index.get(FOO));
            index.truncate();
            index.put(FOO, 13L);
            index.put(FOO2, 14L);
            assertEquals(13L, index.get(FOO).longValue());
            assertEquals(14L, index.get(FOO2).longValue());
            index.truncate();
            assertNull(index.get(FOO));
            assertNull(index.get(FOO2));

        }
    }

    @Test
    public void simpleRestart() throws Exception {
        File folder = tmpDir.newFolder();
        try (RocksDBKeyToPageIndex index = new RocksDBKeyToPageIndex(folder, "table")) {
            index.start(LogSequenceNumber.START_OF_TIME);
            index.put(FOO, 10L);
            assertEquals(10L, index.get(FOO).longValue());
            index.put(FOO, 12L);
            assertEquals(12L, index.get(FOO).longValue());
        }

        try (RocksDBKeyToPageIndex index = new RocksDBKeyToPageIndex(folder, "table")) {
            index.start(LogSequenceNumber.START_OF_TIME);
            // if you boot at "START_OF_TIME" the DB is empty !
            assertNull(index.get(FOO));
        }
    }

    @Test
    public void simpleCheckpoint() throws Exception {
        File folder = tmpDir.newFolder();

        LogSequenceNumber n = new LogSequenceNumber(1, 2);

        try (RocksDBKeyToPageIndex index = new RocksDBKeyToPageIndex(folder, "table")) {
            index.start(LogSequenceNumber.START_OF_TIME);
            index.put(FOO, 10L);
            index.put(FOO, 12L);
            assertEquals(12L, index.get(FOO).longValue());
            index.checkpoint(n, false);
            index.put(FOO, 13L);

        }

        try (RocksDBKeyToPageIndex index = new RocksDBKeyToPageIndex(folder, "table")) {
            index.start(n);
            assertEquals(12L, index.get(FOO).longValue());
        }
    }

}
