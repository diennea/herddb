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
package herddb.collections;

import static org.junit.Assert.assertEquals;
import java.io.Serializable;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

/**
 * Simple tests around TmpMaps, comparing with MapDB.org
 */
public class MapDBCompareTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    static class MyPojo implements Serializable {

        // NO NEED FOR equals/hashCode, we are storing a serialized version of the object
        private final int wrapped;

        public MyPojo(int wrapped) {
            this.wrapped = wrapped;
        }

    }

    @Test
//    @Ignore
    public void testObjectMapDefaultKeySerializer() throws Exception {

        int warmupIterations = 1_000_000;
        int testIterations = 100_000;

        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            try (DB db = DBMaker.fileDB(tmpDir.newFolder())
                    .fileMmapEnableIfSupported()
                    .make();) {
                manager.start();
                long _start = System.currentTimeMillis();
                try (TmpMap<MyPojo, String> tmpMap = manager
                        .<String>newMap()
                        .withExpectedValueSize(8)
                        .withObjectKeys(MyPojo.class)
                        .build()) {
                    for (int i = 0; i < warmupIterations; i++) {
                        tmpMap.put(new MyPojo(i), "foo" + i);
                    }
                    for (int i = 0; i < warmupIterations; i++) {
                        assertEquals("foo" + i, tmpMap.get(new MyPojo(i)));
                    }
                }
                long _stopHerd = System.currentTimeMillis();

                try (HTreeMap tmpMap =
                        db
                                .hashMap("tmpmap", Serializer.ELSA, Serializer.STRING)
                                .createOrOpen();) {
                    for (int i = 0; i < warmupIterations; i++) {
                        tmpMap.put(new MyPojo(i), "foo" + i);
                    }
                    for (int i = 0; i < warmupIterations; i++) {
                        assertEquals("foo" + i, tmpMap.get(new MyPojo(i)));
                    }
                }
                long _stopMapDb = System.currentTimeMillis();
                System.out.println("TIME WARMUP HERDDB: " + (_stopHerd - _start));
                System.out.println("TIME WARMUP MAPDB: " + (_stopMapDb - _stopHerd));
            }

        }
    }
}
