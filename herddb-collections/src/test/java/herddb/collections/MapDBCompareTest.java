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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import java.io.Serializable;
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

        private final int wrapped;

        public MyPojo(int wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 17 * hash + this.wrapped;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final MyPojo other = (MyPojo) obj;
            if (this.wrapped != other.wrapped) {
                return false;
            }
            return true;
        }

    }

    @Test
    public void test() throws Exception {

        // with 1million mapdb is not able to complete the work
        // let's keep sensible sizes for CI
        int mapSize = 100_000;
        int testIterations = 3; // after two iterations results are almost stable

        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(20 * 1024 * 1024) // we want to swap to disk
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();

            for (int it = 0; it < testIterations; it++) {
                long deltaHerd;
                long _stopHerd;
                long _start = System.currentTimeMillis();
                try (TmpMap<MyPojo, String> tmpMap = manager
                        .<String>newMap()
                        .withExpectedValueSize(8)
                        .withObjectKeys(MyPojo.class)
                        .build()) {
                    for (int i = 0; i < mapSize; i++) {
                        tmpMap.put(new MyPojo(i), "foo" + i);
                    }
                    for (int i = 0; i < mapSize; i++) {
                        assertEquals("foo" + i, tmpMap.get(new MyPojo(i)));
                    }
                    System.out.println("STATS: " + tmpMap.size() + " entries");
                    System.out.println("STATS: " + tmpMap.estimateCurrentMemoryUsage() + " bytes in memory");
                    System.out.println("STATS: swapped: " + tmpMap.isSwapped());
                    assertTrue(tmpMap.isSwapped());

                    _stopHerd = System.currentTimeMillis();
                    deltaHerd = (_stopHerd - _start);
                    System.out.println("TIME WARMUP HERDDB: " + deltaHerd);
                }

                try (DB db = DBMaker
                        .fileDB(new File(tmpDir.newFolder(), "db.mapdb"))
                        .fileMmapEnableIfSupported()
                        .make();) {

                    try (HTreeMap tmpMap = db
                            .hashMap("tmpmap", Serializer.ELSA, Serializer.STRING)
                            .createOrOpen();) {
                        for (int i = 0; i < mapSize; i++) {
                            tmpMap.put(new MyPojo(i), "foo" + i);
                        }
                        for (int i = 0; i < mapSize; i++) {
                            assertEquals("foo" + i, tmpMap.get(new MyPojo(i)));
                        }
                    }
                    long _stopMapDb = System.currentTimeMillis();
                    long deltaMapDB = (_stopMapDb - _stopHerd);
                    System.out.println("TIME MAPDB: " + deltaMapDB);
                    if (deltaMapDB < deltaHerd) {
                        fail("MapDB seems faster than HerdDB in this case, after " + it + " iteration");
                    }
                }
            }
        }
    }
}
