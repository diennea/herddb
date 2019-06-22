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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import herddb.utils.Bytes;
import java.io.Serializable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Simple tests around TmpMaps
 */
public class TmpMapTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testIntMap() throws Exception {
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<Integer, String> tmpMap = manager
                    .newMap()
                    .withIntKeys()
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(i, "foo" + i);
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(i));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(i));
                }

                // negative tests
                assertNull(tmpMap.get(-1234));
                assertFalse(tmpMap.containsKey(-1234));
            }
        }
    }

    
    
    @Test
    public void testNoThreadSafeMap() throws Exception {
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)                
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<Integer, String> tmpMap = manager
                    .newMap()
                    .threadsafe(false)
                    .withIntKeys()
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(i, "foo" + i);
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(i));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(i));
                }

                // negative tests
                assertNull(tmpMap.get(-1234));
                assertFalse(tmpMap.containsKey(-1234));
            }
        }
    }

    @Test
    public void testStringMap() throws Exception {
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<Integer, String> tmpMap = manager
                    .newMap()
                    .withIntKeys()
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(i, "foo" + i);
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(i));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(i));
                }

                // negative tests
                assertNull(tmpMap.get(-1234));
                assertFalse(tmpMap.containsKey(-1234));
            }
        }
    }

    static class MyPojo implements Serializable {

        // NO NEED FOR equals/hashCode, we are storing a serialized version of the object
        private final int wrapped;

        public MyPojo(int wrapped) {
            this.wrapped = wrapped;
        }

    }

    @Test
    public void testObjectMapCustomKeySerializer() throws Exception {

        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<MyPojo, String> tmpMap = manager
                    .<String>newMap()
                    .withExpectedValueSize(8)
                    .withObjectKeys(MyPojo.class)
                    .withKeySerializer((MyPojo k) -> Bytes.intToByteArray(k.wrapped))
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(new MyPojo(i), "foo" + i);
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(new MyPojo(i)));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(new MyPojo(i)));
                }

                // negative tests
                assertNull(tmpMap.get(new MyPojo(-1234)));
                assertFalse(tmpMap.containsKey(new MyPojo(-1234)));
            }
        }

    }

    @Test
    public void testObjectMapDefaultKeySerializer() throws Exception {

        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<MyPojo, String> tmpMap = manager
                    .<String>newMap()
                    .withExpectedValueSize(8)
                    .withObjectKeys(MyPojo.class)
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(new MyPojo(i), "foo" + i);
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(new MyPojo(i)));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(new MyPojo(i)));
                }

                // negative tests
                assertNull(tmpMap.get(new MyPojo(-1234)));
                assertFalse(tmpMap.containsKey(new MyPojo(-1234)));
            }
        }
    }
}
