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
import static org.junit.Assert.fail;
import herddb.utils.Bytes;
import herddb.utils.TestUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
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

                {
                    Set<Integer> keys = new HashSet<>();
                    tmpMap.forEachKey(keys::add);
                    assertEquals(1000, keys.size());
                }
                {
                    Set<Integer> keys = new HashSet<>();
                    Set<String> values = new HashSet<>();
                    tmpMap.forEach((k, v) -> {
                        keys.add(k);
                        values.add(v);
                        assertEquals("foo" + k, v);
                        return true;
                    });
                    assertEquals(1000, keys.size());
                    assertEquals(1000, values.size());
                }

                // negative tests
                assertNull(tmpMap.get(-1234));
                assertFalse(tmpMap.containsKey(-1234));
            }
        }
    }

    @Test
    public void testLongMap() throws Exception {
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<Long, String> tmpMap = manager
                    .newMap()
                    .withLongKeys()
                    .build()) {
                for (long i = 0; i < 1000; i++) {
                    tmpMap.put(i, "foo" + i);
                }
                for (long i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(i));
                }
                for (long i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(i));
                }
                {
                    Set<Long> keys = new HashSet<>();
                    tmpMap.forEachKey(keys::add);
                    assertEquals(1000, keys.size());
                }
                {
                    Set<Long> keys = new HashSet<>();
                    Set<String> values = new HashSet<>();
                    tmpMap.forEach((k, v) -> {
                        keys.add(k);
                        values.add(v);
                        assertEquals("foo" + k, v);
                        return true;
                    });
                    assertEquals(1000, keys.size());
                    assertEquals(1000, values.size());
                }

                // negative tests
                assertNull(tmpMap.get(-1234L));
                assertFalse(tmpMap.containsKey(-1234L));
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
            try (TmpMap<String, String> tmpMap = manager
                    .newMap()
                    .withStringKeys()
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(i + "", "foo" + i);
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(i + ""));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals("foo" + i, tmpMap.get(i + ""));
                }
                {
                    Set<String> keys = new HashSet<>();
                    tmpMap.forEachKey(keys::add);
                    assertEquals(1000, keys.size());
                }
                {
                    Set<String> keys = new HashSet<>();
                    Set<String> values = new HashSet<>();
                    tmpMap.forEach((k, v) -> {
                        keys.add(k);
                        values.add(v);
                        assertEquals("foo" + k, v);
                        return true;
                    });
                    assertEquals(1000, keys.size());
                    assertEquals(1000, values.size());
                }

                // negative tests
                assertNull(tmpMap.get(-1234 + ""));
                assertFalse(tmpMap.containsKey(-1234 + ""));
                for (int i = 0; i < 1000; i++) {
                    tmpMap.remove(i + "");
                }
                for (int i = 0; i < 1000; i++) {
                    assertFalse(tmpMap.containsKey(i + ""));
                }

            }
        }
    }

    @Test
    public void testCustomSerializer() throws Exception {
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap<Integer, MyPojo> tmpMap = manager
                    .<MyPojo>newMap()
                    .withValueSerializer(new ValueSerializer<MyPojo>() {
                        @Override
                        public void serialize(MyPojo object, OutputStream outputStream) throws Exception {
                            outputStream.write(Bytes.intToByteArray(object.wrapped));
                        }

                        @Override
                        public MyPojo deserialize(Bytes bytes) throws Exception {
                            return new MyPojo(bytes.to_int());
                        }
                    })
                    .withIntKeys()
                    .build()) {
                for (int i = 0; i < 1000; i++) {
                    tmpMap.put(i, new MyPojo(i));
                }
                for (int i = 0; i < 1000; i++) {
                    assertTrue(tmpMap.containsKey(i));
                }
                for (int i = 0; i < 1000; i++) {
                    assertEquals(new MyPojo(i), tmpMap.get(i));
                }
                {
                    Set<Integer> keys = new HashSet<>();
                    tmpMap.forEachKey(keys::add);
                    assertEquals(1000, keys.size());
                }
                {
                    Set<Integer> keys = new HashSet<>();
                    Set<MyPojo> values = new HashSet<>();
                    tmpMap.forEach((k, v) -> {
                        keys.add(k);
                        values.add(v);
                        assertEquals(new MyPojo(k), v);
                        return true;
                    });
                    assertEquals(1000, keys.size());
                    assertEquals(1000, values.size());
                }

                // negative tests
                assertNull(tmpMap.get(-1234));
                assertFalse(tmpMap.containsKey(-1234));
            }
        }
    }

    static class MyPojo implements Serializable {

        private final int wrapped;

        public MyPojo(int wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 89 * hash + this.wrapped;
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
            return this.wrapped == other.wrapped;
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

    @Test
    public void testForEachSinks() throws Exception {
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

                {
                    Set<Integer> keys = new HashSet<>();
                    tmpMap.forEachKey((k -> {
                        keys.add(k);
                        return k <= 10;
                    }));
                    assertEquals(12, keys.size());
                }

                {
                    SinkException thrown =
                            TestUtils.expectThrows(SinkException.class, () -> {
                                tmpMap.forEach((k, v) -> {
                                    if (k >= 5) {
                                        throw new IOException();
                                    }
                                    return true;
                                });
                            });
                    assertTrue(thrown.getCause() instanceof IOException);
                }

                {
                    SinkException thrown =
                            TestUtils.expectThrows(SinkException.class, () -> {
                                tmpMap.forEachKey((k) -> {
                                    if (k >= 5) {
                                        throw new IOException();
                                    }
                                    return true;
                                });
                            });
                    assertTrue(thrown.getCause() instanceof IOException);
                }

            }
        }
    }

    @Test
    public void testClassCastException() throws Exception {
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .maxMemory(10 * 1024 * 1024)
                .tmpDirectory(tmpDir.newFolder().toPath())
                .build()) {
            manager.start();
            try (TmpMap tmpMap = manager
                    .newMap()
                    .withIntKeys()
                    .build()) {
                    tmpMap.put("this-is-not-an-int", "foo");
                    fail();
            } catch (Exception err) {
                assertTrue(TestUtils.isExceptionPresentInChain(err, ClassCastException.class));
            }
        }
    }
}
