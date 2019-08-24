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

package herddb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DiskArrayListTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void noswap() throws IOException {
        try (DiskArrayList<String> list = new DiskArrayList<>(1000, testFolder.getRoot().toPath(), new StringSerializer());) {
            for (int i = 0; i < 1000; i++) {
                list.add("a");
            }
            list.finish();
            assertFalse(list.isSwapped());
            {
                int read = 0;
                for (String s : list) {
                    read++;
                    assertEquals("a", s);
                }
                assertEquals(1000, read);
            }

            {
                int read = 0;
                for (String s : list) {
                    read++;
                    assertEquals("a", s);
                }
                assertEquals(1000, read);
            }
        }
    }

    @Test
    public void empty() throws IOException {
        try (DiskArrayList<String> list = new DiskArrayList<>(1000, testFolder.getRoot().toPath(), new StringSerializer())) {
            list.finish();

            assertFalse(list.isSwapped());
            int read = 0;
            for (String s : list) {
                read++;
            }

            assertEquals(
                    0, read);
        }
    }

    @Test
    public void swap() throws IOException {
        try (DiskArrayList<String> list = new DiskArrayList<>(1000, testFolder.getRoot().toPath(), new StringSerializer())) {
            for (int i = 0;
                 i < 1100; i++) {
                list.add("a");
            }

            list.finish();

            assertTrue(list.isSwapped());
            int read = 0;
            for (String s : list) {
                read++;
                assertEquals("a", s);
            }

            assertEquals(
                    1100, read);

            // facciamo una seconda lettura
            read = 0;
            for (String s : list) {
                read++;
                assertEquals("a", s);
            }

            assertEquals(1100, read);
        }
    }

    @Test
    public void swapandzip() throws IOException {
        try (DiskArrayList<String> list = new DiskArrayList<>(1000, testFolder.getRoot().toPath(), new StringSerializer())) {
            list.enableCompression();
            for (int i = 0;
                 i < 1100; i++) {
                list.add("a");
            }

            list.finish();

            assertTrue(list.isSwapped());
            int read = 0;
            for (String s : list) {
                read++;
                assertEquals("a", s);
            }

            assertEquals(
                    1100, read);

            // facciamo una seconda lettura
            read = 0;
            for (String s : list) {
                read++;
                assertEquals("a", s);
            }

            assertEquals(
                    1100, read);
        }
    }

    private static class StringSerializer implements DiskArrayList.Serializer<String> {

        public StringSerializer() {
        }

        @Override
        public String read(ExtendedDataInputStream oo) throws IOException {
            return oo.readUTF();
        }

        @Override
        public void write(String object, ExtendedDataOutputStream oo) throws IOException {
            oo.writeUTF(object);
        }
    }
}
