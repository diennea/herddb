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

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class ODirectFileOutputStreamTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testEmpty() throws Exception {
        File file = tmp.newFile();
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
        }
        byte[] read = Files.readAllBytes(file.toPath());
        assertEquals(0, read.length);
    }

    @Test
    public void testSimple() throws Exception {
        byte[] test = "foo".getBytes("ASCII");
        int blocksize;
        File file = tmp.newFile();
        ODirectFileOutputStream _oo;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            oo.write(test);
        }
        byte[] read = Files.readAllBytes(file.toPath());
        assertArrayEquals(test, Arrays.copyOf(read, test.length));
        assertEquals(blocksize, read.length);
        assertEquals(1, _oo.getWrittenBlocks());
    }

    @Test
    public void testTwoWritesInsideFirstBlock() throws Exception {
        byte[] test = "foo".getBytes("ASCII");
        int blocksize;
        File file = tmp.newFile();
        ODirectFileOutputStream _oo;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            oo.write(test);
            oo.write(test);
        }
        byte[] read = Files.readAllBytes(file.toPath());
        assertArrayEquals(test, Arrays.copyOfRange(read, 0, test.length));
        assertArrayEquals(test, Arrays.copyOfRange(read, test.length, test.length + test.length));
        assertEquals(blocksize, read.length);
        assertEquals(1, _oo.getWrittenBlocks());
    }

    @Test
    public void testTwoBlocksLastPadded() throws Exception {
        byte[] test = "foobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobar".getBytes("ASCII");
        int blocksize;
        File file = tmp.newFile();
        ODirectFileOutputStream _oo;
        int countWritten = 0;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            while (oo.getWrittenBlocks() < 1) {
                oo.write(test);
                countWritten++;
            }
        }
        byte[] read = Files.readAllBytes(file.toPath());
        for (int i = 0; i < countWritten; i++) {
            int offset = i * test.length;
            assertArrayEquals(test, Arrays.copyOfRange(read, offset, offset + test.length));
        }
        assertEquals(2, _oo.getWrittenBlocks());
        assertEquals(blocksize * 2, read.length);

    }

    @Test
    public void testThreeBlocksLastPadded() throws Exception {
        byte[] test = "foobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobarfoobar".getBytes("ASCII");
        int blocksize;
        File file = tmp.newFile();
        ODirectFileOutputStream _oo;
        int countWritten = 0;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            while (oo.getWrittenBlocks() < 2) {
                oo.write(test);
                countWritten++;
            }
        }
        byte[] read = Files.readAllBytes(file.toPath());
        for (int i = 0; i < countWritten; i++) {
            int offset = i * test.length;
            assertArrayEquals(test, Arrays.copyOfRange(read, offset, offset + test.length));
        }
        assertEquals(3, _oo.getWrittenBlocks());
        assertEquals(blocksize * 3, read.length);

    }

    @Test
    public void testThreeBlocksExact() throws Exception {
        byte[] test = "four".getBytes("ASCII");
        int blocksize;
        File file = tmp.newFile();
        ODirectFileOutputStream _oo;
        int countWritten = 0;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            assertTrue(blocksize % test.length == 0);

            while (oo.getWrittenBlocks() < 3) {
                oo.write(test);
                countWritten++;
            }
        }
        byte[] read = Files.readAllBytes(file.toPath());
        for (int i = 0; i < countWritten; i++) {
            int offset = i * test.length;
            assertArrayEquals(test, Arrays.copyOfRange(read, offset, offset + test.length));
        }
        assertEquals(3, _oo.getWrittenBlocks());
        assertEquals(blocksize * 3, read.length);

    }

    @Test
    public void testSingleArrayBlockSize() throws Exception {
        int blocksize;
        File file = tmp.newFile();
        byte[] test;
        ODirectFileOutputStream _oo;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            test = new byte[blocksize];
            Arrays.fill(test, (byte) 6);
            oo.write(test);
        }
        byte[] read = Files.readAllBytes(file.toPath());
        assertArrayEquals(test, Arrays.copyOf(read, test.length));
        assertEquals(blocksize, read.length);
        assertEquals(1, _oo.getWrittenBlocks());
    }

    @Test
    public void testSingleArrayBiggerThanBlockSize() throws Exception {
        int blocksize;
        File file = tmp.newFile();
        byte[] test;
        ODirectFileOutputStream _oo;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            test = new byte[blocksize + blocksize / 2];
            Arrays.fill(test, (byte) 6);
            oo.write(test);
        }
        byte[] read = Files.readAllBytes(file.toPath());
        assertArrayEquals(test, Arrays.copyOf(read, test.length));
        assertEquals(blocksize * 2, read.length);
        assertEquals(2, _oo.getWrittenBlocks());
    }

    @Test
    public void testSingleArrayBiggerThanTwoBlockSize() throws Exception {
        int blocksize;
        File file = tmp.newFile();
        byte[] test;
        ODirectFileOutputStream _oo;
        try (ODirectFileOutputStream oo = new ODirectFileOutputStream(file.toPath())) {
            _oo = oo;
            blocksize = oo.getAlignment();
            test = new byte[blocksize + blocksize + blocksize / 2];
            Arrays.fill(test, (byte) 6);
            oo.write(test);
        }
        byte[] read = Files.readAllBytes(file.toPath());
        assertArrayEquals(test, Arrays.copyOf(read, test.length));
        assertEquals(blocksize * 3, read.length);
        assertEquals(3, _oo.getWrittenBlocks());
    }

}
