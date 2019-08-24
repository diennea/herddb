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
import static org.junit.Assert.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Testing O_DIRECT. This file should be in core project, because it leverages Multi-Release JAR
 * feature of JDK10
 *
 * @author diego.salvi
 */
public class ODirectFileInputStreamTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testEmpty() throws Exception {
        Path path = tmp.newFile().toPath();
        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {
            int read = oo.read();
            assertEquals(-1, read);
        }
    }

    @Test
    public void readBytePerByte() throws Exception {
        Random random = new Random();

        Path path = tmp.newFile().toPath();
        int blockSize = (int) OpenFileUtils.getBlockSize(path);

        int writeSize = blockSize + blockSize / 2;
        assertTrue(writeSize > blockSize);

        byte[] data = new byte[writeSize];
        random.nextBytes(data);
        Files.write(path, data);

        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {

            int count = 0;
            while (oo.read() != -1) {
                ++count;
            }

            assertEquals(writeSize, count);
            assertEquals(2, oo.getReadBlocks());
        }
    }

    @Test
    public void readPartialBlock() throws Exception {
        Random random = new Random();

        Path path = tmp.newFile().toPath();
        int blockSize = (int) OpenFileUtils.getBlockSize(path);

        int partialSize = blockSize / 2;
        assertTrue(partialSize < blockSize);

        byte[] data = new byte[partialSize];
        random.nextBytes(data);
        Files.write(path, data);

        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {
            byte[] fullread = new byte[blockSize];
            int read = oo.read(fullread);

            assertEquals(partialSize, read);
            assertEquals(1, oo.getReadBlocks());
        }
    }

    @Test
    public void readFullAndPartialBlock() throws Exception {
        Random random = new Random();

        Path path = tmp.newFile().toPath();
        int blockSize = (int) OpenFileUtils.getBlockSize(path);

        int writeSize = blockSize + blockSize / 2;
        assertTrue(writeSize > blockSize);

        byte[] data = new byte[writeSize];
        random.nextBytes(data);
        Files.write(path, data);

        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {
            byte[] fullread = new byte[blockSize * 2];
            int read = oo.read(fullread);

            assertEquals(writeSize, read);
            assertEquals(2, oo.getReadBlocks());
        }
    }

    @Test
    public void readFullAndPartialBlockBigRead() throws Exception {
        Random random = new Random();

        Path path = tmp.newFile().toPath();
        int blockSize = (int) OpenFileUtils.getBlockSize(path);

        int fetchSize = 2;
        int writeSize = blockSize * (fetchSize + 1) + blockSize / 2;
        assertTrue(writeSize > blockSize);

        byte[] data = new byte[writeSize];
        random.nextBytes(data);
        Files.write(path, data);

        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {
            byte[] fullread = new byte[2 * writeSize];
            int read = oo.read(fullread);

            assertEquals(writeSize, read);
            assertEquals(fetchSize + 2, oo.getReadBlocks());
        }
    }

    @Test
    public void readFullAndPartialBlockSizeReadBlock() throws Exception {
        Random random = new Random();

        Path path = tmp.newFile().toPath();
        int blockSize = (int) OpenFileUtils.getBlockSize(path);

        int partialSize = blockSize / 2;
        int writeSize = blockSize + partialSize;
        assertTrue(writeSize > blockSize);
        assertTrue(partialSize < blockSize);

        byte[] data = new byte[writeSize];
        random.nextBytes(data);
        Files.write(path, data);

        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {
            byte[] fullread = new byte[blockSize];
            int read = oo.read(fullread);

            assertEquals(blockSize, read);
            assertEquals(1, oo.getReadBlocks());

            read = oo.read(fullread);

            assertEquals(partialSize, read);
            assertEquals(2, oo.getReadBlocks());
        }
    }

    @Test
    public void readFullAndPartialBlockSizeReadLessThanBlock() throws Exception {
        Random random = new Random();

        Path path = tmp.newFile().toPath();
        int blockSize = (int) OpenFileUtils.getBlockSize(path);

        int partialSize = blockSize / 2;
        int writeSize = blockSize + partialSize;
        assertTrue(writeSize > blockSize);
        assertTrue(partialSize < blockSize);

        byte[] data = new byte[writeSize];
        random.nextBytes(data);
        Files.write(path, data);

        try (ODirectFileInputStream oo = new ODirectFileInputStream(path)) {
            byte[] partialRead = new byte[partialSize];

            int totalRead = 0;
            while (totalRead < writeSize) {
                int read = oo.read(partialRead);
                if (read > -1) {
                    totalRead += read;
                }

                assertTrue(read == -1 || read <= partialSize);

            }
            assertEquals(writeSize, totalRead);
            assertEquals(2, oo.getReadBlocks());
        }
    }

}
