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

import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Utilities
 *
 * @author enrico.olivelli
 */
public class FileUtils {

    private static final boolean USE_DIRECT_BUFFER =
            SystemProperties.getBooleanSystemProperty("herddb.nio.usedirectmemory", false);

    public static void cleanDirectory(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return;
        }
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });
    }

    public static byte[] fastReadFile(Path f) throws IOException {
        int len = (int) Files.size(f);
        if (USE_DIRECT_BUFFER) {
            try (SeekableByteChannel c = Files.newByteChannel(f, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(len);
                try {
                    long res = c.read(buffer);
                    if (res != len) {
                        throw new IOException("not all file " + f.toAbsolutePath() + " was read with NIO len=" + len + " writeen=" + res);
                    }
                    buffer.flip();
                    byte[] result = new byte[len];
                    buffer.get(result);
                    return result;
                } finally {
                    forceReleaseBuffer(buffer);
                }
            }
        } else {
            byte[] result = new byte[len];
            try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "r")) {
                long res = raf.read(result, 0, len);
                if (res != len) {
                    throw new IOException("not all file " + f.toAbsolutePath() + " was read with NIO len=" + len + " read=" + res);
                }
            }
            return result;
        }
    }

    public static void forceReleaseBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        PlatformDependent.freeDirectBuffer(buffer);
    }
}
