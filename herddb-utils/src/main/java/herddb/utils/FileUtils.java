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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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
        Files.walkFileTree(directory, DeleteFileVisitor.INSTANCE);
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


    private static final int COPY_BUFFER_SIZE = 8 * 1024;

    public static long copyStreams(final InputStream input, final OutputStream output) throws IOException {
        long count = 0;
        int n = 0;
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    public static long copyStreams(final InputStream input, final OutputStream output, final long sizeToCopy) throws IOException {
        long count = 0;
        int n = 0;
        final int bufferSize;
        if (COPY_BUFFER_SIZE > sizeToCopy) {
            bufferSize = (int) sizeToCopy;
        } else {
            bufferSize = COPY_BUFFER_SIZE;
        }
        byte[] buffer = new byte[bufferSize];
        while (count + bufferSize < sizeToCopy && -1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        int remaining = (int) (sizeToCopy - count);
        if (remaining > 0) {
            buffer = new byte[remaining];
            n = input.read(buffer);
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }
}
