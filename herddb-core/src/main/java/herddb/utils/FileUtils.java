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
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

/**
 * Utilities
 *
 * @author enrico.olivelli
 */
public class FileUtils {

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
        try (SeekableByteChannel c = Files.newByteChannel(f, StandardOpenOption.READ)) {
            int len = (int) Files.size(f);
            if (USE_DIRECT_BUFFER) {
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
            } else {
                ByteBuffer buffer = ByteBuffer.allocate(len);
                long res = c.read(buffer);
                if (res != len) {
                    throw new IOException("not all file " + f.toAbsolutePath() + " was read with NIO len=" + len + " read=" + res);
                }
                byte[] result = buffer.array();
                return result;
            }
        }
    }

    private static final boolean USE_DIRECT_BUFFER
        = SystemProperties.getBooleanSystemProperty("herddb.nio.usedirectmemory", true);

    public static void fastWriteFile(Path f, byte[] buffer, int offset, int len) throws IOException {
        if (USE_DIRECT_BUFFER) {
            ByteBuffer b = ByteBuffer.allocateDirect(len);
            try {
                b.put(buffer, offset, len);
                b.flip();
                try (SeekableByteChannel c = Files.newByteChannel(f, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                    int res = c.write(b);
                    if (res != len) {
                        throw new IOException("not all file " + f.toAbsolutePath() + " was written with NIO len=" + len + " writen=" + res);
                    }
                }
            } finally {
                forceReleaseBuffer(b);
            }
        } else {
            byte[] copy = Arrays.copyOfRange(buffer, offset, offset + len);
            Files.write(f, copy, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }
    }

    private static final Class<? extends ByteBuffer> SUN_DIRECT_BUFFER;
    private static final Method SUN_BUFFER_CLEANER;
    private static final Method SUN_CLEANER_CLEAN;

    static {
        if (!USE_DIRECT_BUFFER) {
            SUN_DIRECT_BUFFER = null;
            SUN_BUFFER_CLEANER = null;
            SUN_CLEANER_CLEAN = null;
        } else {
            Method bufferCleaner = null;
            Method cleanerClean = null;
            Class<? extends ByteBuffer> BUF_CLASS = null;
            try {
                BUF_CLASS = (Class<? extends ByteBuffer>) Class.forName("sun.nio.ch.DirectBuffer", true, Thread.currentThread().getContextClassLoader());
                if (BUF_CLASS != null) {
                    bufferCleaner = BUF_CLASS.getMethod("cleaner", (Class[]) null);
                    Class<?> cleanClazz = Class.forName("sun.misc.Cleaner", true, Thread.currentThread().getContextClassLoader());
                    cleanerClean = cleanClazz.getMethod("clean", (Class[]) null);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
            SUN_DIRECT_BUFFER = BUF_CLASS;
            SUN_BUFFER_CLEANER = bufferCleaner;
            SUN_CLEANER_CLEAN = cleanerClean;
        }
    }

    public static void forceReleaseBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        if (SUN_DIRECT_BUFFER != null && SUN_DIRECT_BUFFER.isAssignableFrom(buffer.getClass())) {
            try {
                Object cleaner = SUN_BUFFER_CLEANER.invoke(buffer, (Object[]) null);
                SUN_CLEANER_CLEAN.invoke(cleaner, (Object[]) null);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}
