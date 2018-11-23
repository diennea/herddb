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

import com.sun.jna.Native;
import com.sun.jna.Platform;
import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

/**
 *
 * @author eolivelli
 */
/**
 * Provides access to operating system-specific {@code fallocate} and
 * {@code posix_fallocate} functions.
 */
public final class Fallocate {

    private static final boolean IS_LINUX = Platform.isLinux();
    private static final boolean IS_POSIX = !Platform.isWindows();

    private static final int FALLOC_FL_KEEP_SIZE = 0x01;

    private final int fd;
    private int mode;
    private long offset;
    private final long length;

    private Fallocate(int fd, long length) {
        if (!isSupported()) {
            throwUnsupported("fallocate");
        }
        this.fd = fd;
        this.length = length;
    }

    public static boolean isSupported() {
        return IS_POSIX;
    }

    public static Fallocate forChannel(FileChannel channel, long length) {
        return new Fallocate(getDescriptor(channel), length);
    }

    public static Fallocate forDescriptor(FileDescriptor descriptor, long length) {
        return new Fallocate(getDescriptor(descriptor), length);
    }

    public Fallocate fromOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public Fallocate keepSize() {
        requireLinux("fallocate keep size");
        mode |= FALLOC_FL_KEEP_SIZE;
        return this;
    }

    private void requireLinux(String feature) {
        if (!IS_LINUX) {
            throwUnsupported(feature);
        }
    }

    private void throwUnsupported(String feature) {
        throw new UnsupportedOperationException(feature
                + " is not supported on this operating system");
    }

    public void execute() throws IOException {
        final int errno;
        if (IS_LINUX) {
            final int result = FallocateHolder.fallocate(fd, mode, offset, length);
            errno = result == 0 ? 0 : Native.getLastError();
        } else {
            errno = PosixFallocateHolder.posix_fallocate(fd, offset, length);
        }
        if (errno != 0) {
            throw new IOException("fallocate returned " + errno);
        }
    }

    private static class FallocateHolder {

        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        private static native int fallocate(int fd, int mode, long offset, long length);
    }

    private static class PosixFallocateHolder {

        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        private static native int posix_fallocate(int fd, long offset, long length);
    }

    private static int getDescriptor(FileChannel channel) {
        try {
            // sun.nio.ch.FileChannelImpl declares private final java.io.FileDescriptor fd
            final Field field = channel.getClass().getDeclaredField("fd");
            field.setAccessible(true);
            return getDescriptor((FileDescriptor) field.get(channel));
        } catch (final Exception e) {
            throw new UnsupportedOperationException("unsupported FileChannel implementation", e);
        }
    }

    private static int getDescriptor(FileDescriptor descriptor) {
        try {
            // Oracle java.io.FileDescriptor declares private int fd
            final Field field = descriptor.getClass().getDeclaredField("fd");
            field.setAccessible(true);
            return (int) field.get(descriptor);
        } catch (final Exception e) {
            throw new UnsupportedOperationException("unsupported FileDescriptor implementation", e);
        }
    }
}
