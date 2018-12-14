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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Java 8 compatibile version. In Java 8 you cannot use O_DIRECT
 *
 * @author enrico.olivelli
 */
public class OpenFileUtils {

    private static final Logger LOG = Logger.getLogger(OpenFileUtils.class.getName());

    static {
        LOG.info("This JVM is not able to use O_DIRECT (only from Java 10+)");
    }

    public static FileChannel openFileChannelWithO_DIRECT(Path path, OpenOption... options) throws IOException {
        return FileChannel.open(path, options);
    }

    public static long getBlockSize(Path p) throws IOException {
        // this is dummy
        return 4096;
    }

    public static boolean isO_DIRECT_Supported() {
        return false;
    }

    public static ByteBuffer alignedSlice(ByteBuffer buffer, int alignment) {
        // on JDK8 you cannot require an aligned slice, but you cannot use O_DIRECT as well
        return buffer;
    }

    public static void releaseAlignedBuffer(ByteBuffer buffer) {
        PlatformDependent.freeDirectBuffer(buffer);
    }
}
