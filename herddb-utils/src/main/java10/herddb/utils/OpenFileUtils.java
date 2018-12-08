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

import com.sun.nio.file.ExtendedOpenOption;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Java 10 compatibile version. In Java 10 you can use O_DIRECT
 *
 * @author enrico.olivelli
 */
public class OpenFileUtils {

    private static final Logger LOG = Logger.getLogger(OpenFileUtils.class.getName());

    static {
        LOG.info("Using O_DIRECT, available from JDK10+");
    }

    public static FileChannel openFileChannelWithO_DIRECT(Path path, OpenOption... options) throws IOException {
        OpenOption[] options2 = new OpenOption[options.length + 1];
        System.arraycopy(options, 0, options2, 0, options.length);
        options2[options2.length - 1] = ExtendedOpenOption.DIRECT;
        return FileChannel.open(path, options2);
    }

    public static long getBlockSize(Path p) throws IOException {
        return Files.getFileStore(p).getBlockSize();
    }

    public static boolean isO_DIRECT_Supported() {
        return true;
    }

    public static ByteBuffer allocateAlignedBuffer(int capacity, int alignment) {
        return ByteBuffer
                .allocateDirect(capacity)
                .alignedSlice(alignment);
    }
}
