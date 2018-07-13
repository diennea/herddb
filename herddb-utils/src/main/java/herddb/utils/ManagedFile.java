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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.EnumSet;
import java.util.Set;

/**
 * A <i>"managed"</i> file that give access to underlying {@link OutputStream} for write operations and permit
 * to sync to disk flushed data.
 *
 * @author diego.salvi
 */
public class ManagedFile implements AutoCloseable {

    private final static boolean REQUIRE_FSYNCH = SystemProperties.getBooleanSystemProperty(
        "herddb.file.requirefsync", true);

    private final FileChannel channel;
    private final OutputStream stream;

    private ManagedFile(FileChannel channel) {
        this.channel = channel;
        this.stream = Channels.newOutputStream(channel);
    }

    private static final Set<StandardOpenOption> DEFAULT_OPTIONS_SET = EnumSet.of(
            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE );

    private static final StandardOpenOption[] DEFAULT_OPTIONS =
            DEFAULT_OPTIONS_SET.toArray(new StandardOpenOption[DEFAULT_OPTIONS_SET.size()]);


    public static ManagedFile open(Path path) throws IOException {
        return open(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE));
    }

    /**
     * Opens or creates a file, returning a {@link ManagedFile} that may be used to write bytes to the file.
     *
     * <p>
     * If no options are present then this method works as if the CREATE, TRUNCATE_EXISTING, and WRITE options
     * are present. In other words, it opens the file for writing, creating the file if it doesn't exist, or
     * initially truncating an existing regular-file to a size of 0 if it exists.
     * </p>
     *
     * @param path
     * @param options
     * @return
     * @throws IOException
     */
    public static ManagedFile open(Path path, StandardOpenOption... options) throws IOException {
        return open(FileChannel.open(path, options == null || options.length < 1 ? DEFAULT_OPTIONS : options));
    }

    /**
     * Opens or creates a file, returning a {@link ManagedFile} that may be used to write bytes to the file.
     *
     * <p>
     * If no options are present then this method works as if the CREATE, TRUNCATE_EXISTING, and WRITE options
     * are present. In other words, it opens the file for writing, creating the file if it doesn't exist, or
     * initially truncating an existing regular-file to a size of 0 if it exists.
     * </p>
     *
     * @param path
     * @param options
     * @param attrs
     * @return
     * @throws IOException
     */
    public static ManagedFile open(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return open(FileChannel.open(path, options == null || options.size() < 1 ? DEFAULT_OPTIONS_SET : options, attrs));
    }

    public static ManagedFile open(FileChannel channel) throws IOException {
        return new ManagedFile(channel);
    }

    /**
     * Forces any updates to be written to the storage device that contains it.
     *
     * @throws IOException
     *
     * @see FileChannel#force(boolean)
     */
    public void sync() throws IOException {
        if (!REQUIRE_FSYNCH) {
            return;
        }
        channel.force(false);
    }

    /**
     * Forces any updates to be written to the storage device that contains it.
     *
     * @throws IOException
     *
     * @see FileChannel#force(boolean)
     */
    public void sync(boolean metaData) throws IOException {
        if (!REQUIRE_FSYNCH) {
            return;
        }
        channel.force(metaData);
    }

    /**
     * Returns the {@link OutputStream} that can be used to write to the file
     *
     * @return file output stream
     */
    public OutputStream getOutputStream() {
        return stream;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

}
