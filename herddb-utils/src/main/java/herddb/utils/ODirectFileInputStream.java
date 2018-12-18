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
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import io.netty.util.internal.PlatformDependent;

/**
 * {@code O_DIRECT} InputStream implementation.
 *
 * @author diego.salvi
 */
public class ODirectFileInputStream extends InputStream {

    private final ByteBuffer originalBuffer;
    private final ByteBuffer block;
    private final FileChannel fc;
    private final int alignment;
    private final int fetchSize;
    private final int batchBlocks;

    private int readBlocks;

    private static final int EOF = -1;
    private boolean eof;

    public ODirectFileInputStream(Path path) throws IOException {
        this(path, 1);
    }

    public ODirectFileInputStream(Path path, int batchBlocks) throws IOException {

        fc = OpenFileUtils.openFileChannelWithO_DIRECT(path, StandardOpenOption.READ);

        this.batchBlocks = batchBlocks;
        try {
            alignment = (int) OpenFileUtils.getBlockSize(path);
        } catch (IOException err) {
            fc.close();
            throw err;
        }
        fetchSize = alignment * batchBlocks;
        originalBuffer = ByteBuffer.allocateDirect(fetchSize + fetchSize);
        block = OpenFileUtils.alignedSlice(originalBuffer, alignment);

        eof = false;

        ((Buffer) block).position(0);
        ((Buffer) block).limit(alignment);
        ((Buffer) block).flip();
    }

    public int getAlignment() {
        return alignment;
    }

    public int getReadBlocks() {
        return readBlocks;
    }

    public int getBatchBlocks() {
        return batchBlocks;
    }

    public FileChannel getFileChannel() {
        return fc;
    }

    @Override
    public int read() throws IOException {

        if (block.remaining() == 0) {
            fill();
            if (eof && !block.hasRemaining()) {
                return EOF;
            }
        }

        return block.get() & 0xff;

    }

    private void fill() throws IOException {

        ((Buffer) block).position(0);
        ((Buffer) block).limit(fetchSize);

        int read = fc.read(block);

        ((Buffer) block).flip();

        if (read > EOF) {
            readBlocks += batchBlocks;
        }

        if (read < fetchSize) {
            eof = true;
        }

    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {

        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int read = 0;
        while (read < len) {

            if (eof && !block.hasRemaining()) {

                // ritorna quello che ha letto fin'ora!!
                if (read > 0) {
                    return read;
                }

                return EOF;
            }

            int remaining = block.remaining();

            if (remaining < 1) {
                fill();
                remaining = block.remaining();
            }

            /* Trasferisce più dati possibili dal buffer di lettura al byte array */
            int buflen = Math.min(len - read, remaining);
            block.get(b, off + read, buflen);

            read += buflen;

        }

        return len;
    }

    @Override
    public int available() throws IOException {
        return block.remaining();
    }

    @Override
    public void close() throws IOException {
        fc.close();
        PlatformDependent.freeDirectBuffer(originalBuffer);
    }

}
