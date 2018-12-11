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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Implementation of OutputStream which is writing using O_DIRECT flag
 *
 * @author enrico.olivelli
 */
public class ODirectFileOutputStream extends OutputStream {

    final ByteBuffer block;
    final FileChannel fc;
    final int alignment;
    int writtenBlocks;

    public ODirectFileOutputStream(Path p) throws IOException {
        this.alignment = (int) OpenFileUtils.getBlockSize(p);
        this.block = OpenFileUtils.allocateAlignedBuffer(alignment + alignment, alignment);
        this.block.position(0);
        this.block.limit(alignment);
        this.fc = OpenFileUtils.openFileChannelWithO_DIRECT(p,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    public int getAlignment() {
        return alignment;
    }

    @Override
    public void write(int b) throws IOException {
        block.put((byte) b);
        flushIfNeeded();
    }

    private void flushIfNeeded() throws IOException {
        if (block.remaining() == 0) {
            block.flip();
            fc.write(block);
            writtenBlocks++;
            block.position(0);
            block.limit(alignment);
        }
    }

    @Override
    public void close() throws IOException {
        // this will add padding
        flush(true);
        fc.close();
    }

    @Override
    public void flush() throws IOException {
        // this will add padding
        flush(true);
    }

    private void flush(boolean pad) throws IOException {
        if (block.position() == 0) {
            // nothing to flush
            return;
        }
        if (pad) {
            int remaining = block.remaining();
            for (int i = 0; i < remaining; i++) {
                block.put((byte) 0);
            }
        }
        block.flip();
        fc.write(block);
        writtenBlocks++;
        block.position(0);
        block.limit(alignment);
    }

    public FileChannel getFc() {
        return fc;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remainingInBlock = block.remaining();
        if (remainingInBlock >= len) {
            // simple
            block.put(b, off, len);
            flushIfNeeded();
        } else {
            int end = off + len;
            int pos = off;
            while (pos < end) {
                int remainingToWrite = len - pos;

                if (remainingToWrite > remainingInBlock) {
                    block.put(b, pos, remainingInBlock);
                    flush(false);
                    pos += remainingInBlock;
                    remainingInBlock = block.remaining();
                } else {
                    block.put(b, pos, remainingToWrite);
                    pos += remainingToWrite;
                    flushIfNeeded();
                }
            }
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public int getWrittenBlocks() {
        return writtenBlocks;
    }

}
