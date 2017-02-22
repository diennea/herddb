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
import java.io.OutputStream;
import java.util.Arrays;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Utility for XXHash64
 *
 * @author enrico.olivelli
 */
public class XXHash64Utils {

    private final static int DEFAULT_SEED = 0x9747b28c;
    private final static XXHashFactory factory = XXHashFactory.fastestInstance();
    private final static XXHash64 HASHER = factory.hash64();
    private final static int HASH_LEN = 8;

    public static byte[] digest(byte[] array, int offset, int len) {
        long hash = HASHER.hash(array, offset, len, DEFAULT_SEED);
        byte[] digest = Bytes.from_long(hash).data;
        return digest;
    }

    public static boolean verifyBlockWithFooter(byte[] array, int offset, int len) {
        byte[] expectedFooter = Arrays.copyOfRange(array, len - HASH_LEN, len);
        long expectedHash = HASHER.hash(array, offset, len - HASH_LEN, DEFAULT_SEED);
        long hash = Bytes.toLong(expectedFooter, 0, HASH_LEN);
        return hash == expectedHash;
    }

    public static final class HashingStream extends InputStream {

        private final StreamingXXHash64 hash;
        private final byte[] singleByteBuffer = new byte[1];
        private final InputStream in;

        public HashingStream(InputStream in) {
            this.in = in;
            hash = factory.newStreamingHash64(DEFAULT_SEED);
        }

        @Override
        public long skip(long n) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int res = in.read(b, off, len); //To change body of generated methods, choose Tools | Templates.
            if (res > 0) {
                hash.update(b, off, res);
            }
            return res;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int res = in.read(b); //To change body of generated methods, choose Tools | Templates.
            if (res > 0) {
                hash.update(b, 0, res);
            }
            return res;
        }

        @Override
        public int read() throws IOException {
            int result = in.read();
            if (result == -1) {
                return -1;
            }
            singleByteBuffer[0] = (byte) result;
            hash.update(singleByteBuffer, 0, 1);
            return result;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mark(int readlimit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public int available() throws IOException {
            return in.available();
        }

        public long hash() {
            return hash.getValue();
        }

    }

    public static final class HashingOutputStream extends OutputStream {

        private final StreamingXXHash64 hash;
        private final byte[] singleByteBuffer = new byte[1];
        private final OutputStream out;
        private long size;

        public HashingOutputStream(OutputStream in) {
            this.out = in;
            hash = factory.newStreamingHash64(DEFAULT_SEED);
        }

        @Override
        public void write(int b) throws IOException {
            singleByteBuffer[0] = (byte) b;
            hash.update(singleByteBuffer, 0, 1);
            out.write(b);
            size++;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            hash.update(b, off, len);
            out.write(b, off, len);
            size += len;
        }

        @Override
        public void write(byte[] b) throws IOException {
            int len = b.length;
            hash.update(b, 0, len);
            out.write(b, 0, len);
            size += len;
        }

        public long hash() {
            return hash.getValue();
        }

        public long size() {
            return size;
        }

    }

}
