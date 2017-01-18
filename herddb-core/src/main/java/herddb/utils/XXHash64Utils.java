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

import java.util.Arrays;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Utility for XXHash64
 *
 * @author enrico.olivelli
 */
public class XXHash64Utils {

    private final static int DEFAULT_SEED = 0x9747b28c;
    private final static XXHash64 HASHER = XXHashFactory.fastestInstance().hash64();
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
}
