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

package herddb.network;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/**
 * Utility for Secure hashes
 */
public class HashUtils {

    /**
     * Compute a SHA-1 hash using Java built-in MessageDigest, and format the
     * result in hex. Beware that hash-computation is a CPU intensive operation
     *
     * @param data
     * @return
     */
    public static String sha1(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("sha-1", "SUN");
            byte[] sign = md.digest(data);
            return arraytohexstring(sign);
        } catch (NoSuchAlgorithmException | NoSuchProviderException imp) {
            throw new RuntimeException(imp);
        }
    }

    public static String sha1(String data) {
        return sha1(data.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Compute a SHA-256 hash using Java built-in MessageDigest, and format the
     * result in hex. Beware that hash-computation is a CPU intensive operation
     *
     * @param data
     * @return
     */
    public static String sha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("sha-256", "SUN");
            byte[] sign = md.digest(data);
            return arraytohexstring(sign);
        } catch (NoSuchAlgorithmException | NoSuchProviderException imp) {
            throw new RuntimeException(imp);
        }
    }

    public static String sha256(String data) {
        return sha256(data.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Compute a MD5 hash using Java built-in MessageDigest, and format the
     * result in hex. Beware that hash-computation is a CPU intensive operation
     *
     * @param data
     * @return
     */
    public static String md5(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("md5", "SUN");
            byte[] sign = md.digest(data);
            return arraytohexstring(sign);
        } catch (NoSuchAlgorithmException | NoSuchProviderException imp) {
            throw new RuntimeException(imp);
        }
    }

    public static String md5(String data) {
        return md5(data.getBytes(StandardCharsets.UTF_8));
    }

    public static String arraytohexstring(byte[] bytes) {
        StringBuilder string = new StringBuilder();
        for (byte b : bytes) {
            String hexString = Integer.toHexString(0x00FF & b);
            string.append(hexString.length() == 1 ? "0" + hexString : hexString);
        }
        return string.toString();
    }
}
