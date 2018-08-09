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
import java.util.logging.Logger;

/**
 * Java 9 compatibile version. Use Arrays.compare(byte[], byte[]) which leverage
 * HotSpot intrisicts
 */
public final class CompareBytesUtils {

    private static final Logger LOG = Logger.getLogger(CompareBytesUtils.class.getName());

    static {
        LOG.info("Using Arrays#compare(byte[], byte[])");
    }

    private CompareBytesUtils() {
    }

    public static int compare(byte[] left, byte[] right) {
        return Arrays.compare(left, right);
    }
}
