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

import java.util.stream.Stream;

public final class QueryParser {
    private QueryParser() {
        // no-op
    }

    public static Stream<String[]> parseQueryKeyPairs(final String url) {
        final int sep = url.indexOf('?');
        if (sep > 0) {
            final String sub = url.substring(sep + 1);
            if (!sub.isEmpty()) {
                return Stream.of(sub.split("&"))
                        .map(it -> {
                            final int subSep = it.indexOf('=');
                            return subSep > 0 ? new String[]{it.substring(0, subSep), it.substring(subSep + 1)} : new String[]{it, ""};
                        });
            }
        }
        return Stream.empty();
    }
}
