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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities
 *
 * @author enrico.olivelli
 */
public class QueryUtils {

    private static final Pattern pattern_select = Pattern.compile("select.+from[\\W]+(?<tablespace>[\\S]+\\.)?(?<tablename>[\\S]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern pattern_update = Pattern.compile("update[\\W]+(?<tablespace>[\\S]+\\.)?(?<tablename>[\\S]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern pattern_insert = Pattern.compile("insert[\\W]+into[\\W]+(?<tablespace>[\\S]+\\.)?(?<tablename>[\\S]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern pattern_createtable = Pattern.compile("create\\W+table\\W+(?<tablespace>[\\S]+\\.)?(?<tablename>[\\S]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern pattern_droptable = Pattern.compile("drop\\W+table\\W+(?<tablespace>[\\S]+\\.)?(?<tablename>[\\S]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern pattern_altertable = Pattern.compile("alter\\W+table\\W+(?<tablespace>[\\S]+\\.)?(?<tablename>[\\S]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern[] ALL_PATTERNS = {
        pattern_select,
        pattern_update,
        pattern_insert,
        pattern_createtable,
        pattern_droptable,
        pattern_altertable};

    public static String discoverTablespace(String defaultTableSpace, String query) {
        if (query == null) {
            return defaultTableSpace;
        }
        query = query.trim();
        for (Pattern p : ALL_PATTERNS) {
            Matcher matcher_select = p.matcher(query);
            if (matcher_select.find()) {
                String group0 = matcher_select.group("tablespace");
                if (group0 != null) {
                    return group0.substring(0, group0.length() - 1);
                } else {
                    return defaultTableSpace;
                }
            }
        }
        return defaultTableSpace;
    }
}
