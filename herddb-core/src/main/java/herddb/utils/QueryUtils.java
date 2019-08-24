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

import herddb.model.Index;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class QueryUtils {

    /**
     * Prefix for SELECT before tablespace.tablename
     */
    private static final String PREFIX_SELECT = "select.+\\W+from\\W+";

    /**
     * Prefix for UPDATE before tablespace.tablename
     */
    private static final String PREFIX_UPDATE = "update\\W+";

    /**
     * Prefix for INSERT before tablespace.tablename
     */
    private static final String PREFIX_INSERT = "insert\\W+into\\W+";

    /**
     * Prefix for DELETE before tablespace.tablename
     */
    private static final String PREFIX_DELETE = "delete\\W+from\\W+";

    /**
     * Prefix for TABLE CREATE before tablespace.tablename
     */
    private static final String PREFIX_TABLE_CREATE = "create\\W+table\\W+";

    /**
     * Prefix for TABLE DROP before tablespace.tablename
     */
    private static final String PREFIX_TABLE_DROP = "drop\\W+table\\W+";

    /**
     * Prefix for TRUNCATE TABLE before tablespace.tablename
     */
    private static final String PREFIX_TABLE_TRUNCATE = "truncate\\W+table\\W+";

    /**
     * Prefix for TABLE ALTER before tablespace.tablename
     */
    private static final String PREFIX_TABLE_ALTER = "alter\\W+table\\W+";

    /**
     * Prefix for INDEX CREATE before tablespace.tablename
     */
    private static final String PREFIX_INDEX_CREATE =
            "create\\W+(?:(" + Index.TYPE_HASH + "|" + Index.TYPE_BRIN + ")\\W+)?index\\W+.+\\W+on\\W+";

    /**
     * Prefix for INDEX DROP before tablespace.tablename
     */
    private static final String PREFIX_INDEX_DROP = "drop\\W+index\\W+";

    /**
     * Combines prefixes and add tablespace.tablename groups
     */
    private static final Pattern TABLE_SPACE_NAME_PATTERN = Pattern.compile(
            "(?:" + PREFIX_SELECT
                    + "|" + PREFIX_UPDATE
                    + "|" + PREFIX_INSERT
                    + "|" + PREFIX_DELETE
                    + "|" + PREFIX_TABLE_CREATE
                    + "|" + PREFIX_TABLE_DROP
                    + "|" + PREFIX_TABLE_TRUNCATE
                    + "|" + PREFIX_TABLE_ALTER
                    + "|" + PREFIX_INDEX_CREATE
                    + "|" + PREFIX_INDEX_DROP
                    + ")"
                    + "(?<tablespace>\\S+\\.)?(?<tablename>\\S+)", Pattern.CASE_INSENSITIVE);

    /**
     * Extract a tablespace for a given query. If no tablespace is given on the query provided default will be returned.
     *
     * @param defaultTableSpace default tablespace if no tablespace can be discovered
     * @param query             sql query from which extract a tablespace
     * @return discovered tablespace
     */
    public static String discoverTablespace(String defaultTableSpace, String query) {
        if (query == null) {
            return defaultTableSpace;
        }

        final Matcher matcher = TABLE_SPACE_NAME_PATTERN.matcher(query);

        /* A find will be used, no trim is needed */
        if (matcher.find()) {
            final String tableSpace = matcher.group("tablespace");

            if (tableSpace != null) {
                return tableSpace.substring(0, tableSpace.length() - 1);
            }
        }

        return defaultTableSpace;
    }
}
