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

package org.herddb.ui;


public class Utils {

    public static final String TS_DEFAULT_PARAM = "defaultts";

    private static String getBoolean(boolean bool) {
        if (bool) {
            return "<span><i class='ti-check' style='color: green'></i></span>";
        } else {
            return "<span><i class='ti-close' style='color: red'></i></span>";
        }
    }

    public static Object formatValue(Object obj) {
        return formatValue(obj, false);
    }

    public static Object formatValue(Object obj, boolean isboolean) {
        if (isboolean) {
            if (obj instanceof Integer) {
                Integer bj = (Integer) obj;
                if (bj == 0) {
                    return getBoolean(false);
                } else if (bj == 1) {
                    return getBoolean(true);
                }
            }
            if (obj instanceof Boolean) {
                Boolean bj = (Boolean) obj;
                return getBoolean(bj);
            }
            if (obj instanceof String) {
                String bj = (String) obj;
                if (bj.equalsIgnoreCase("true")) {
                    return getBoolean(true);
                } else if (bj.equalsIgnoreCase("false")) {
                    return getBoolean(false);
                }
            }
        }
        if (obj instanceof java.sql.Date || obj instanceof java.sql.Timestamp) {
            return obj.toString();
        }
        return obj;
    }

}
