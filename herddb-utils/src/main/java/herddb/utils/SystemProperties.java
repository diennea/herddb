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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author enrico.olivelli
 */
public class SystemProperties {

    private static final Logger LOGGER = Logger.getLogger(SystemProperties.class.getName());

    public static String getStringSystemProperty(String name, String defaultvalue) {
        return getProperty(name, defaultvalue);
    }

    public static int getIntSystemProperty(String name, int defaultvalue) {
        return getIntSystemProperty(name, defaultvalue, "");
    }

    public static int getIntSystemProperty(String name, int defaultvalue, String description) {
        String value = getProperty(name, defaultvalue + "");
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException err) {
            RuntimeException rerr = new RuntimeException("Error reading system property " + name + " =" + value, err);
            throw rerr;
        }
    }

    public static long getLongSystemProperty(String name, long defaultvalue) {
        String value = getProperty(name, defaultvalue + "");
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException err) {
            RuntimeException rerr = new RuntimeException("Error reading system property " + name + " =" + value, err);
            throw rerr;
        }
    }

    public static boolean getBooleanSystemProperty(String name, boolean defaultvalue) {
        String value = getProperty(name, defaultvalue + "");
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        } else {
            RuntimeException rerr = new RuntimeException("Error reading system property "
                    + name + " =" + value + " allowed only true|false");
            throw rerr;
        }
    }

    private static String getProperty(String name, String defaultvalue) {
        String res = AccessController.doPrivileged((PrivilegedAction<String>) (() -> {
            return System.getProperty(name, defaultvalue);
        }));
        LOGGER.log(Level.CONFIG, "read system property: {0}={1}", new Object[] {name, res});
        return res;
    }
}
