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
import java.util.Properties;

/**
 * @author enrico.olivelli
 */
public class Version {

    private static final String VERSION;
    private static final int JDBC_DRIVER_MAJOR_VERSION;
    private static final int JDBC_DRIVER_MINOR_VERSION;

    static {
        Properties pluginProperties = new Properties();
        try (InputStream in = Version.class.getResourceAsStream("/META-INF/herddb.version.properties")) {
            pluginProperties.load(in);
            VERSION = pluginProperties.get("version") + "";
            String[] split = VERSION.split("\\.");
            if (split.length > 2) {
                JDBC_DRIVER_MINOR_VERSION = Integer.parseInt(split[1]);
                JDBC_DRIVER_MAJOR_VERSION = Integer.parseInt(split[0]);
            } else {
                JDBC_DRIVER_MINOR_VERSION = 0;
                JDBC_DRIVER_MAJOR_VERSION = 0;
            }
        } catch (IOException | NumberFormatException err) {
            throw new RuntimeException(err);
        }
    }

    public static String getVERSION() {
        return VERSION;
    }

    public static int getJDBC_DRIVER_MAJOR_VERSION() {
        return JDBC_DRIVER_MAJOR_VERSION;
    }

    public static int getJDBC_DRIVER_MINOR_VERSION() {
        return JDBC_DRIVER_MINOR_VERSION;
    }

}
