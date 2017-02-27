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
package herddb.jmx;

import herddb.core.HerdDBInternalException;
import java.lang.management.ManagementFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 * Utility for MBeans registration
 *
 */
public class JMXUtils {

    private static final Logger LOG = Logger.getLogger(JMXUtils.class.getName());
    private static MBeanServer platformMBeanServer;
    private static Throwable mBeanServerLookupError;

    static {
        try {
            platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        } catch (Exception err) {
            mBeanServerLookupError = err;
            err.printStackTrace();
            platformMBeanServer = null;
        }
    }

    private static String safeName(String s) {
        return s.replaceAll("[?,:=\\W]", ".");
    }

    public static MBeanServer getMBeanServer() {
        return platformMBeanServer;
    }

    public static void registerTableManagerStatsMXBean(String tableSpaceName, String tableName, TableManagerStatsMXBean bean) {
        if (platformMBeanServer == null) {
            throw new HerdDBInternalException("PlatformMBeanServer not available", mBeanServerLookupError);
        }
        String safeTableSpaceName = safeName(tableSpaceName);
        String safeTableName = safeName(tableName);

        try {
            ObjectName name = new ObjectName("herddb.server:type=Table,Name=" + safeTableSpaceName + "." + safeTableName);
            LOG.log(Level.FINE, "Publishing stats for table {0}.{1} at {2}", new Object[]{tableSpaceName, tableName, name});
            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
            platformMBeanServer.registerMBean(bean, name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            throw new HerdDBInternalException("Could not register MXBean " + e);
        }
    }

    public static void unregisterTableManagerStatsMXBean(String tableSpaceName, String tableName) {
        if (platformMBeanServer == null) {
            return;
        }
        String safeTableSpaceName = safeName(tableSpaceName);
        String safeTableName = safeName(tableName);

        try {
            ObjectName name = new ObjectName("herddb.server:type=Table,Name=" + safeTableSpaceName + "." + safeTableName);
            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            throw new HerdDBInternalException("Could not unregister MXBean " + e);
        }
    }

    public static void registerTableSpaceManagerStatsMXBean(String tableSpaceName, TableSpaceManagerStatsMXBean bean) {
        if (platformMBeanServer == null) {
            throw new HerdDBInternalException("PlatformMBeanServer not available", mBeanServerLookupError);
        }
        String safeTableSpaceName = safeName(tableSpaceName);

        try {
            ObjectName name = new ObjectName("herddb.server:type=TableSpace,Name=" + safeTableSpaceName);
            LOG.log(Level.FINE, "Publishing stats for table {0} at {1}", new Object[]{tableSpaceName, name});
            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
            platformMBeanServer.registerMBean(bean, name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            throw new HerdDBInternalException("Could not register MXBean " + e);
        }
    }

    public static void unregisterTableSpaceManagerStatsMXBean(String tableSpaceName) {
        if (platformMBeanServer == null) {
            return;
        }
        String safeTableSpaceName = safeName(tableSpaceName);

        try {
            ObjectName name = new ObjectName("herddb.server:type=TableSpace,Name=" + safeTableSpaceName);
            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            throw new HerdDBInternalException("Could not unregister MXBean " + e);
        }
    }

}
