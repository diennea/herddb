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

package herddb.security;

import herddb.client.ClientConfiguration;
import herddb.server.ServerConfiguration;

/**
 * Simple User Manager with only an user,
 *
 * @author enrico.olivelli
 */
public class SimpleSingleUserManager extends UserManager {

    private final String adminUsername;
    private final String adminPassword;

    public static final String PROPERTY_ADMIN_USERNAME = "admin.username";
    public static final String PROPERTY_ADMIN_PASSWORD = "admin.password";

    public SimpleSingleUserManager(ServerConfiguration configuration) {
        adminUsername = configuration.getString(PROPERTY_ADMIN_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
        adminPassword = configuration.getString(PROPERTY_ADMIN_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
    }

    @Override
    public String getExpectedPassword(String username) {
        if (adminUsername.equalsIgnoreCase(username)) {
            return adminPassword;
        } else {
            return null;
        }
    }

}
