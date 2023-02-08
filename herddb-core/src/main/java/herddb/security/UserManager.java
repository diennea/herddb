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

import java.io.IOException;
import java.util.Objects;

/**
 * Abstract over user management.
 *
 * @author enrico.olivelli
 */
public abstract class UserManager {

    /**
     * Return the expected password for a given user or null if the user doesn't exist.
     * This method is needed only by the DIGEST-MD5 mechanism.
     * @param username the username
     * @return the password, or null if the user doesn't exist
     */
    public abstract String getExpectedPassword(String username) throws IOException;

    /**
     * Validate a username/password pair.
     *
     * @param username the username
     * @param pwd the password
     * @throws IOException in case of failure
     */
    public void authenticate(String username, char[] pwd) throws IOException {
        String password = pwd != null ? new String(pwd) : null;
        String expectedPassword = getExpectedPassword(username);
        if (expectedPassword == null // user does not exist
                || !Objects.equals(password, expectedPassword)) {
            throw new IOException("Password doesn't match for user " + username + " (or user doesn't exist)");
        }
    }
}
