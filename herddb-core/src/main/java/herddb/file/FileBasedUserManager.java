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
package herddb.file;

import herddb.security.UserManager;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple User Manager which stores user data on disk
 *
 * @author enrico.olivelli
 */
public class FileBasedUserManager extends UserManager {

    private Map<String, String> users = new HashMap<>();
    private static final Logger LOG = Logger.getLogger(FileBasedUserManager.class.getName());

    public FileBasedUserManager(Path file) throws IOException {
        Files.readAllLines(file, StandardCharsets.UTF_8).forEach(l -> {
            l = l.trim();
            if (l.startsWith("#") || l.isEmpty()) {
                return;
            }
            // username,password,role
            String[] split = l.split(",");
            if (split.length == 3) {
                String role = split[2];
                if (role.equalsIgnoreCase("admin")) {
                    users.put(split[0], split[1]);
                    LOG.log(Level.SEVERE, "Configure user " + split[0] + " with role " + split[2]);
                } else {
                   LOG.log(Level.SEVERE, "Skipped user " + split[0] + " with role " + split[2]+". bad role. Only 'admin' is available");   
                }
            }
        }
        );
    }

    @Override
    public String getExpectedPassword(String username) {
        return users.get(username);
    }

}
