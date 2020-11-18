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

package herddb.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the identity of the local node
 *
 * @author enrico.olivelli
 */
public class LocalNodeIdManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNodeIdManager.class.getName());

    private final Path dataPath;

    public LocalNodeIdManager(Path dataPath) {
        this.dataPath = dataPath;
    }

    public String readLocalNodeId() throws IOException {
        Path file = dataPath.resolve("nodeid");
        try {
            LOG.info("Looking for local node id into file {}", file);
            if (!Files.isRegularFile(file)) {
                final String fallback = System.getProperty("herddb.local.nodeid");
                if (fallback == null) {
                    LOG.error("Cannot find file {}", file);
                }
                return fallback;
            }
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            for (String line : lines) {
                line = line.trim().toLowerCase();
                // skip comments and empty lines
                if (line.startsWith("#") || line.isEmpty()) {
                    continue;
                }
                return line;
            }
            throw new IOException("Cannot find any valid line inside file " + file.toAbsolutePath());
        } catch (IOException error) {
            LOG.error("Error while reading file " + file.toAbsolutePath(), error);
            throw error;
        }
    }

    public void persistLocalNodeId(String nodeId) throws IOException {
        Files.createDirectories(dataPath);
        Path file = dataPath.resolve("nodeid");
        StringBuilder message = new StringBuilder();
        message.append("# This file contains the id of this node\n");
        message.append("# Do not change the contents of this file, otherwise the beheaviour of the system will\n");
        message.append("# lead eventually to data loss\n");
        message.append("# \n");
        message.append("# Any line which starts with '#' and and blank line will be ignored\n");
        message.append("# The system will consider the first non-blank line as node id, making it lowercase\n");
        message.append("\n\n");
        message.append(nodeId);
        Files.write(file, message.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
    }

}
