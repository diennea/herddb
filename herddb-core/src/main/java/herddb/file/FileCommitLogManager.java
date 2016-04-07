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

import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Commit logs on local files
 * @author enrico.olivelli
 */
public class FileCommitLogManager extends CommitLogManager {

    private final Path baseDirectory;

    public FileCommitLogManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    @Override
    public CommitLog createCommitLog(String tableSpace) {
        try {
            Path folder = baseDirectory.resolve(tableSpace + ".txlog");
            Files.createDirectories(folder);
            return new FileCommitLog(folder, 1024 * 1024);
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }
}
