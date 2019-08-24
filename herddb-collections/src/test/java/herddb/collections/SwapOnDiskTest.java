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

package herddb.collections;

import static org.junit.Assert.assertTrue;
import herddb.server.ServerConfiguration;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Simple tests around TmpMaps
 */
public class SwapOnDiskTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testIntMap() throws Exception {
        Properties props = new Properties();
        props.put(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 2 * 1024 * 1024);
        props.put(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 10 * 1024 * 1024);
        try (CollectionsManager manager = CollectionsManager
                .builder()
                .tmpDirectory(tmpDir.newFolder().toPath())
                .configuration(props)
                .build()) {
            manager.start();
            try (TmpMap<Integer, String> tmpMap = manager
                    .newMap()
                    .withIntKeys()
                    .build()) {
                for (int i = 0; i < 10_00_000; i++) {
                    tmpMap.put(i, "foofffffffffffffffffffffffffffffffffff" + i);
                }
                assertTrue(tmpMap.isSwapped());
            }
        }
    }

}
