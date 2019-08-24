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

package herddb.benchs.simple;

import herddb.benchs.BaseBench;
import herddb.benchs.InsertOperation;
import java.io.IOException;
import org.junit.Test;

/**
 * Simple concurrent reads and writes on a single table
 *
 * @author enrico.olivelli
 */
public class HugeTableTest extends BaseBench {

    public HugeTableTest() {
        super(20,
                1000000,
                100000,
                2);
        addOperation(new InsertOperation());
    }

    @Override
    protected void makeServerConfiguration() throws IOException {
        super.makeServerConfiguration();
        // we want to swap-in/swap-out pages
        // serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_TABLE_USED_MEMORY, 64 * 1024 * 1024);
        // serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 64 * 1024 * 512);

    }

    @Test
    public void run() throws Exception {
        generateData();
        performOperations();
        waitForResults();
        restartServer();
    }

}
