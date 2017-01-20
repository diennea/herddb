/*
 * Copyright 2017 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.benchs.simple;

import herddb.benchs.BaseBench;
import herddb.benchs.UpdateByPKOperation;
import org.junit.Test;

/**
 * Simple concurrent reads and writes on a single table
 *
 * @author enrico.olivelli
 */
public class ConcurrentUpdatesTest extends BaseBench {

    public ConcurrentUpdatesTest() {
        super(20,
            100000,
            10000,
            2);
        addOperation(new UpdateByPKOperation());
    }

    @Test
    public void run() throws Exception {
        generateData();
        performOperations();
        waitForResults();
        restartServer();
    }

}
