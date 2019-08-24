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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests.
 */
public class ServerSidePreparedStatementCacheTest {

    @Test
    public void testBasicFunctions() {
        String tableSpace = "test";
        String text = "select * from table";
        ServerSidePreparedStatementCache instance = new ServerSidePreparedStatementCache(Long.MAX_VALUE);
        long id = instance.prepare(tableSpace, text);
        assertEquals(text, instance.resolveQuery(tableSpace, id));
        assertEquals(1, instance.getSize());
        assertEquals(null, instance.resolveQuery("wrong_ts", id));
        instance.clear();
        assertEquals(null, instance.resolveQuery(tableSpace, id));
    }

    @Test
    public void testEviction() {
        String tableSpace = "test";
        String text = "select * from table";
        ServerSidePreparedStatementCache instance =
                new ServerSidePreparedStatementCache((text.length() + tableSpace.length()) * 10);
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            long id = instance.prepare(tableSpace, text + " WHERE a=" + i);
            ids.add(id);
        }
        boolean oneEvicted = false;
        for (Long id : ids) {
            if (instance.resolveQuery(tableSpace, id) == null) {
                oneEvicted = true;
            }
        }
        assertTrue(oneEvicted);
    }

}
