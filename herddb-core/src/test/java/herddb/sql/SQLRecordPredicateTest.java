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
package herddb.sql;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author enrico.olivelli
 */
public class SQLRecordPredicateTest {

    @Test
    public void testLike() throws Exception {
        assertTrue(SQLRecordPredicate.like("test", "%est"));
        assertTrue(SQLRecordPredicate.like("test", "test%"));
        assertFalse(SQLRecordPredicate.like("test", "a%"));
        assertTrue(SQLRecordPredicate.like("test", "%test%"));
        assertTrue(SQLRecordPredicate.like("test", "%es%"));
        assertFalse(SQLRecordPredicate.like("tesst", "te_t"));
        assertTrue(SQLRecordPredicate.like("test", "te_t"));
    }

}
