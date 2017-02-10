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

import herddb.utils.RawString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Simple tests
 *
 * @author enrico.olivelli
 */
public class SimpleSQLRecordPredicateTest {

    @Test
    public void testCompareAndLike() throws Exception {
        assertTrue(SQLRecordPredicate.like("test", "%est"));
        assertTrue(SQLRecordPredicate.like("test", "test%"));
        assertFalse(SQLRecordPredicate.like("test", "a%"));
        assertTrue(SQLRecordPredicate.like("test", "%test%"));
        assertTrue(SQLRecordPredicate.like("test", "%es%"));
        assertFalse(SQLRecordPredicate.like("tesst", "te_t"));
        assertTrue(SQLRecordPredicate.like("test", "te_t"));
        assertTrue(SQLRecordPredicate.compare(1, 2) < 0);
        assertTrue(SQLRecordPredicate.compare(1, 1) == 0);
        assertTrue(SQLRecordPredicate.compare(2, 1) > 0);
        assertTrue(SQLRecordPredicate.compare(1L, 2L) < 0);
        assertTrue(SQLRecordPredicate.compare(1L, 1L) == 0);
        assertTrue(SQLRecordPredicate.compare(2L, 1L) > 0);
        assertTrue(SQLRecordPredicate.compare(1d, 2L) < 0);
        assertTrue(SQLRecordPredicate.compare(1L, 1f) == 0);
        assertTrue(SQLRecordPredicate.compare(2, 1f) > 0);

        assertTrue(SQLRecordPredicate.compare("a", RawString.of("a")) == 0);
        assertTrue(SQLRecordPredicate.compare("a", RawString.of("b")) < 0);
        assertTrue(SQLRecordPredicate.compare("c", RawString.of("a")) > 0);

        assertTrue(SQLRecordPredicate.compare(RawString.of("a"), "a") == 0);
        assertTrue(SQLRecordPredicate.compare(RawString.of("a"), "b") < 0);
        assertTrue(SQLRecordPredicate.compare(RawString.of("c"), "a") > 0);

    }

}
