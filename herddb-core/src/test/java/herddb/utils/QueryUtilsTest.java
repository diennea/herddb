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

package herddb.utils;

import static org.junit.Assert.assertEquals;
import herddb.model.TableSpace;
import org.junit.Test;

/**
 * Test cases on QueryUtils
 *
 * @author enrico.olivelli
 */
public class QueryUtilsTest {

    @Test
    public void testDiscoverTablespaceFromSelect() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "select * from test"));
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, ""));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "select a from myts.test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "select a.b from myts.test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "select a.b from myts.test where foo='bar'"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "select * from myts.test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "SELECT COUNT(M.id) FROM myts.a M WHERE 1=1 AND (M.id IN (SELECT H.id FROM myts.b AS H WHERE H.FIELD_NAME='From' AND H.FIELD_VALUE LIKE 'aaa'))"));
    }

    @Test
    public void testDiscoverTablespaceFromUpdate() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "update test"));
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, ""));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "update myts.test set a='b'"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "update myts.test"));
    }

    @Test
    public void testDiscoverTablespaceFromInsert() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "insert into test(a,b) values(?,?,?)"));
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "insert into test values(?,?,?)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "insert into myts.test(a,b) values(?,?,?,?)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "insert into myts.test values(?,?,?,?)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "INSERT INTO myts.a SELECT * FROM myts.c M WHERE 1=1 AND (M.id IN (SELECT H.id FROM myts.b AS H WHERE H.FIELD_NAME='Into' AND H.FIELD_VALUE LIKE 'aaa'))"));
    }
 
    @Test
    public void testDiscoverTablespaceFromUpsert() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "upsert into test(a,b) values(?,?,?)"));
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "upsert into test values(?,?,?)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "upsert into myts.test(a,b) values(?,?,?,?)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "upsert into myts.test values(?,?,?,?)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "UPSERT INTO myts.a SELECT * FROM myts.c M WHERE 1=1 AND (M.id IN (SELECT H.id FROM myts.b AS H WHERE H.FIELD_NAME='Into' AND H.FIELD_VALUE LIKE 'aaa'))"));
    }

    @Test
    public void testDiscoverTablespaceFromDelete() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "delete * from test"));
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, ""));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "delete a from myts.test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "delete a.b from myts.test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "delete a.b from myts.test where foo='bar'"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "delete * from myts.test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "DELTE FROM myts.a M WHERE 1=1 AND (M.id IN (SELECT H.id FROM myts.b AS H WHERE H.FIELD_NAME='From' AND H.FIELD_VALUE LIKE 'aaa'))"));
    }

    @Test
    public void testDiscoverTablespaceFromCreateTable() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "create table test(a,b)"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "create table myts.test(a,b)"));
    }

    @Test
    public void testDiscoverTablespaceFromDropTable() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "drop table test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "drop table myts.test"));
    }

    @Test
    public void testDiscoverTablespaceFromTruncateTable() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "truncate table test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "truncate table myts.test"));
    }

    @Test
    public void testDiscoverTablespaceFromAlterTable() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "alter table test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "alter table myts.test"));
    }

    @Test
    public void testDiscoverTablespaceFromCreateIndex() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "create index test on test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "create index test on myts.test"));

        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "create brin index test on test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "create brin index test on myts.test"));
    }

    @Test
    public void testDiscoverTablespaceFromDropIndex() {
        String defaultTableSpace = TableSpace.DEFAULT;
        String theTableSpace = "myts";
        assertEquals(defaultTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "drop index test"));
        assertEquals(theTableSpace, QueryUtils.discoverTablespace(defaultTableSpace, "drop index myts.test"));
    }

}
