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
package herddb.cli;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author enrico.olivelli
 */
public class MySqlDumpInsertStatementRewriterTest {

    @Test
    public void testRewriteSimpleInsertStatement() throws Exception {

        check("INSERT INTO table VALUES ('a',1)", "INSERT INTO table VALUES (?,?)", Arrays.asList("a", Long.valueOf(1)));
        check("INSERT INTO table VALUES ('a')", "INSERT INTO table VALUES (?)", Arrays.asList("a"));
        check("INSERT INTO table VALUES ('a','b')", "INSERT INTO table VALUES (?,?)", Arrays.asList("a", "b"));
        check("INSERT INTO table VALUES (1,2)", "INSERT INTO table VALUES (?,?)", Arrays.asList(Long.valueOf(1), Long.valueOf(2)));
        check("INSERT INTO table VALUES (1,2,null)", "INSERT INTO table VALUES (?,?,?)", Arrays.asList(Long.valueOf(1), Long.valueOf(2), null));
        check("INSERT INTO table VALUES (null,2,1)", "INSERT INTO table VALUES (?,?,?)", Arrays.asList(null, Long.valueOf(2), Long.valueOf(1)));
        check("INSERT INTO table VALUES (null)", "INSERT INTO table VALUES (?)", Collections.singletonList(null));
        check("INSERT INTO table VALUES (a,2)", null, null);

        check("INSERT INTO table VALUES ('a',1),('b','c')", "INSERT INTO table VALUES (?,?),(?,?)", Arrays.asList("a", Long.valueOf(1),
            "b", "c"));
        check("INSERT INTO table VALUES ('a',null),('b','c')", "INSERT INTO table VALUES (?,?),(?,?)", Arrays.asList("a", null,
            "b", "c"));
    }

    private static void check(String original, String rewritten, List<Object> expectedParameters) {
        QueryWithParameters result = MySqlDumpInsertStatementRewriter.rewriteSimpleInsertStatement(original);
        if (rewritten == null) {
            assertNull(result);
            return;
        }
        assertNotNull(result);
        assertEquals(rewritten, result.query);
        assertEquals(expectedParameters, result.jdbcParameters);
    }

}
