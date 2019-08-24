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

import static org.junit.Assert.assertEquals;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
import org.junit.Test;

public class TableSpaceMapperTest {

    @Test
    public void testGetTableSpace() throws Exception {
        String script = "if (tableName.startsWith('test')) return 'ns1' else return 'ns2';";
        TableSpaceMapper mapper = new TableSpaceMapper(script);
        {
            Insert insert = new Insert();
            Table table = new Table("test1");
            insert.setTable(table);
            assertEquals("ns1", mapper.getTableSpace(insert));
        }
        {
            Insert insert = new Insert();
            Table table = new Table("notest1");
            insert.setTable(table);
            assertEquals("ns2", mapper.getTableSpace(insert));
        }

    }

}
