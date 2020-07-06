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
package herddb.jdbc;

import herddb.server.ServerConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JDBCBoostrapTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        DriverManager.registerDriver(new Driver());
        long _start = System.currentTimeMillis();
        Properties props = new Properties();
        props.put(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toString());
        try (Connection connection = DriverManager.getConnection("jdbc:herddb:local?autoClose=true", props);
                Statement statement = connection.createStatement();) {
            long _boot = System.currentTimeMillis();
            System.out.println("Total time BOOT: " + (_boot - _start) + " ms");
            for (int i = 0; i < 40; i++) {
                String tableName = "tt" + i;
                String indexName = tableName + "ix";
                statement.execute("CREATE TABLE " + tableName + "(k1 string primary key, n1 int not null)");
                statement.execute("CREATE INDEX " + indexName + " ON " + tableName + "(n1)");
            }
        }
        long _end = System.currentTimeMillis();
        System.out.println("Total time OVER: " + (_end - _start) + " ms");

    }
}
