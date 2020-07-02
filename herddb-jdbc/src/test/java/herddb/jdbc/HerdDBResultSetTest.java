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

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HerdDBResultSetTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void getStatement() throws Exception {
        try (final Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();

            try (final HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                try (final BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client)) {

                    try (Connection con = dataSource.getConnection();
                         Statement statement = con.createStatement()) {
                        statement.execute("CREATE TABLE mytable (c1 int primary key)");
                    }

                    try (final Connection con = dataSource.getConnection();
                         final PreparedStatement statement = con.prepareStatement("select * from mytable");
                         final ResultSet rows = statement.executeQuery()) {

                        Assert.assertEquals(statement, rows.getStatement());
                    }

                }
            }
        }
    }
}
