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

import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.model.TableSpace;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test some tricky aspected of the static client side metadata provider
 *
 * @author enrico.olivelli
 */
public class CreateTableSpaceWithStaticMetadataProviderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {

        try (HerdDBEmbeddedDataSource dataSource = new HerdDBEmbeddedDataSource()) {
            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ClientConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.setMaxActive(20);
            try (Connection con = dataSource.getConnection()) {
            }
            assertTrue(dataSource.getClient().getClientSideMetadataProvider() instanceof StaticClientSideMetadataProvider);

            try (Connection con = dataSource.getConnection();
                 Statement statement = con.createStatement()) {
                String leader = dataSource.getClient().getClientSideMetadataProvider().getTableSpaceLeader(TableSpace.DEFAULT);
                statement.execute("CREATE TABLESPACE 'mytablespace','wait:5000','leader:" + leader + "'");
                statement.execute("CREATE TABLE mytablespace.mytable(pk int primary key)");
            }
        }
    }

    @Test
    public void testStandAloneServer() throws Exception {

        try (HerdDBEmbeddedDataSource dataSource = new HerdDBEmbeddedDataSource()) {
            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ClientConfiguration.PROPERTY_BASEDIR, folder.newFolder().getAbsolutePath());
            dataSource.getProperties().setProperty(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_STANDALONE);
            dataSource.getProperties().setProperty(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
            dataSource.setStartServer(true);
            dataSource.setMaxActive(20);
            try (Connection con = dataSource.getConnection()) {
            }
            assertTrue(dataSource.getClient().getClientSideMetadataProvider() instanceof StaticClientSideMetadataProvider);

            try (Connection con = dataSource.getConnection();
                 Statement statement = con.createStatement()) {
                String leader = dataSource.getClient().getClientSideMetadataProvider().getTableSpaceLeader(TableSpace.DEFAULT);
                System.out.println("leader:" + leader);
                statement.execute("CREATE TABLESPACE 'mytablespace','wait:5000','leader:" + leader + "'");
                statement.execute("CREATE TABLE mytablespace.mytable(pk int primary key)");
            }
        }
    }
}
