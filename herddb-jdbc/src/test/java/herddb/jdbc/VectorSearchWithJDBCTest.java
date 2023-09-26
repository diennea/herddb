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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author enrico.olivelli
 */
public class VectorSearchWithJDBCTest {

    static final String CREATE_TABLE = "CREATE TABLE DOCUMENTS (\n"
            + "  FILENAME string,\n"
            + "  CHUNKID int,  \n"
            + "  TEXT string,  \n"
            + "  EMBEDDINGSVECTOR floata, \n"
            + "  PRIMARY KEY(FILENAME, CHUNKID) \n"
            + ")";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSimpleVectorSearch() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement create = con.createStatement();
                        PreparedStatement statement_insert = con.prepareStatement("INSERT INTO DOCUMENTS(filename,chunkid,text,embeddingsvector) values(?,?,?,?)");
                        PreparedStatement vector_search = con.prepareStatement("SELECT text FROM DOCUMENTS ORDER BY cosine_similarity(embeddingsvector,cast(? as FLOAT ARRAY)) DESC LIMIT 10")) {
                    create.execute(CREATE_TABLE);


                    float[] embeddingsVector = new float[] {1, 2, 3, 4, 5};
                    statement_insert.setString(1, "document.pdf");
                    statement_insert.setInt(2, 1);
                    statement_insert.setString(3, "Lorem ipsum");
                    statement_insert.setObject(4, embeddingsVector);
                    assertEquals(1, statement_insert.executeUpdate());

                    vector_search.setObject(1, embeddingsVector);
                    try (ResultSet rs = vector_search.executeQuery()) {
                        assertTrue(rs.next());
                        String text = rs.getString(1);
                        assertEquals("Lorem ipsum", text);
                    }

                    List<Float>  embeddingVectorAsListOfFloat = Arrays.asList(1f, 2f, 3f, 4f, 5f);
                    vector_search.setObject(1, embeddingVectorAsListOfFloat);
                    try (ResultSet rs = vector_search.executeQuery()) {
                        assertTrue(rs.next());
                        String text = rs.getString(1);
                        assertEquals("Lorem ipsum", text);
                    }

                    List<Double>  embeddingVectorAsListOfDouble = Arrays.asList(1d, 2d, 3d, 4d, 5d);
                    vector_search.setObject(1, embeddingVectorAsListOfDouble);
                    try (ResultSet rs = vector_search.executeQuery()) {
                        assertTrue(rs.next());
                        String text = rs.getString(1);
                        assertEquals("Lorem ipsum", text);
                    }

                }
            }
        }

    }

}
