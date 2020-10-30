/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JdbcForeignKeyMetadataTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (Server server = new Server(new ServerConfiguration(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                        Connection con = dataSource.getConnection();
                        Statement statement = con.createStatement()) {
                    statement.execute("CREATE TABLE ptable (pkey string primary key, p1 string, p2 string)");
                    statement.execute("CREATE TABLE ctable (ckey string primary key, c1 string, c2 string,"
                            + "        CONSTRAINT `fk1` FOREIGN KEY (`c1`,`c2`) REFERENCES ptable(`p1`,`p2`) ON DELETE CASCADE)");
                    DatabaseMetaData metaData = con.getMetaData();
                    try (ResultSet rs = metaData.getImportedKeys(null, null, "CTABLE");) {
                        verifyForeignKeyResultSet(rs);
                    }
                    try (ResultSet rs = metaData.getImportedKeys(null, TableSpace.DEFAULT, "CTABLE");) {
                        verifyForeignKeyResultSet(rs);
                    }
                    try (ResultSet rs = metaData.getImportedKeys(null, TableSpace.DEFAULT, "PTABLE");) {
                        assertFalse(rs.next());
                    }
                    try (ResultSet rs = metaData.getExportedKeys(null, null, "ptable");) {
                        verifyForeignKeyResultSet(rs);
                    }
                    try (ResultSet rs = metaData.getExportedKeys(null, TableSpace.DEFAULT, "ptable");) {
                        verifyForeignKeyResultSet(rs);
                    }
                    try (ResultSet rs = metaData.getExportedKeys(null, null, "CTABLE");) {
                        assertFalse(rs.next());
                    }
                    try (ResultSet rs = metaData.getCrossReference(null, TableSpace.DEFAULT, "ptable", null, TableSpace.DEFAULT, "ctable")) {
                        verifyForeignKeyResultSet(rs);
                    }
                    try (ResultSet rs = metaData.getCrossReference(null, TableSpace.DEFAULT, "ptable", null, null, "ctable")) {
                        verifyForeignKeyResultSet(rs);
                    }
                    try (ResultSet rs = metaData.getCrossReference(null, null, "ptable", null, null, "ctable")) {
                        verifyForeignKeyResultSet(rs);
                    }
                }
            }
        }
    }

    private void verifyForeignKeyResultSet(final ResultSet importedKeys) throws SQLException {
        int count = 0;
        while (importedKeys.next()) {
            assertEquals(count + 1, importedKeys.getInt("KEY_SEQ"));
            assertEquals(null, importedKeys.getString("PKTABLE_CAT"));
            assertEquals(TableSpace.DEFAULT, importedKeys.getString("PKTABLE_SCHEM"));
            assertEquals("ctable", importedKeys.getString("PKTABLE_NAME"));

            assertEquals(null, importedKeys.getString("FKTABLE_CAT"));
            assertEquals(TableSpace.DEFAULT, importedKeys.getString("FKTABLE_SCHEM"));
            assertEquals("ptable", importedKeys.getString("FKTABLE_NAME"));

            assertEquals("fk1", importedKeys.getString("FK_NAME"));
            assertEquals(null, importedKeys.getString("PK_NAME"));
            assertEquals(DatabaseMetaData.importedKeyNotDeferrable, importedKeys.getInt("DEFERRABILITY"));
            if (count == 0) {
                assertEquals("c1", importedKeys.getString("PKCOLUMN_NAME"));
                assertEquals("p1", importedKeys.getString("FKCOLUMN_NAME"));
            } else {
                assertEquals("c2", importedKeys.getString("PKCOLUMN_NAME"));
                assertEquals("p2", importedKeys.getString("FKCOLUMN_NAME"));
            }
            assertEquals(DatabaseMetaData.importedKeyNoAction, importedKeys.getInt("UPDATE_RULE"));
            assertEquals(DatabaseMetaData.importedKeyCascade, importedKeys.getInt("DELETE_RULE"));
            count++;
        }
        assertEquals(2, count);
    }
}
