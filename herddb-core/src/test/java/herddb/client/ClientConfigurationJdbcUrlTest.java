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

package herddb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

/**
 * Tests the JDBC url parser of {@link ClientConfiguration}, in particular the database id of the
 * {@code jdbc:herddb:local:<databaseId>} form: the id identifies the in-JVM database, so two urls
 * carrying different ids must never resolve to the same database.
 *
 * @author diego.salvi
 */
public class ClientConfigurationJdbcUrlTest {

    private static ClientConfiguration parse(String url) {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.readJdbcUrl(url);
        return configuration;
    }

    /**
     * Resolves the database id of a local url, that is the value the parser stores as server address.
     */
    private static String localDatabaseId(String url) {
        ClientConfiguration configuration = parse(url);
        assertEquals(ClientConfiguration.PROPERTY_MODE_LOCAL,
                configuration.getString(ClientConfiguration.PROPERTY_MODE, ""));
        return configuration.getString(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                ClientConfiguration.PROPERTY_SERVER_ADDRESS_DEFAULT);
    }

    @Test
    public void testLocalDatabaseIdIsResolvedVerbatim() {
        assertEquals("db1", localDatabaseId("jdbc:herddb:local:db1"));
        assertEquals("mydatabase", localDatabaseId("jdbc:herddb:local:mydatabase"));
        assertEquals(0, parse("jdbc:herddb:local:db1").getInt(ClientConfiguration.PROPERTY_SERVER_PORT, -1));
    }

    @Test
    public void testLocalDatabaseIdsDifferingOnlyInTheFirstCharacterStayDistinct() {
        assertEquals("ab", localDatabaseId("jdbc:herddb:local:ab"));
        assertEquals("cb", localDatabaseId("jdbc:herddb:local:cb"));
        assertNotEquals(localDatabaseId("jdbc:herddb:local:ab"), localDatabaseId("jdbc:herddb:local:cb"));

        assertEquals("db1", localDatabaseId("jdbc:herddb:local:db1"));
        assertEquals("xdb1", localDatabaseId("jdbc:herddb:local:xdb1"));
        assertNotEquals(localDatabaseId("jdbc:herddb:local:db1"), localDatabaseId("jdbc:herddb:local:xdb1"));

        assertEquals("prod", localDatabaseId("jdbc:herddb:local:prod"));
        assertEquals("zprod", localDatabaseId("jdbc:herddb:local:zprod"));
        assertNotEquals(localDatabaseId("jdbc:herddb:local:prod"), localDatabaseId("jdbc:herddb:local:zprod"));
    }

    @Test
    public void testSingleCharacterLocalDatabaseId() {
        assertEquals("x", localDatabaseId("jdbc:herddb:local:x"));
        assertEquals("y", localDatabaseId("jdbc:herddb:local:y"));
        assertNotEquals(localDatabaseId("jdbc:herddb:local:x"), localDatabaseId("jdbc:herddb:local:y"));
        assertEquals(0, parse("jdbc:herddb:local:x").getInt(ClientConfiguration.PROPERTY_SERVER_PORT, -1));
    }

    @Test
    public void testLocalWithoutDatabaseIdKeepsDefaults() {
        assertEquals(ClientConfiguration.PROPERTY_SERVER_ADDRESS_DEFAULT, localDatabaseId("jdbc:herddb:local"));
        assertEquals(ClientConfiguration.PROPERTY_SERVER_PORT_DEFAULT,
                parse("jdbc:herddb:local").getInt(ClientConfiguration.PROPERTY_SERVER_PORT,
                        ClientConfiguration.PROPERTY_SERVER_PORT_DEFAULT));
    }

    @Test
    public void testLocalWithEmptyDatabaseIdKeepsDefaults() {
        assertEquals(ClientConfiguration.PROPERTY_SERVER_ADDRESS_DEFAULT, localDatabaseId("jdbc:herddb:local:"));
    }

    @Test
    public void testLocalDatabaseIdIsTrimmedAndLowerCased() {
        assertEquals("mydb", localDatabaseId("jdbc:herddb:local:MyDb "));
        assertEquals("mydb", localDatabaseId("jdbc:herddb:local: mydb "));
        assertEquals("mydb", localDatabaseId("jdbc:herddb:local:MYDB"));
    }

    @Test
    public void testLocalDatabaseIdWithQueryString() {
        ClientConfiguration configuration = parse("jdbc:herddb:local:db1?client.timeout=1234");
        assertEquals("db1", configuration.getString(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                ClientConfiguration.PROPERTY_SERVER_ADDRESS_DEFAULT));
        assertEquals(1234, configuration.getInt(ClientConfiguration.PROPERTY_TIMEOUT,
                ClientConfiguration.PROPERTY_TIMEOUT_DEFAULT));
    }

    @Test
    public void testServerUrl() {
        ClientConfiguration withPort = parse("jdbc:herddb:server:myhost:1234");
        assertEquals(ClientConfiguration.PROPERTY_MODE_STANDALONE,
                withPort.getString(ClientConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost", withPort.getString(ClientConfiguration.PROPERTY_SERVER_ADDRESS, ""));
        assertEquals(1234, withPort.getInt(ClientConfiguration.PROPERTY_SERVER_PORT, -1));

        ClientConfiguration withoutPort = parse("jdbc:herddb:server:myhost");
        assertEquals(ClientConfiguration.PROPERTY_MODE_STANDALONE,
                withoutPort.getString(ClientConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost", withoutPort.getString(ClientConfiguration.PROPERTY_SERVER_ADDRESS, ""));
        assertEquals(ClientConfiguration.PROPERTY_SERVER_PORT_DEFAULT,
                withoutPort.getInt(ClientConfiguration.PROPERTY_SERVER_PORT, -1));
    }

    @Test
    public void testZookeeperUrl() {
        ClientConfiguration withPath = parse("jdbc:herddb:zookeeper:myhost:2181/herdtest");
        assertEquals(ClientConfiguration.PROPERTY_MODE_CLUSTER,
                withPath.getString(ClientConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost:2181", withPath.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ""));
        assertEquals("/herdtest", withPath.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, ""));

        ClientConfiguration withoutPath = parse("jdbc:herddb:zookeeper:myhost:2181");
        assertEquals(ClientConfiguration.PROPERTY_MODE_CLUSTER,
                withoutPath.getString(ClientConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost:2181", withoutPath.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ""));
        assertEquals(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT,
                withoutPath.getString(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH,
                        ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT));
    }
}
