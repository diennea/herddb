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

package herddb.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

/**
 * Tests the JDBC url parser of {@link ServerConfiguration}, in particular the database id of the
 * {@code jdbc:herddb:local:<databaseId>} form: the id identifies the in-JVM database, so two urls
 * carrying different ids must never resolve to the same database.
 *
 * @author diego.salvi
 */
public class ServerConfigurationJdbcUrlTest {

    private static ServerConfiguration parse(String url) {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.readJdbcUrl(url);
        return configuration;
    }

    /**
     * Resolves the database id of a local url, that is the value the parser stores as host.
     */
    private static String localDatabaseId(String url) {
        ServerConfiguration configuration = parse(url);
        assertEquals(ServerConfiguration.PROPERTY_MODE_LOCAL,
                configuration.getString(ServerConfiguration.PROPERTY_MODE, ""));
        return configuration.getString(ServerConfiguration.PROPERTY_HOST,
                ServerConfiguration.PROPERTY_HOST_DEFAULT);
    }

    @Test
    public void testLocalDatabaseIdIsResolvedVerbatim() {
        assertEquals("db1", localDatabaseId("jdbc:herddb:local:db1"));
        assertEquals("mydatabase", localDatabaseId("jdbc:herddb:local:mydatabase"));

        ServerConfiguration configuration = parse("jdbc:herddb:local:db1");
        assertEquals(0, configuration.getInt(ServerConfiguration.PROPERTY_PORT, -1));
        assertEquals("db1", configuration.getString(ServerConfiguration.PROPERTY_NODEID, ""));
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

        assertNotEquals(parse("jdbc:herddb:local:ab").getString(ServerConfiguration.PROPERTY_NODEID, ""),
                parse("jdbc:herddb:local:cb").getString(ServerConfiguration.PROPERTY_NODEID, ""));
    }

    @Test
    public void testSingleCharacterLocalDatabaseId() {
        assertEquals("x", localDatabaseId("jdbc:herddb:local:x"));
        assertEquals("y", localDatabaseId("jdbc:herddb:local:y"));
        assertNotEquals(localDatabaseId("jdbc:herddb:local:x"), localDatabaseId("jdbc:herddb:local:y"));

        ServerConfiguration configuration = parse("jdbc:herddb:local:x");
        assertEquals(0, configuration.getInt(ServerConfiguration.PROPERTY_PORT, -1));
        assertEquals("x", configuration.getString(ServerConfiguration.PROPERTY_NODEID, ""));
    }

    @Test
    public void testLocalWithoutDatabaseIdKeepsDefaults() {
        assertEquals(ServerConfiguration.PROPERTY_HOST_DEFAULT, localDatabaseId("jdbc:herddb:local"));

        ServerConfiguration configuration = parse("jdbc:herddb:local");
        assertEquals(ServerConfiguration.PROPERTY_PORT_DEFAULT,
                configuration.getInt(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_DEFAULT));
        assertEquals("local", configuration.getString(ServerConfiguration.PROPERTY_NODEID, ""));
    }

    @Test
    public void testLocalWithEmptyDatabaseIdKeepsDefaults() {
        assertEquals(ServerConfiguration.PROPERTY_HOST_DEFAULT, localDatabaseId("jdbc:herddb:local:"));
        assertEquals("local", parse("jdbc:herddb:local:").getString(ServerConfiguration.PROPERTY_NODEID, ""));
    }

    @Test
    public void testLocalDatabaseIdIsTrimmedAndLowerCased() {
        assertEquals("mydb", localDatabaseId("jdbc:herddb:local:MyDb "));
        assertEquals("mydb", localDatabaseId("jdbc:herddb:local: mydb "));
        assertEquals("mydb", localDatabaseId("jdbc:herddb:local:MYDB"));
    }

    @Test
    public void testLocalDatabaseIdWithQueryString() {
        ServerConfiguration configuration = parse("jdbc:herddb:local:db1?server.base.dir=/tmp/herddbtest");
        assertEquals("db1", configuration.getString(ServerConfiguration.PROPERTY_HOST,
                ServerConfiguration.PROPERTY_HOST_DEFAULT));
        assertEquals("/tmp/herddbtest", configuration.getString(ServerConfiguration.PROPERTY_BASEDIR,
                ServerConfiguration.PROPERTY_BASEDIR_DEFAULT));
    }

    @Test
    public void testServerUrl() {
        ServerConfiguration withPort = parse("jdbc:herddb:server:myhost:1234");
        assertEquals(ServerConfiguration.PROPERTY_MODE_STANDALONE,
                withPort.getString(ServerConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost", withPort.getString(ServerConfiguration.PROPERTY_HOST, ""));
        assertEquals(1234, withPort.getInt(ServerConfiguration.PROPERTY_PORT, -1));

        ServerConfiguration withoutPort = parse("jdbc:herddb:server:myhost");
        assertEquals(ServerConfiguration.PROPERTY_MODE_STANDALONE,
                withoutPort.getString(ServerConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost", withoutPort.getString(ServerConfiguration.PROPERTY_HOST, ""));
        assertEquals(ServerConfiguration.PROPERTY_PORT_DEFAULT,
                withoutPort.getInt(ServerConfiguration.PROPERTY_PORT, -1));
    }

    @Test
    public void testZookeeperUrl() {
        ServerConfiguration withPath = parse("jdbc:herddb:zookeeper:myhost:2181/herdtest");
        assertEquals(ServerConfiguration.PROPERTY_MODE_CLUSTER,
                withPath.getString(ServerConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost:2181", withPath.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ""));
        assertEquals("/herdtest", withPath.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, ""));

        ServerConfiguration withoutPath = parse("jdbc:herddb:zookeeper:myhost:2181");
        assertEquals(ServerConfiguration.PROPERTY_MODE_CLUSTER,
                withoutPath.getString(ServerConfiguration.PROPERTY_MODE, ""));
        assertEquals("myhost:2181", withoutPath.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ""));
        assertEquals(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT,
                withoutPath.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH,
                        ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT));
    }
}
