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
package herddb.core.stats;

import java.util.List;

/**
 * Info about actual connections
 *
 * @author enrico.olivelli
 */
public class ConnectionsInfo {

    public List<ConnectionInfo> connections;

    public ConnectionsInfo(List<ConnectionInfo> connections) {
        this.connections = connections;
    }

    public static final class ConnectionInfo {

        public String id;
        public long connectionTs;
        public String username;
        public String address;
        public int numPreparedStatements;

        public ConnectionInfo(String id, long connectionTs, String username, String address, int numPreparedStatements) {
            this.id = id;
            this.connectionTs = connectionTs;
            this.username = username;
            this.address = address;
            this.numPreparedStatements = numPreparedStatements;
        }

    }
}
