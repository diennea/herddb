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

import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.client.HDBClient;
import herddb.server.Server;
import java.sql.SQLException;

/**
 * A simple DataSource wrapping around {@link HDBClient} and an optional {@link Server}.
 * <p>
 * It can be used instead of {@link HerdDBEmbeddedDataSource} when a direct configuration of {@link HDBClient} and/or
 * {@link Server} is needed.
 * </p>
 *
 * @author diego.salvi
 */
public class HerdDBWrappingDataSource extends BasicHerdDBDataSource {

    private static final Logger LOGGER = Logger.getLogger(HerdDBWrappingDataSource.class.getName());

    private Server server;

    public HerdDBWrappingDataSource(HDBClient client) {
        super(client);
    }

    public HerdDBWrappingDataSource(HDBClient client, Server server) {
        super(client);

        this.server = server;
    }

    @Override
    protected synchronized void ensureClient() throws SQLException {
        super.ensureClient();
        
        doWaitForTableSpace();
    }

    @Override
    public synchronized void close() {
        super.close();

        if (server != null) {
            try {
                server.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error during server shutdown:" + err, err);
            }
            server = null;
        }
    }

}
