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

import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scanner on the server-side
 *
 * @author enrico.olivelli
 */
public class ServerSideScannerPeer implements AutoCloseable {

    private final DataScanner scanner;

    public ServerSideScannerPeer(DataScanner scanner) {
        this.scanner = scanner;
    }

    public DataScanner getScanner() {
        return scanner;
    }

    public void clientClose() {
        try {
            scanner.close();
        } catch (DataScannerException ex) {
            LOG.log(Level.SEVERE, "error con closing scanner " + ex, ex);
        }
    }

    private static final Logger LOG = Logger.getLogger(ServerSideScannerPeer.class.getName());

    @Override
    public void close() {
        try {
            scanner.close();
        } catch (DataScannerException ex) {
            LOG.log(Level.SEVERE, "error con closing scanner " + ex, ex);
        }
    }

}
