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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection on the client side
 *
 * @author enrico.olivelli
 */
public class ClientSideConnection implements AutoCloseable {

    private Map<String, RoutedClientSideConnection> routes = new ConcurrentHashMap<>();
    private final AtomicLong IDGENERATOR = new AtomicLong();
    private final long id = IDGENERATOR.incrementAndGet();
    private final HDBClient client;    

    public ClientSideConnection(HDBClient client) {
        this.client = client;
    }

    public long getId() {
        return id;
    }

    @Override
    public void close() {
        List<RoutedClientSideConnection> routesAtClose = new ArrayList<>(routes.values());
        for (RoutedClientSideConnection route : routesAtClose) {
            route.close();
        }
        client.releaseConnection(this);
    }

    void releaseRoute(String key) {
        routes.remove(key);
    }

    public long executeUpdate(String tableSpace, String query, Object... params) {
        return -1;
    }

}
