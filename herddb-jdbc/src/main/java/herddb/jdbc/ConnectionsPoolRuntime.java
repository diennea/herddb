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

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * This class holds all references to Commons Pool2, in order
 * to not have hard links to Commons2 in the pure JDBC Driver.
 */
class ConnectionsPoolRuntime {

    private final BasicHerdDBDataSource parent;
    private final GenericObjectPool<HerdDBConnection> pool;

    public ConnectionsPoolRuntime(BasicHerdDBDataSource parent) {
        this.parent = parent;
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setBlockWhenExhausted(true);
        int maxActive = parent.getMaxActive();
        config.setMaxTotal(parent.getMaxActive());
        config.setMaxIdle(maxActive);
        config.setMinIdle(maxActive / 2);
        config.setJmxNamePrefix("HerdDBClient");
        pool = new GenericObjectPool<>(new ConnectionsFactory(), config);
    }

    HerdDBConnection borrowObject() throws Exception {
        return pool.borrowObject();
    }

    void returnObject(HerdDBConnection connection) {
        pool.returnObject(connection);
    }

    private final class ConnectionsFactory implements PooledObjectFactory<HerdDBConnection> {

        @Override
        public PooledObject<HerdDBConnection> makeObject() throws Exception {
            HerdDBConnection res = parent.makeConnection();
            return new DefaultPooledObject<>(res);
        }

        @Override
        public void destroyObject(PooledObject<HerdDBConnection> po) throws Exception {
            po.getObject().close();
        }

        @Override
        public boolean validateObject(PooledObject<HerdDBConnection> po) {
            return true;
        }

        @Override
        public void activateObject(PooledObject<HerdDBConnection> po) throws Exception {
            po.getObject().reset(parent.getDefaultSchema());
        }

        @Override
        public void passivateObject(PooledObject<HerdDBConnection> po) throws Exception {
        }

    }

}
