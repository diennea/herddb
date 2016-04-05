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
package herddb.metadata;

import herddb.model.DDLException;
import herddb.model.Table;
import herddb.model.TableSpace;
import java.util.Collection;

/**
 * Store of all metadata of the system: definition of tables, tablesets,
 * available nodes
 *
 * @author enrico.olivelli
 */
public abstract class MetadataStorageManager {

    /**
     * Enumerates all the available TableSpaces in the system
     *
     * @return
     */
    public abstract Collection<String> listTableSpaces();

    /**
     * Describe a single TableSpace
     *
     * @param name
     * @return
     */
    public abstract TableSpace describeTableSpace(String name);

    /**
     * Registers a new table space on the metadata storage
     *
     * @param tableSpace
     */
    public abstract void registerTableSpace(TableSpace tableSpace) throws DDLException;

    /**
     * Updates table space metadata on the metadata storage
     *
     * @param tableSpace
     */
    public abstract void updateTableSpace(TableSpace tableSpace) throws DDLException;
}
