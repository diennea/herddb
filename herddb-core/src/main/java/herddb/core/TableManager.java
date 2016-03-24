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
package herddb.core;

import herddb.log.CommitLog;
import herddb.log.SequenceNumber;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.utils.Bytes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 */
public class TableManager {

    /**
     * a buffer which contains the rows contained into the loaded pages
     * (map<byte[],byte[]>)
     */
    private final Map<Bytes, Bytes> buffer = new HashMap<>();

    /**
     * keyToPage: a structure which maps each key to the ID of the page
     * (map<byte[], long>) (this can be quite large)
     */
    private final Map<Bytes, Long> keyToPage = new HashMap<>();

    /**
     * a structure which holds the set of the pages which are loaded in memory
     * (set<long>)
     */
    private final Set<Long> loadedPages = new HashSet<>();

    /**
     * a structure which holds the set of the keys of changed rows (set<byte[]>)
     */
    private final Set<Bytes> dirtyRows = new HashSet<>();

    /**
     * Definition of the table
     */
    private Table table;
    private final CommitLog log;
    private final DataStorageManager dataStorageManager;

    TableManager(Table table, CommitLog log, DataStorageManager dataStorageManager) {
        this.table = table;
        this.log = log;
        this.dataStorageManager = dataStorageManager;
    }

    public void boot() {

    }

}
