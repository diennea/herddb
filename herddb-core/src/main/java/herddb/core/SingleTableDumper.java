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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import herddb.core.stats.TableManagerStats;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.network.Channel;
import herddb.network.KeyValue;
import herddb.network.Message;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;

/**
 * Dumps data of a table
 *
 * @author enrico.olivelli
 */
class SingleTableDumper implements FullTableScanConsumer {

    private final AbstractTableManager tableManager;
    private final Channel _channel;
    private final String dumpId;
    private final String tableSpaceName;
    private final int timeout;
    private final int fetchSize;

    public SingleTableDumper(String tableSpaceName, AbstractTableManager tableManager, Channel _channel, String dumpId, int timeout, int fetchSize) {
        this.tableSpaceName = tableSpaceName;
        this.tableManager = tableManager;
        this._channel = _channel;
        this.dumpId = dumpId;
        this.timeout = timeout;
        this.fetchSize = fetchSize;
    }
    final List<KeyValue> batch = new ArrayList<>();

    @Override
    public void acceptTableStatus(TableStatus tableStatus) {
        try {
            Table table = tableManager.getTable();
            byte[] serialized = table.serialize();
            Map<String, Object> beginTableData = new HashMap<>();
            TableManagerStats stats = tableManager.getStats();
            beginTableData.put("command", "beginTable");
            beginTableData.put("table", serialized);
            beginTableData.put("estimatedSize", stats.getTablesize());
            beginTableData.put("dumpLedgerid", tableStatus.sequenceNumber.ledgerId);
            beginTableData.put("dumpOffset", tableStatus.sequenceNumber.offset);
            List<byte[]> indexes = tableManager.getAvailableIndexes()
                .stream()
                .map(Index::serialize)
                .collect(Collectors.toList());
            beginTableData.put("indexes", indexes);
            _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, beginTableData), timeout);
        } catch (InterruptedException | TimeoutException err) {
            throw new HerdDBInternalException(err);
        }
    }

    @Override
    public void startPage(long pageId) {
    }

    @Override
    public void acceptRecord(Record record) {
        try {
            batch.add(new KeyValue(record.key.data, record.value.data));
            if (batch.size() == fetchSize) {
                Map<String, Object> data = new HashMap<>();
                data.put("command", "data");
                data.put("records", batch);
                _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, data), timeout);
                batch.clear();
            }
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    @Override
    public void endPage() {
    }

    @Override
    public void endTable() {
        try {
            if (!batch.isEmpty()) {
                Map<String, Object> data = new HashMap<>();
                data.put("command", "data");
                data.put("records", batch);
                _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, data), timeout);
                batch.clear();
            }
            Map<String, Object> endTableData = new HashMap<>();
            endTableData.put("command", "endTable");
            _channel.sendMessageWithReply(Message.TABLESPACE_DUMP_DATA(null, tableSpaceName, dumpId, endTableData), timeout);
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

}
