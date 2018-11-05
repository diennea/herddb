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
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import herddb.core.stats.TableManagerStats;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.network.Channel;
import herddb.proto.Pdu;
import herddb.utils.KeyValue;
import herddb.proto.PduCodec;
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
            byte[] tableDefinition = table.serialize();
            TableManagerStats stats = tableManager.getStats();
            List<byte[]> indexes = tableManager.getAvailableIndexes()
                    .stream()
                    .map(Index::serialize)
                    .collect(Collectors.toList());
            long id = _channel.generateRequestId();
            try (Pdu pdu = _channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                    id, tableSpaceName, dumpId, "beginTable", tableDefinition, stats.getTablesize(),
                    tableStatus.sequenceNumber.ledgerId, tableStatus.sequenceNumber.offset,
                    indexes, null), timeout);) {
            }
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
                sendBatch();
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
                sendBatch();
            }
            long id = _channel.generateRequestId();
            try (Pdu pdu = _channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                    id, tableSpaceName, dumpId, "endTable", null, 0,
                    0, 0,
                    null, null), timeout)) {

            }
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    private void sendBatch() throws TimeoutException, InterruptedException {
        long id = _channel.generateRequestId();
        try (Pdu pdu = _channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                id, tableSpaceName, dumpId, "data", null, 0,
                0, 0,
                null, batch), timeout)) {
        }
        batch.clear();
    }

}
