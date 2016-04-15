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

import herddb.model.Record;
import herddb.model.ScanResultSink;
import herddb.model.Table;
import herddb.network.Channel;
import herddb.network.Message;
import herddb.network.SendResultCallback;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scanner on the server-side
 *
 * @author enrico.olivelli
 */
public class ServerSideScannerPeer extends ScanResultSink {

    private static final int MAX_RECORDS_PER_CHUNK = 10;
    private final Channel channel;
    private final String scannerId;
    private List<Map<String, Object>> buffer = new ArrayList<>();
    private volatile boolean closed;
    private Table table;
    private Message queryMessage;
    private long totalCount = 0;

    public ServerSideScannerPeer(Channel channel, String scannerId, Message message) {
        this.channel = channel;
        this.scannerId = scannerId;
        this.queryMessage = message;
    }

    @Override
    public void finished() throws Exception {
        channel.sendOneWayMessage(Message.RESULTSET_CHUNK(null, scannerId, buffer, totalCount), new SendResultCallback() {
            @Override
            public void messageSent(Message originalMessage, Throwable error) {
                if (error != null) {
                    closed = true;
                }
            }
        });
    }

    public void clientClose() {
        closed = true;
    }

    @Override
    public boolean accept(Record record) throws Exception {
        if (closed) {
            return false;
        }
        totalCount++;
        buffer.add(record.toBean(table));
        if (buffer.size() == MAX_RECORDS_PER_CHUNK) { // TODO: configuration parameter?
            channel.sendOneWayMessage(Message.RESULTSET_CHUNK(null, scannerId, buffer, -1L), new SendResultCallback() {
                @Override
                public void messageSent(Message originalMessage, Throwable error) {
                    if (error != null) {
                        closed = true;
                    }
                }
            });
            buffer = new ArrayList<>();
        }

        return !closed;
    }

    @Override
    public void begin(Table table) throws Exception {
        this.table = table;
        this.channel.sendReplyMessage(queryMessage, Message.ACK(null));
    }

}
