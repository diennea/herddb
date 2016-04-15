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
package herddb.client.impl;

import herddb.client.ScanRecordConsumer;
import herddb.network.Channel;
import herddb.network.Message;
import herddb.network.SendResultCallback;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scanner on the client-side
 *
 * @author enrico.olivelli
 */
public class ClientSideScannerPeer {

    private static final Logger LOGGER = Logger.getLogger(ClientSideScannerPeer.class.getName());
    private final String scannerId;
    private final ScanRecordConsumer consumer;
    private volatile boolean closed;
    private final Channel channel;
    private AtomicLong totalCount = new AtomicLong();
    private long totalCountAtEnd;

    public ClientSideScannerPeer(String scannerId, ScanRecordConsumer consumer, Channel channel) {
        this.scannerId = scannerId;
        this.consumer = consumer;
        this.channel = channel;
    }

    public boolean isClosed() {
        return closed;
    }

    public void accept(List<Map<String, Object>> records, long totalCountAtEnd) {
        if (totalCountAtEnd >= 0) {
            this.totalCountAtEnd = totalCountAtEnd;
        }
                
        for (Map<String, Object> record : records) {
            boolean ok = false;
            try {
                ok = consumer.accept(record);
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "scanner " + scannerId + " error " + err, err);
            }
            if (!ok) {
                LOGGER.log(Level.SEVERE, "scanner " + scannerId + " interrupted by client accept -> false");
                closed = true;
                channel.sendOneWayMessage(Message.CLOSE_SCANNER(null, scannerId), new SendResultCallback() {
                    @Override
                    public void messageSent(Message originalMessage, Throwable error) {
                        if (error != null) {
                            LOGGER.log(Level.SEVERE, "scanner " + scannerId + " error " + error);
                        }
                        consumer.finish();
                    }
                });
                break;
            }
        }
        long actual = totalCount.addAndGet(records.size());        
        checkFinished(actual);
    }

    public void finished(long totalCountAtEnd) {
        consumer.finish();
    }

    private void checkFinished(long actual) {        
        if (actual == totalCountAtEnd) {
            consumer.finish();
        }
    }
}
