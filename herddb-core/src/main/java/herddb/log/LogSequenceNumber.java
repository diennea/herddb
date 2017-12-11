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
package herddb.log;

import herddb.utils.Bytes;

/**
 * Write-ahead-log offset
 *
 * @author enrico.olivelli
 */
public final class LogSequenceNumber {

    public final long ledgerId;
    public final long offset;

    public static final LogSequenceNumber START_OF_TIME = new LogSequenceNumber(-1, -1);

    public LogSequenceNumber(long ledgerId, long offset) {
        this.ledgerId = ledgerId;
        this.offset = offset;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 19 * hash + (int) (this.ledgerId ^ (this.ledgerId >>> 32));
        hash = 19 * hash + (int) (this.offset ^ (this.offset >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LogSequenceNumber other = (LogSequenceNumber) obj;
        if (this.ledgerId != other.ledgerId) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "(" + ledgerId + ',' + offset + ')';
    }

    public boolean after(LogSequenceNumber snapshotSequenceNumber) {
        if (this.ledgerId < snapshotSequenceNumber.ledgerId) {
            return false;
        } else if (this.ledgerId == snapshotSequenceNumber.ledgerId) {
            return this.offset > snapshotSequenceNumber.offset;
        } else {
            return true;
        }
    }

    public boolean isStartOfTime() {
        return ledgerId == -1 && offset == -1;
    }

    public byte[] serialize() {
        byte[] array = new byte[8 + 8];
        Bytes.putLong(array, 0, ledgerId);
        Bytes.putLong(array, 8, offset);
        return array;
    }

    public static LogSequenceNumber deserialize(byte[] array) {
        return new LogSequenceNumber(
            Bytes.toLong(array, 0),
            Bytes.toLong(array, 8));
    }

}
