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

package herddb.backup;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.log.LogSequenceNumber;

/**
 * An entry of log dumped for backup/restore
 *
 * @author enrico.olivelli
 */
public final class DumpedLogEntry implements Comparable<DumpedLogEntry> {

    public final LogSequenceNumber logSequenceNumber;
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public final byte[] entryData;

    public DumpedLogEntry(LogSequenceNumber logSequenceNumber, byte[] entryData) {
        this.logSequenceNumber = logSequenceNumber;
        this.entryData = entryData;
    }

    @Override
    @SuppressFBWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS")
    public int compareTo(DumpedLogEntry o) {
        boolean after = this.logSequenceNumber.after(o.logSequenceNumber);
        if (after) {
            return 1;
        } else if (this.logSequenceNumber.equals(o.logSequenceNumber)) {
            return 0;
        } else {
            return -1;
        }
    }

}
