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

/**
 * The log cannot be replayed any further because the content of the tablespace was replaced by a snapshot, see
 * {@link RestoredFromSnapshot}.
 * <p>
 * This is one of the reasons why a full download of the data of the tablespace is needed, and it is the only one that
 * names a restore. The others, such as a ledger that is no longer available, say nothing about restores, so a reader
 * that wants to talk about restores has to recognise this one instead of assuming that every
 * {@link FullRecoveryNeededException} comes from here.
 * </p>
 */
public class RestoredFromSnapshotException extends FullRecoveryNeededException {

    private final LogSequenceNumber markerPosition;

    /**
     * @param markerPosition position of the marker that stopped the reader of the log
     */
    public RestoredFromSnapshotException(String message, LogSequenceNumber markerPosition) {
        super(message);
        this.markerPosition = markerPosition;
    }

    /**
     * @return the position of the restore marker that stopped the reader of the log
     */
    public LogSequenceNumber getMarkerPosition() {
        return markerPosition;
    }

}
