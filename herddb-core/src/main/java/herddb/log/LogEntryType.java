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
 * Types of log entry
 *
 * @author enrico.olivelli
 */
public class LogEntryType {

    public static final short CREATE_TABLE = 1;
    public static final short INSERT = 2;
    public static final short UPDATE = 3;
    public static final short DELETE = 4;
    public static final short BEGINTRANSACTION = 5;
    public static final short COMMITTRANSACTION = 6;
    public static final short ROLLBACKTRANSACTION = 7;
    public static final short ALTER_TABLE = 8;
    public static final short DROP_TABLE = 9;
    public static final short CREATE_INDEX = 10;
    public static final short DROP_INDEX = 11;
    public static final short TRUNCATE_TABLE = 12;
    public static final short NOOP = 13;
    public static final short TABLE_CONSISTENCY_CHECK = 14;
    /**
     * The content of the tablespace has been replaced by a snapshot, see {@link RestoredFromSnapshot}.
     *
     * <p>
     * <b>DECLARED BREAKING CHANGE of the commit log format.</b> This is the first entry type added since the format
     * was frozen, and {@code LogEntry.deserialize} answers a type it does not know with an exception, on purpose: an
     * entry it cannot read is an entry whose effect it cannot reproduce, and carrying on would rebuild a content the
     * tablespace never had. That guard is not being relaxed, so this type has consequences for a rolling upgrade
     * that have to be planned for:
     * </p>
     * <ul>
     * <li><b>Upgrade the replicas before the leader.</b> The first restore run on an upgraded leader puts this entry
     * on the commit log of the tablespace, and every replica still running a version that predates this type dies
     * while replaying it, with {@code unsupported type 15}.</li>
     * <li><b>Once a restore has run on a tablespace, no node serving that tablespace can be rolled back</b> to a
     * version that predates this type: the entry stays on the log until a checkpoint above it lets the ledgers below
     * be dropped, and a downgraded node meets it as soon as it replays that far.</li>
     * <li>A backup is unaffected and can still be restored on a version that predates this type. A backup does carry
     * log entries, but never one of these: a dump holds the tablespace locked, for write and then for read, for the
     * whole time its listener is attached to the commit log, and a restore needs that same write lock to open itself,
     * so no marker can ever be written while a dump is being taken.</li>
     * </ul>
     */
    public static final short RESTORED_FROM_SNAPSHOT = 15;

}
