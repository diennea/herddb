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

import herddb.storage.DataStorageManagerException;

/**
 * This node leads a tablespace whose content is not here, and no restart can put that right: the content has to be
 * fetched from a node that holds it, and a leader has nobody to fetch from.
 * <p>
 * This is what is left of a tablespace whose content was replaced by a restore from a snapshot that this node never
 * received. The data of a restore comes from a client, one request at a time, and never travels through the commit
 * log, so the log describes nothing of it and replaying it rebuilds nothing.
 * </p>
 * <p>
 * It is told apart from every other reason a tablespace fails to boot because of what has to happen next. A boot that
 * fails on a transient problem, an unreachable metadata store or a bookie that is not answering, is worth stopping
 * the node for: the state of that tablespace is unknown, and a restart may well find it healthy. This state is known,
 * permanent and repairable only by an operator, either by moving the leadership to a node that holds the content or
 * by dropping the tablespace, and both of those are commands that need a node alive to receive them. A node that
 * stopped itself over this would stop again at every start, taking every other tablespace it serves with it, and
 * would leave nobody to give the command to.
 * </p>
 */
public class TableSpaceCannotBeLedHereException extends DataStorageManagerException {

    private final String tableSpaceName;

    public TableSpaceCannotBeLedHereException(String message, String tableSpaceName, Throwable cause) {
        super(message, cause);
        this.tableSpaceName = tableSpaceName;
    }

    public String getTableSpaceName() {
        return tableSpaceName;
    }

}
