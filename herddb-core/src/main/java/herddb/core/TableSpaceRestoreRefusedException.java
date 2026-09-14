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

import herddb.model.StatementExecutionException;

/**
 * A restore from a snapshot was refused before anything was written: no marker reached the commit log, no restore was
 * opened on the tablespace, and the tablespace is exactly as it was.
 * <p>
 * That is the whole point of having a type of its own. Every other way the first step of a restore can fail leaves the
 * marker on the log, which means the tablespace really is in the middle of a restore and whoever asked for it owns
 * one; a refusal owns nothing, and the tablespace it names is a healthy tablespace that must not be taken out of
 * service because somebody aimed a restore at it.
 * </p>
 */
public class TableSpaceRestoreRefusedException extends StatementExecutionException {

    public TableSpaceRestoreRefusedException(String message) {
        super(message);
    }

}
