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

}
