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

package herddb.storage;

import herddb.core.HerdDBInternalException;

/**
 * Error on storage
 *
 * @author enrico.olivelli
 */
public class DataStorageManagerException extends HerdDBInternalException {

    public DataStorageManagerException(String message) {
        super(message);
    }

    public DataStorageManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataStorageManagerException(Throwable cause) {
        super(cause);
    }

}
