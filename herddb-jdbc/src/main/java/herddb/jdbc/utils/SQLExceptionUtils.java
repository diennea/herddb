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

package herddb.jdbc.utils;

import herddb.client.HDBException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.concurrent.CompletionException;

/**
 * Utility to report SQLException correctly to the client
 *
 * @author enrico.olivelli
 */
public class SQLExceptionUtils {

    public static SQLException wrapException(Throwable exception) {
        while (exception instanceof CompletionException) {
            exception = exception.getCause();
        }
        SQLException res;

        if (exception instanceof HDBException && exception.getCause() instanceof SQLIntegrityConstraintViolationException) {
            res = new SQLIntegrityConstraintViolationException(exception.getMessage());
        } else if (exception instanceof HDBException) {
            HDBException ex = (HDBException) exception;
            res = new SQLException(ex.getMessage(), ex);
        } else {
            res = new SQLException(exception);
        }
        return res;
    }
}
