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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import herddb.utils.Holder;

import herddb.client.HDBConnection;
import herddb.client.TableSpaceRestoreSource;
import herddb.model.TableSpace;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.NonClosingInputStream;
import herddb.utils.NonClosingOutputStream;

/**
 * Backup Restore Utility
 *
 * @author enrico.olivelli
 */
public class BackupUtils {

    public static void dumpTableSpace(String schema, int fetchSize, HDBConnection connection, OutputStream fout, ProgressListener listener) throws Exception {

        /* Do not close externally provided streams */
        try (NonClosingOutputStream nos = new NonClosingOutputStream(fout);
            ExtendedDataOutputStream eos = new ExtendedDataOutputStream(nos)) {
            dumpTablespace(schema, fetchSize, connection, eos, listener);
        }
    }

    private static void dumpTablespace(String schema, int fetchSize, HDBConnection hdbconnection, ExtendedDataOutputStream out, ProgressListener listener) throws Exception {

        Holder<Throwable> errorHolder = new Holder<>();
        CountDownLatch waiter = new CountDownLatch(1);
        hdbconnection.dumpTableSpace(schema, new TableSpaceDumpFileWriter(listener, errorHolder, waiter, schema, out), fetchSize, true);
        if (errorHolder.value != null) {
            throw new Exception(errorHolder.value);
        }
        waiter.await();
    }

    public static void restoreTableSpace(String schema, String node, HDBConnection hdbconnection, InputStream fin, ProgressListener listener) throws Exception {

        /* Do not close externally provided streams */
        try (NonClosingInputStream nis = new NonClosingInputStream(fin);
            ExtendedDataInputStream eis = new ExtendedDataInputStream(nis)) {
            restoreTableSpace(schema, node, hdbconnection, eis, listener);
        }
    }

    public static void restoreTableSpace(String schema, String node, HDBConnection hdbconnection, ExtendedDataInputStream in, ProgressListener listener) throws Exception {

        listener.log("startRestore", "creating tablespace " + schema + " with leader " + node, Collections.singletonMap("tablespace", schema));
        hdbconnection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLESPACE '" + schema + "','leader:" + node + "','wait:60000'", 0, false, false, Collections.emptyList());

        TableSpaceRestoreSource source = new TableSpaceRestoreSourceFromFile(in, listener);
        hdbconnection.restoreTableSpace(schema, source);

        listener.log("restoreFinished", "restore finished for tablespace " + schema, Collections.singletonMap("tablespace", schema));

    }

}
