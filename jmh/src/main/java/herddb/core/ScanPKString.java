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

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Fork(1)
@State(Scope.Benchmark)
public class ScanPKString {

    DBManager manager;
    String key;

    @Setup
    public void setup() throws Exception {

        key = String.format("%1$" + keySize + "s", "a");

        String nodeId = "localhost";
        manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);
        manager.start();
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);

        execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,n2 long, s1 string)", Collections.emptyList());

        executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList(key, Integer.valueOf(1), "a", Long.valueOf(3)));
        executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a", Long.valueOf(2)));
        executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b", Long.valueOf(1)));
        executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4"));
        System.out.println("key:" + key);
        System.out.println("keylen:" + key.length());
        System.out.println("keyszue:" + keySize);
    }

    @TearDown
    public void tearDown() {
        manager.close();
    }

    @Param({"1", "1000", "100000"})
    public int keySize;

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void scanByStringPrimaryKey() throws Exception {
        try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where k1=?", Arrays.asList(key));) {
            List<DataAccessor> result = scan1.consume();
            if (result.size() != 1) {
                throw new RuntimeException();
            }
        }
    }

}
