/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
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
