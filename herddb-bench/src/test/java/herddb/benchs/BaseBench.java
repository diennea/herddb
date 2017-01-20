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
package herddb.benchs;

import static herddb.benchs.BaseTableDefinition.COUNT;
import static herddb.benchs.BaseTableDefinition.CREATE_TABLE;
import static herddb.benchs.BaseTableDefinition.INSERT;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.jdbc.BasicHerdDBDataSource;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class BaseBench {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    protected Server server;
    protected ServerConfiguration serverConfiguration;
    protected HDBClient client;
    protected BasicHerdDBDataSource dataSource;
    protected ExecutorService threadpool;

    protected final int numThreads;
    protected final int dataSetSize;
    protected final int operations;
    protected final int maxBatchSize;
    protected final List<Future<?>> tasks = new ArrayList<>();
    protected final Random random = new Random();
    protected final List<Operation> operationTypes = new ArrayList<>();

    public BaseBench(int numThreads, int dataSetSize, int operations, int maxBatchSize) {
        this.numThreads = numThreads;
        this.dataSetSize = dataSetSize;
        this.operations = operations;
        this.maxBatchSize = maxBatchSize;
    }

    public void generateData() throws Exception {
        long start = System.currentTimeMillis();
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement(CREATE_TABLE);) {
                ps.executeUpdate();
            }

            try (PreparedStatement ps = con.prepareStatement(INSERT);) {
                for (int i = 0; i < dataSetSize; i++) {
                    String value = "pk" + i;
                    ps.setString(1, value);
                    ps.setString(2, value);
                    ps.setString(3, value);
                    ps.addBatch();
                    if (i % 1000 == 0) {
                        ps.executeBatch();
                        con.commit();
                    }
                }
                ps.executeBatch();
                con.commit();
            }
            con.setAutoCommit(true);

            try (PreparedStatement ps = con.prepareStatement(COUNT);
                ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(dataSetSize, rs.getInt(1));
            }

        }
        long stop = System.currentTimeMillis();
        System.out.println("[BENCH] Time to generate " + dataSetSize + " records: " + (stop - start) + " ms");
    }

    public final void addOperation(Operation op) {
        this.operationTypes.add(op);
    }

    public void performOperations() throws Exception {
        int operationTypesLen = operationTypes.size();
        for (int i = 0; i < operations; i++) {
            int seed = random.nextInt(dataSetSize);
            int operation = random.nextInt(operationTypesLen);
            int batchSize = 1 + random.nextInt(maxBatchSize);
            Callable newInstance = operationTypes.get(operation).newInstance(seed, batchSize, dataSource);
            submit(newInstance);
        }
    }

    @Before
    public void startServer() throws Exception {
        serverConfiguration = new ServerConfiguration(folder.newFolder().toPath());
        threadpool = Executors.newFixedThreadPool(numThreads);
        server = new Server(serverConfiguration);
        server.start();
        server.waitForStandaloneBoot();
        client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
        client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
        dataSource = new BasicHerdDBDataSource(client);
    }

    public Future<?> submit(Callable runnable) {
        Future<?> submit = threadpool.submit(runnable);
        tasks.add(submit);
        return submit;
    }

    public void restartServer() throws Exception {
        long _start = System.currentTimeMillis();
        System.out.println("[BENCH] restarting server");
        server.close();
        server = new Server(serverConfiguration);
        server.start();
        server.waitForTableSpaceBoot(TableSpace.DEFAULT, Integer.MAX_VALUE, true);
        long _stop = System.currentTimeMillis();
        System.out.println("[BENCH] server restarted in " + (_stop - _start) + " ms");
    }

    public void waitForResults() throws Exception {
        long start = System.currentTimeMillis();
        int i = 0;
        for (Future f : tasks) {
            f.get();
            if (i % 1000 == 0) {
                long stop = System.currentTimeMillis();
                System.out.println("[BENCH] done #" + i + " operations, time: " + (stop - start) + " ms");
            }
            i++;
        }
        long stop = System.currentTimeMillis();
        System.out.println("[BENCH] time: " + (stop - start) + " ms");
        tasks.clear();
    }

    @After
    public void stopServer() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        for (Future f : tasks) {
            f.cancel(true);
        }
        threadpool.shutdown();
    }

}
