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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.jdbc.BasicHerdDBDataSource;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.After;
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
        generateData(10);
    }

    public void generateData(int datasize) throws Exception {
        StringBuilder filler = new StringBuilder();
        for (int i = 0; i < datasize; i++) {
            filler.append('a');
        }
        String padding = filler.toString();

        long start = System.currentTimeMillis();
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement(CREATE_TABLE)) {
                ps.executeUpdate();
            }

            try (PreparedStatement ps = con.prepareStatement(INSERT)) {
                for (int i = 0; i < dataSetSize; i++) {
                    String value = "pk" + i;
                    ps.setString(1, value);
                    ps.setString(2, value + padding);
                    ps.setString(3, value + padding);
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
        makeServerConfiguration();
        threadpool = Executors.newFixedThreadPool(numThreads);
        server = new Server(serverConfiguration);
        server.start();
        server.waitForStandaloneBoot();
        client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
        client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
        dataSource = new BasicHerdDBDataSource(client);
    }

    protected void makeServerConfiguration() throws IOException {
        serverConfiguration = new ServerConfiguration(folder.newFolder().toPath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_PORT, 7002);
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

    public void backupRestore(int batchSize, ProgressListener pl) throws Exception {
        if (pl == null) {
            pl = new ProgressListener() {
            };
        }
        System.out.println("BACKUP PHASE");
        File backupFile = folder.newFile("test.backup");
        try {
            try (HDBConnection connection = client.openConnection();
                 FileOutputStream oo = new FileOutputStream(backupFile);
                 GZIPOutputStream zout = new GZIPOutputStream(oo)) {
                BackupUtils.dumpTableSpace(TableSpace.DEFAULT, batchSize, connection, zout, pl);
            }

            System.out.println("RESTORE PHASE on a " + backupFile.length() + " bytes file");
            try (HDBConnection connection = client.openConnection();
                 FileInputStream oo = new FileInputStream(backupFile);
                 GZIPInputStream zin = new GZIPInputStream(oo)) {
                BackupUtils.restoreTableSpace("newschema", server.getNodeId(), connection, zin, pl);
            }
        } finally {
            backupFile.delete();
        }

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
