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
package herddb.daemons;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Lock sul pid file, per evitare avvii concorrenti del servizio
 *
 * @author enrico.olivelli
 */
public class PidFileLocker implements AutoCloseable {

    private final Path file;
    private final byte[] pid;
    private volatile boolean closed;
    private final static String PIDFILE = System.getProperty("pidfile", "");
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "pid-file-locker");
            t.setDaemon(true);
            return t;
        }
    });

    public PidFileLocker(Path basePath) {
        this.file = basePath.resolve(PIDFILE);
        String _pid = ManagementFactory.getRuntimeMXBean().getName();
        int choi = _pid.indexOf('@');
        _pid = _pid.substring(0, choi);
        System.out.println("Current process PID:" + _pid);
        this.pid = _pid.getBytes(StandardCharsets.UTF_8);
    }

    public void lock() throws IOException {
        if (PIDFILE.isEmpty()) {
            return;
        }
        System.out.println("Creating and locking file " + file.toAbsolutePath());
        if (Files.exists(file)) {
            throw new IOException("file " + file.toAbsolutePath() + " already exists");
        }
        Files.write(file, pid, StandardOpenOption.CREATE_NEW);
        timer.scheduleWithFixedDelay(() -> {
            try {
                check();
            } catch (Exception err) {
                Runtime.getRuntime().halt(1);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    public void check() throws Exception {
        if (PIDFILE.isEmpty() || closed) {
            return;
        }
        if (!Files.isRegularFile(file)) {
            System.out.println("Lock file " + file.toAbsolutePath() + " does not exists any more. stopping service");
            throw new Exception("Lock file " + file.toAbsolutePath() + " does not exists any more. stopping service");
        } else {
            byte[] actualContent;
            try {
                actualContent = Files.readAllBytes(file);
            } catch (IOException err) {
                System.out.println("Lock file " + file.toAbsolutePath() + " cannot be read (" + err + "). stopping service");
                throw new Exception("Lock file " + file.toAbsolutePath() + " cannot be read (" + err + "). stopping service", err);
            }
            if (!Arrays.equals(pid, actualContent)) {
                System.out.println("Lock file " + file.toAbsolutePath() + " changed, now contains " + new String(actualContent, StandardCharsets.UTF_8) + ". stopping service");
                throw new Exception("Lock file " + file.toAbsolutePath() + " changed, now contains " + new String(actualContent, StandardCharsets.UTF_8) + ". stopping service");
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdown();
        if (PIDFILE.isEmpty()) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException err) {
        }
    }

}
