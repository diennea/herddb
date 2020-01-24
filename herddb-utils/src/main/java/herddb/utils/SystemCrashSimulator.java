/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility for fault inject.
 *
 * @author enrico.olivelli
 */
public class SystemCrashSimulator {

    /**
     * Listens only for a given crashpoint
     */
    public static abstract class SimpleCrashPointListener implements CrashPointListener {

        private final String crashpointid;

        public SimpleCrashPointListener(String crashpointid) {
            this.crashpointid = crashpointid;
        }

        public abstract void crashPoint(Object... args) throws Exception;

        @Override
        public final void crashPoint(String crashpointid, Object... args) throws Exception {
            if (crashpointid.equals(this.crashpointid)) {
                crashPoint(args);
            }
        }

    }

    public static interface CrashPointListener {

        public void crashPoint(String crashpointid, Object... args) throws Exception;
    }
    private static List<CrashPointListener> listeners = null;

    public static void addListener(CrashPointListener l) {
        if (listeners == null) {
            listeners = new ArrayList<>();
        }
        listeners.add(l);
    }

    public static void crashPointRuntimeException(String crashpointid, Object... args) {
        if (listeners != null) {
            for (CrashPointListener listener : listeners) {
                try {
                    listener.crashPoint(crashpointid, args);
                } catch (Throwable t) {
                    if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    }
                    throw new RuntimeException(t);
                }
            }
        }
    }

    public static void clear() {
        // settiamo a null
        // se facciamo clear non è possibile aggiungere/togliere listeners durante l'invocazione di un listener (ConcurrentModificationException)
        listeners = null;
    }
}
