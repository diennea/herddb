/*
 * Copyright 2018 eolivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.utils;

/**
 * Utility for instrumenting code
 *
 * @author eolivelli
 */
public abstract class InstrumentationUtils {

    private InstrumentationUtils() {
    }

    public static interface Listener {

        public void run(String id, Object... args);
    }
    private static Listener[] listeners = new Listener[0];

    public static void instrument(String id, Object... args) {
        try {
            for (Listener l : listeners) {
                l.run(id, args);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static void addListener(Listener l) {
        Listener[] copy = new Listener[listeners.length + 1];
        System.arraycopy(listeners, 0, copy, 0, listeners.length);
        copy[copy.length - 1] = l;
        listeners = copy;
    }

    public static void clear() {
        listeners = new Listener[0];
    }
}
