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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Spaces out the attempts at something that the activator would otherwise try once per second, for as long as those
 * attempts keep ending the same way.
 *
 * <p>
 * The activator wakes up at least once a second and works out what each tablespace of this node should be doing. Two
 * of the things it decides are expensive: booting a tablespace reads the whole part of its commit log that follows the
 * last checkpoint, and so does asking whether this node could lead a tablespace whose leader has gone silent. Both are
 * worth that much when they change something. When they keep ending in a refusal instead, and the tablespace stays
 * exactly where it was, repeating them once a second costs a full read of the log once a second, and one report of the
 * same failure once a second, for as long as nobody repairs anything.
 * </p>
 *
 * <p>
 * So a refusal pushes the next attempt away, doubling the wait up to a cap. The answer itself is never kept: the
 * attempt is always made again, only later. That is the whole difference between this and remembering the refusal.
 * An answer that is read out of the commit log cannot be remembered against anything this node holds, because that log
 * belongs to whichever node leads the tablespace and keeps growing while the position this node sits at stands still,
 * so an answer kept against that position would be wrong exactly when it matters. A wait can only ever make an attempt
 * late, never wrong.
 * </p>
 *
 * <p>
 * The wait is dropped as soon as the situation the refusal was about changes, so that a repair is acted upon at the
 * next pass of the activator rather than at the end of the current wait. What the situation is belongs to the caller,
 * which hands it over with every attempt: see {@link #due}.
 * </p>
 */
final class PacedRetries {

    /**
     * How long the first retry of an attempt that has just been refused waits.
     */
    private final long firstDelay;

    /**
     * The longest wait a refusal that keeps repeating can push an attempt to.
     */
    private final long maxDelay;

    /**
     * The keys that are being held back, and for each of them the refusal that is holding it back. A key that is not
     * here is a key nothing has refused yet, or one whose attempt finally succeeded.
     */
    private final ConcurrentMap<String, Refusal> refusals = new ConcurrentHashMap<>();

    PacedRetries(long firstDelay, long maxDelay) {
        this.firstDelay = firstDelay;
        this.maxDelay = maxDelay;
    }

    /**
     * @param key what the attempts are about, one wait per key
     * @param situation everything the previous refusal depended on, as far as this node can see it. A situation that
     * is not equal to the one of the previous refusal drops the wait and the attempt is made now, because what it is
     * about is no longer what was refused
     * @return the attempt to make now, or {@code null} when the previous refusal is still holding this key back
     */
    Attempt due(String key, Object situation) {
        Refusal refusal = refusals.get(key);
        if (refusal == null || !Objects.equals(refusal.situation, situation)) {
            return new Attempt(key, situation, firstDelay, true);
        }
        if (System.currentTimeMillis() < refusal.nextAttempt) {
            return null;
        }
        long delay = Math.min(refusal.delay * 2, maxDelay);
        return new Attempt(key, situation, delay, delay > refusal.delay);
    }

    /**
     * Records that the attempt was refused, which is what pushes the next one away.
     */
    void refused(Attempt attempt) {
        refusals.put(attempt.key,
                new Refusal(attempt.situation, attempt.delay, System.currentTimeMillis() + attempt.delay));
    }

    /**
     * Records that there is nothing left to hold back for this key, because the attempt finally went through.
     */
    void succeeded(String key) {
        refusals.remove(key);
    }

    /**
     * Forgets every key that is not in the given collection, which is how the keys that do not exist any more stop
     * taking up room.
     */
    void forgetAllExcept(Collection<String> keys) {
        refusals.keySet().retainAll(keys);
    }

    /**
     * An attempt that may be made right now.
     */
    static final class Attempt {

        private final String key;

        private final Object situation;

        /**
         * How long the next attempt waits if this one is refused.
         */
        private final long delay;

        /**
         * Whether that wait is longer than the one this attempt waited.
         */
        private final boolean waitGrows;

        private Attempt(String key, Object situation, long delay, boolean waitGrows) {
            this.key = key;
            this.situation = situation;
            this.delay = delay;
            this.waitGrows = waitGrows;
        }

        /**
         * @return whether a refusal of this attempt is worth reporting in full, with everything that led to it. It is
         * the first refusal, or one that makes this node wait longer than it waited before; the repeats in between say
         * nothing that was not said then, to an operator who has already been told what is wrong and what to do about
         * it
         */
        boolean reportInFull() {
            return waitGrows;
        }

        /**
         * @return how long the next attempt waits, if this one is refused
         */
        long retryIn() {
            return delay;
        }
    }

    /**
     * An attempt that was refused, and the wait it bought.
     */
    private static final class Refusal {

        private final Object situation;

        private final long delay;

        private final long nextAttempt;

        private Refusal(Object situation, long delay, long nextAttempt) {
            this.situation = situation;
            this.delay = delay;
            this.nextAttempt = nextAttempt;
        }
    }
}
