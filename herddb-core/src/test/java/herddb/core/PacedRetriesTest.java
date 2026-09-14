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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * What a refusal buys, and what it must never buy: the wait grows while the refusal keeps repeating, and the answer
 * itself is never kept, so every attempt that is made is made in full.
 *
 * <p>
 * The waits these tests work with are either a few milliseconds long, where what is checked is how long the next
 * attempt is told to wait, or longer than the whole test run, where what is checked is that an attempt is held back at
 * all. Neither of them measures how long anything actually took, so a machine that stops for a while between two lines
 * of a test changes nothing about its result.
 * </p>
 */
public class PacedRetriesTest {

    /**
     * What the attempts of most of these tests are about. The value means nothing here: it is a key, and the only
     * thing that matters about it is that it is not the other one.
     */
    private static final String A_TABLESPACE = "1d4ec4a3d6e04e2b8b8a0f0f4a5a6b7c";

    /**
     * A second key, used where what is being checked is that one key is held back without the other one being.
     */
    private static final String ANOTHER_TABLESPACE = "2f5ad5b4e7f15f3c9c9b1a1a5b6b7c8d";

    /**
     * Everything the refusal of an attempt depended on, as the caller sees it.
     */
    private static final Object A_SITUATION = "leader:node1";

    /**
     * The same thing after somebody repaired it, which is what a wait must not outlive.
     */
    private static final Object THE_SITUATION_AFTER_A_REPAIR = "leader:node2";

    /**
     * The first wait of the tests that watch the wait grow. It only has to be short enough for a test to sit through
     * a few of them.
     */
    private static final long FIRST_DELAY = 1;

    /**
     * The cap of the same tests, three doublings above the first wait.
     */
    private static final long MAX_DELAY = 8;

    /**
     * How long the next attempt is told to wait, on the first refusal and on the ones that follow it, until the cap
     * stops the doubling.
     */
    private static final long[] WAITS_A_REPEATING_REFUSAL_BUYS = {FIRST_DELAY, 2, 4, MAX_DELAY, MAX_DELAY};

    /**
     * Whether each of those refusals is worth reporting with everything that led to it: the first one, and every one
     * that makes the caller wait longer than it waited before.
     */
    private static final boolean[] REFUSALS_WORTH_REPORTING_IN_FULL = {true, true, true, true, false};

    /**
     * A wait no test sits through, used where the point is that an attempt is held back and not how long for.
     */
    private static final long A_WAIT_NO_TEST_OUTLIVES = TimeUnit.MINUTES.toMillis(10);

    /**
     * How long a test waits for an attempt that is a few milliseconds away before it calls it lost.
     */
    private static final long TIME_GIVEN_TO_AN_ATTEMPT = TimeUnit.MINUTES.toMillis(1);

    /**
     * An attempt nobody has refused yet is made now, and it is worth reporting in full, because there is nobody who
     * has been told about it before.
     */
    @Test
    public void testAnAttemptNothingHoldsBackIsMadeNow() {
        PacedRetries retries = new PacedRetries(FIRST_DELAY, MAX_DELAY);

        PacedRetries.Attempt attempt = retries.due(A_TABLESPACE, A_SITUATION);

        assertNotNull("the first attempt at something nothing has refused yet was held back", attempt);
        assertEquals("the first refusal buys a wait that is not the one it was configured with", FIRST_DELAY,
                attempt.retryIn());
        assertTrue("the first refusal of something says nothing worth telling in full, so an operator is never told"
                + " what is wrong and what to do about it", attempt.reportInFull());
    }

    /**
     * Asking twice changes nothing on its own. The wait is bought by a refusal and by nothing else: an attempt that
     * was made and never came back is an attempt whose answer is still unknown, and holding the next one back over it
     * would be waiting for something nobody is doing.
     */
    @Test
    public void testAnAttemptThatWasNotRefusedHoldsNothingBack() {
        PacedRetries retries = new PacedRetries(A_WAIT_NO_TEST_OUTLIVES, A_WAIT_NO_TEST_OUTLIVES);

        retries.due(A_TABLESPACE, A_SITUATION);

        assertNotNull("an attempt that was made and not refused held the next one back", retries.due(A_TABLESPACE,
                A_SITUATION));
    }

    /**
     * A refusal holds the next attempt back, which is the whole point: the caller would otherwise make it again
     * straight away, and pay in full for an answer that has had no occasion to change.
     */
    @Test
    public void testARefusalHoldsTheNextAttemptBack() {
        PacedRetries retries = new PacedRetries(A_WAIT_NO_TEST_OUTLIVES, A_WAIT_NO_TEST_OUTLIVES);

        retries.refused(retries.due(A_TABLESPACE, A_SITUATION));

        assertNull("an attempt that has just been refused is made again straight away, so the refusal bought nothing",
                retries.due(A_TABLESPACE, A_SITUATION));
    }

    /**
     * Every refusal doubles the wait until the cap stops it there. What is held back is the attempt and never the
     * answer, so the wait can only ever make an attempt late: the cap is what keeps late from becoming never.
     */
    @Test
    public void testEveryRefusalDoublesTheWaitUpToTheCap() throws Exception {
        PacedRetries retries = new PacedRetries(FIRST_DELAY, MAX_DELAY);

        for (int refusal = 0; refusal < WAITS_A_REPEATING_REFUSAL_BUYS.length; refusal++) {
            PacedRetries.Attempt attempt = waitUntilDue(retries, A_TABLESPACE, A_SITUATION);
            assertEquals("refusal number " + (refusal + 1) + " of the same attempt buys the wrong wait, so the"
                    + " attempts are not spaced out the way they were meant to be",
                    WAITS_A_REPEATING_REFUSAL_BUYS[refusal], attempt.retryIn());
            assertEquals("refusal number " + (refusal + 1) + " of the same attempt is reported the wrong way: the"
                    + " first one and the ones that make the caller wait longer than before are the ones that have"
                    + " something to add, and the repeats in between have nothing",
                    REFUSALS_WORTH_REPORTING_IN_FULL[refusal], attempt.reportInFull());
            retries.refused(attempt);
        }
    }

    /**
     * The wait is dropped as soon as what was refused is not what is being attempted any more. This is what makes the
     * wait safe rather than a memo: an operator who has just repaired something is watching, and making them sit
     * through an interval that was chosen for the state before the repair would be its own kind of failure.
     */
    @Test
    public void testASituationThatChangesIsAttemptedStraightAway() {
        PacedRetries retries = new PacedRetries(A_WAIT_NO_TEST_OUTLIVES, A_WAIT_NO_TEST_OUTLIVES);

        retries.refused(retries.due(A_TABLESPACE, A_SITUATION));

        PacedRetries.Attempt afterTheRepair = retries.due(A_TABLESPACE, THE_SITUATION_AFTER_A_REPAIR);
        assertNotNull("a repair is not acted upon until the wait bought by the state before it is over",
                afterTheRepair);
        assertTrue("the refusal of something that has just changed is not reported in full, although nobody has been"
                + " told about this one yet", afterTheRepair.reportInFull());
    }

    /**
     * What a change of situation drops is the whole wait and not one step of it: what was refused before says nothing
     * about what is being attempted now, so a repair that does not work costs no more than a refusal of its own.
     */
    @Test
    public void testTheWaitStartsOverWhenTheSituationChanges() throws Exception {
        PacedRetries retries = new PacedRetries(FIRST_DELAY, MAX_DELAY);

        for (int refusal = 0; refusal < WAITS_A_REPEATING_REFUSAL_BUYS.length; refusal++) {
            retries.refused(waitUntilDue(retries, A_TABLESPACE, A_SITUATION));
        }

        PacedRetries.Attempt afterTheRepair = retries.due(A_TABLESPACE, THE_SITUATION_AFTER_A_REPAIR);
        assertNotNull("the wait that the refusals of what was there before had grown to still holds back the attempt"
                + " at what is there now", afterTheRepair);
        assertEquals("the attempt that follows a repair carries the wait the refusals before it had grown to, so"
                + " something that is refused right after a repair is treated as a repeat of what the repair was"
                + " about", FIRST_DELAY, afterTheRepair.retryIn());
    }

    /**
     * An attempt that finally goes through leaves nothing behind. The wait it grew to belonged to the refusals, and
     * the next time this key is refused it is the first refusal again.
     */
    @Test
    public void testAnAttemptThatSucceedsDropsTheWait() throws Exception {
        PacedRetries retries = new PacedRetries(FIRST_DELAY, MAX_DELAY);

        retries.refused(waitUntilDue(retries, A_TABLESPACE, A_SITUATION));
        retries.refused(waitUntilDue(retries, A_TABLESPACE, A_SITUATION));
        retries.succeeded(A_TABLESPACE);

        PacedRetries.Attempt afterTheSuccess = retries.due(A_TABLESPACE, A_SITUATION);
        assertNotNull("an attempt that went through still holds the next one back", afterTheSuccess);
        assertEquals("the wait that the refusals before a success had grown to is still there afterwards, so the"
                + " first refusal of something that has been working ever since is treated as a repeat",
                FIRST_DELAY, afterTheSuccess.retryIn());
    }

    /**
     * One wait per key. The attempts are about things that fail and are repaired one at a time, so a refusal of one of
     * them must not slow down the attempts at any other.
     */
    @Test
    public void testEachKeyIsHeldBackOnItsOwn() {
        PacedRetries retries = new PacedRetries(A_WAIT_NO_TEST_OUTLIVES, A_WAIT_NO_TEST_OUTLIVES);

        retries.refused(retries.due(A_TABLESPACE, A_SITUATION));

        assertNotNull("the refusal of one key holds back the attempts at every other one", retries.due(
                ANOTHER_TABLESPACE, A_SITUATION));
    }

    /**
     * The keys that do not exist any more are forgotten, and the ones that still do keep the wait they had. Nothing is
     * ever removed from this by the attempts themselves, so this is the only thing that keeps a node that creates and
     * drops tablespaces from holding a wait for every one of them it ever refused.
     */
    @Test
    public void testTheKeysThatAreNotThereAnyMoreAreForgotten() {
        PacedRetries retries = new PacedRetries(A_WAIT_NO_TEST_OUTLIVES, A_WAIT_NO_TEST_OUTLIVES);

        retries.refused(retries.due(A_TABLESPACE, A_SITUATION));
        retries.refused(retries.due(ANOTHER_TABLESPACE, A_SITUATION));
        retries.forgetAllExcept(Collections.singletonList(A_TABLESPACE));

        assertNull("a key that still exists lost the wait it had, so a refusal that keeps repeating is paid for in"
                + " full every time anything else disappears", retries.due(A_TABLESPACE, A_SITUATION));
        assertNotNull("a key that does not exist any more still holds a wait", retries.due(ANOTHER_TABLESPACE,
                A_SITUATION));
    }

    /**
     * Forgetting everything is what a node with no tablespace left does, and it has to leave nothing behind.
     */
    @Test
    public void testForgettingEveryKeyLeavesNothingBehind() {
        PacedRetries retries = new PacedRetries(A_WAIT_NO_TEST_OUTLIVES, A_WAIT_NO_TEST_OUTLIVES);

        retries.refused(retries.due(A_TABLESPACE, A_SITUATION));
        retries.refused(retries.due(ANOTHER_TABLESPACE, A_SITUATION));
        retries.forgetAllExcept(Collections.emptyList());

        for (String key : Arrays.asList(A_TABLESPACE, ANOTHER_TABLESPACE)) {
            assertNotNull("key " + key + " still holds a wait although nothing is left of what it was about",
                    retries.due(key, A_SITUATION));
        }
    }

    /**
     * A wait that is over is over for good: once the attempt it was holding back has been made, asking again keeps
     * giving an attempt, for as long as nobody refuses one. An answer that was refused once is not remembered, and
     * this is what says so.
     */
    @Test
    public void testAWaitThatIsOverHoldsNothingBackAnyMore() throws Exception {
        PacedRetries retries = new PacedRetries(FIRST_DELAY, MAX_DELAY);

        retries.refused(retries.due(A_TABLESPACE, A_SITUATION));
        waitUntilDue(retries, A_TABLESPACE, A_SITUATION);

        assertNotNull("the attempt that a wait was holding back is held back again although it was never refused"
                + " again, so an attempt that keeps being asked for is made once and then remembered",
                retries.due(A_TABLESPACE, A_SITUATION));
    }

    /**
     * Waits for the attempt at a key to be due and hands it over. How long that takes is not what any of these tests
     * is about: they are about what the attempt says, so this waits for as long as it has to.
     */
    private static PacedRetries.Attempt waitUntilDue(
            PacedRetries retries, String key, Object situation
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TIME_GIVEN_TO_AN_ATTEMPT;
        PacedRetries.Attempt attempt = retries.due(key, situation);
        while (attempt == null) {
            assertFalse("the attempt at " + key + " was still held back " + TIME_GIVEN_TO_AN_ATTEMPT + " ms after a"
                    + " refusal that buys a wait of a few milliseconds, so a refusal holds an attempt back for good",
                    System.currentTimeMillis() > deadline);
            Thread.sleep(1);
            attempt = retries.due(key, situation);
        }
        return attempt;
    }
}
