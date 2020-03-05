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

package herddb.utils;

import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author francesco.caliumi
 */
public class TestUtils {

    /**
     * ** Copied from JUnit 4.13 ** *
     */
    public interface ThrowingRunnable {

        void run() throws Throwable;
    }

    /**
     * Asserts that {@code runnable} throws an exception of type
     * {@code expectedThrowable} when executed. If it does not throw an
     * exception, an {@link AssertionError} is thrown. If it throws the wrong
     * type of exception, an {@code AssertionError} is thrown describing the
     * mismatch; the exception that was actually thrown can be obtained by
     * calling {@link
     * AssertionError#getCause}.
     *
     * @param expectedThrowable the expected type of the exception
     * @param runnable          a function that is expected to throw an exception when
     *                          executed
     * @since 4.13
     */
    public static void assertThrows(Class<? extends Throwable> expectedThrowable, ThrowingRunnable runnable) {
        expectThrows(expectedThrowable, runnable);
    }

    /**
     * Asserts that {@code runnable} throws an exception of type
     * {@code expectedThrowable} when executed. If it does, the exception object
     * is returned. If it does not throw an exception, an {@link AssertionError}
     * is thrown. If it throws the wrong type of exception, an {@code
     * AssertionError} is thrown describing the mismatch; the exception that was
     * actually thrown can be obtained by calling
     * {@link AssertionError#getCause}.
     *
     * @param expectedThrowable the expected type of the exception
     * @param runnable          a function that is expected to throw an exception when
     *                          executed
     * @return the exception thrown by {@code runnable}
     * @since 4.13
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedThrowable, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable actualThrown) {
            if (expectedThrowable.isInstance(actualThrown)) {
                @SuppressWarnings("unchecked")
                T retVal = (T) actualThrown;
                return retVal;
            } else {
                String mismatchMessage = String.format("unexpected exception type thrown expected %s actual %s",
                        expectedThrowable.getSimpleName(), actualThrown.getClass().getSimpleName());

                // The AssertionError(String, Throwable) ctor is only available on JDK7.
                AssertionError assertionError = new AssertionError(mismatchMessage);
                assertionError.initCause(actualThrown);
                throw assertionError;
            }
        }
        String message = String.format("expected %s to be thrown, but nothing was thrown",
                expectedThrowable.getSimpleName());
        throw new AssertionError(message);
    }

    public static <T extends Throwable> void verifyThrows(Class<T> expectedThrowable, ThrowingRunnable runnable, Function<T, Boolean> verify) throws Exception {
        T t = expectThrows(expectedThrowable, runnable);
        if (!verify.apply(t)) {
            throw new AssertionError("Returned exception " + t + " did not very the assertion");
        }
    }

    public static void waitForCondition(Callable<Boolean> condition, Callable<Void> callback, int seconds) throws Exception {
        waitForCondition(condition, callback, seconds, null);
    }

    public static void waitForCondition(Callable<Boolean> condition, Callable<Void> callback, int seconds, String description) throws Exception {
        try {
            long _start = System.currentTimeMillis();
            long millis = seconds * 1000;
            while (System.currentTimeMillis() - _start <= millis) {
                if (condition.call()) {
                    return;
                }
                callback.call();
                Thread.sleep(100);
            }
        } catch (InterruptedException ee) {
            throw new AssertionError("test interrupted!", ee);
        } catch (Exception ee) {
            LOG.log(Level.SEVERE, "error during wait", ee);
            throw new AssertionError("error while evaluating condition:" + ee);
        }
        String d = description == null ? "" : " " + description;
        throw new AssertionError("condition not met in time!" + d);
    }

    private static final Logger LOG = Logger.getLogger(TestUtils.class.getName());

    public static final Callable<Void> NOOP = () -> null;

    public static void assertExceptionPresentInChain(Throwable t, Class<?> clazz) {
        if (!isExceptionPresentInChain(t, clazz)) {
            throw new AssertionError("exception didn't contain expected " + clazz.getSimpleName(), t);
        }
    }

    public static boolean isExceptionPresentInChain(Throwable t, Class clazz) {
        if (t == null) {
            return false;
        }
        if (clazz.isInstance(t)) {
            return true;
        }
        return isExceptionPresentInChain(t.getCause(), clazz);
    }

    public static <T extends Throwable> T getExceptionIfPresentInChain(Throwable t, Class<T> clazz) {
        if (t == null) {
            return null;
        }
        if (clazz.isInstance(t)) {
            return (T) t;
        }

        return getExceptionIfPresentInChain(t.getCause(), clazz);
    }

}
