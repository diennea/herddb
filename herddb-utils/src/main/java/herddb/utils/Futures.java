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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Futures {

    private static final Function<Throwable, Exception> DEFAULT_EXCEPTION_HANDLER = cause -> {
        if (cause instanceof Exception) {
            return (Exception) cause;
        } else {
            return new Exception(cause);
        }
    };

    public static <T> CompletableFuture<T> exception(Throwable cause) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    public static <T> CompletableFuture<List<T>> collect(List<CompletableFuture<T>> futureList) {
        CompletableFuture<Void> finalFuture =
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
        return finalFuture.thenApply(result
                -> futureList
                        .stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    public static <T, ExceptionT extends Throwable> T result(
            CompletableFuture<T> future,
            long timeout,
            TimeUnit timeUnit) throws ExceptionT, TimeoutException, InterruptedException, Exception {
        return result(future, DEFAULT_EXCEPTION_HANDLER, timeout, timeUnit);
    }

     public static <T, ExceptionT extends Throwable> T result(
            CompletableFuture<T> future) throws ExceptionT, TimeoutException, InterruptedException, Exception {
        return result(future, DEFAULT_EXCEPTION_HANDLER);
    }

    public static <T, ExceptionT extends Throwable> T result(
            CompletableFuture<T> future,
            Function<Throwable, ExceptionT> exceptionHandler,
            long timeout,
            TimeUnit timeUnit) throws ExceptionT, TimeoutException, InterruptedException {
        try {
            return future.get(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            ExceptionT cause = exceptionHandler.apply(e.getCause());
            if (null == cause) {
                return null;
            } else {
                throw cause;
            }
        }
    }

    public static <T, ExceptionT extends Throwable> T result(
        CompletableFuture<T> future, Function<Throwable, ExceptionT> exceptionHandler) throws ExceptionT, InterruptedException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            ExceptionT cause = exceptionHandler.apply(e.getCause());
            if (null == cause) {
                return null;
            } else {
                throw cause;
            }
        }
    }
}
