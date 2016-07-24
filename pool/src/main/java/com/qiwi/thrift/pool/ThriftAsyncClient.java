package com.qiwi.thrift.pool;

import com.qiwi.thrift.utils.ThriftConnectionException;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Usage examples in com.qiwi.thrift.demo.AsyncDemoTest
 * @param <I>
 */
public interface ThriftAsyncClient<I> extends Closeable {
    <R> CompletableFuture<R> execAsync(
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) throws NoSuchElementException, ThriftConnectionException;
}
