package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.pool.ThriftAsyncClient;
import com.qiwi.thrift.pool.ThriftAsyncFunction;
import com.qiwi.thrift.utils.ThriftConnectionException;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

public interface ThriftKeyAsyncClient<K, I> extends ThriftAsyncClient<I> {
    <R> CompletableFuture<R> execOnKey(
            K key,
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) throws NoSuchElementException, ThriftConnectionException;

    <R> CompletableFuture<R> execOnQuorum(
            K key,
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) throws NoSuchElementException, ThriftConnectionException;

}
