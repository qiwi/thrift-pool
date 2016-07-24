package com.qiwi.thrift.pool;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncMethodCall;

@FunctionalInterface
public interface ThriftAsyncFunction<I, C extends TAsyncMethodCall> {
    void call(I client, AsyncMethodCallback<C> callback) throws TException;
}
