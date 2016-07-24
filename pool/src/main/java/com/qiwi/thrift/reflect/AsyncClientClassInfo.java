package com.qiwi.thrift.reflect;

import com.qiwi.thrift.pool.ThriftAsyncVerifier;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.Optional;
import java.util.function.BiFunction;

public class AsyncClientClassInfo<I, T extends TAsyncClient> {
    private final Class<I> clientInterfaceClazz;
    private final BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientFactory;
    private final Optional<ThriftAsyncVerifier<I>> validator;

    AsyncClientClassInfo(
            Class<I> clientInterfaceClazz,
            BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientFactory,
            Optional<ThriftAsyncVerifier<I>> validator
    ) {
        this.clientInterfaceClazz = clientInterfaceClazz;
        this.clientFactory = clientFactory;
        this.validator = validator;
    }

    public Class<I> getClientInterfaceClazz() {
        return clientInterfaceClazz;
    }

    public BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> getClientFactory() {
        return clientFactory;
    }

    public Optional<ThriftAsyncVerifier<I>> getValidator() {
        return validator;
    }
}
