package com.qiwi.thrift.reflect;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;

import java.util.Optional;
import java.util.function.Predicate;

public class SyncClientClassInfo<I, T extends TServiceClient> {
    private final Class<I> clientInterfaceClazz;
    private final TServiceClientFactory<T> clientFactory;
    private final Optional<Predicate<I>> validator;

    public SyncClientClassInfo(
            Class<I> clientInterfaceClazz,
            TServiceClientFactory<T> clientFactory,
            Optional<Predicate<I>> validator
    ) {
        this.clientInterfaceClazz = clientInterfaceClazz;
        this.clientFactory = clientFactory;
        this.validator = validator;
    }

    public Class<I> getClientInterfaceClazz() {
        return clientInterfaceClazz;
    }

    public TServiceClientFactory<T> getClientFactory() {
        return clientFactory;
    }

    public Optional<Predicate<I>> getValidator() {
        return validator;
    }
}
