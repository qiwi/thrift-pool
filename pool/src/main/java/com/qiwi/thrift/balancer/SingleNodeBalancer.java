package com.qiwi.thrift.balancer;

import java.util.Optional;
import java.util.stream.Stream;

public class SingleNodeBalancer<I> implements Balancer<I> {
    private final Optional<I> node;

    public SingleNodeBalancer(I node) {
        this.node = Optional.of(node);
    }

    @Override
    public Optional<I> get() {
        return node;
    }

    @Override
    public Stream<I> nodes() {
        return Stream.of(node.get());
    }

    @Override
    public void reBalance() {
    }
}
