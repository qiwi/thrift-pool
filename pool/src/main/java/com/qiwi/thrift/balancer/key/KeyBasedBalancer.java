package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.Balancer;

import java.util.Optional;
import java.util.stream.Stream;

public interface KeyBasedBalancer<K, I> extends Balancer<I> {
    Optional<I> get(K key);

    Stream<I> getQuorum(K key);
}
