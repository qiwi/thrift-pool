package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.Balancer;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BalancerConsistentHash<K, I> implements KeyBasedBalancer<K, I> {
    private final Balancer<I> balancer;
    private final ConsistentHash<K, Optional<I>> hash;

    public BalancerConsistentHash(
            List<I> nodes,
            Balancer<I> balancer,
            Function<I, String> nodeName,
            ToLongFunction<K> keyMapper,
            int quorumSize
    ) {
        this.balancer = balancer;
        if (nodes.isEmpty()) {
            hash = null;
        } else {
            List<Optional<I>> list = nodes.stream()
                            .map(node -> Optional.of(node))
                            .collect(Collectors.toList());
            hash = ConsistentHash.buildWithMd5ForNodeNames(
                    list,
                    optional -> nodeName.apply(optional.get()),
                    keyMapper,
                    quorumSize,
                    false
            );
        }
    }

    @Override
    public Optional<I> get(K key) {
        if (hash == null){
            return Optional.empty();
        } else {
            return hash.getNode(key);
        }
    }

    @Override
    public Stream<I> getQuorum(K key) {
        if (hash == null){
            return Stream.empty();
        } else {
            return hash.getQuorum(key).map(Optional::get);
        }
    }

    @Override
    public Optional<I> get() {
        return balancer.get();
    }

    @Override
    public Stream<I> nodes() {
        return balancer.nodes();
    }

    @Override
    public void reBalance() {
        balancer.reBalance();
    }
}
