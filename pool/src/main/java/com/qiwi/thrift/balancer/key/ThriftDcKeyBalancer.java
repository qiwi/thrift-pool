package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.Balancer;
import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.balancer.load.ThriftDcBalancer;
import com.qiwi.thrift.pool.ThriftClient;

import java.util.List;
import java.util.Optional;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

public class ThriftDcKeyBalancer<K, I, C extends ThriftClient<I>> extends ThriftDcBalancer<I, C> {
    private final ToLongFunction<K> keyMapper;

    protected ThriftDcKeyBalancer(
            String serviceName,
            String dcName,
            ThriftKeyBalancerConfig config,
            ToLongFunction<K> keyMapper
    ) {
        super(serviceName, dcName, config);
        this.keyMapper = keyMapper;
    }

    @Override
    protected Balancer<NodeStatus<I, C>> createBalancer(
            List<NodeStatus<I, C>> items, BalancerType type
    ) {
        Balancer<NodeStatus<I, C>> balancer = super.createBalancer(items, type);
        return new BalancerConsistentHash<>(
                items,
                balancer,
                node -> node.getAddress().toShortString(),
                keyMapper,
                ((ThriftKeyBalancerConfig)getConfig()).getQuorumSize()
        );
    }

    public Optional<NodeStatus<I, C>> getByKey(K key) {
        return ((BalancerConsistentHash<K, NodeStatus<I, C>>)getBalancer()).get(key);
    }

    public Optional<NodeStatus<I, C>> getByKeyRecovery(K key) {
        Optional<NodeStatus<I, C>> nodeStatus = ((BalancerConsistentHash<K, NodeStatus<I, C>>) getBalancerRecovery()).get(key);
        return nodeStatus.filter(NodeStatus::shouldSendTestRequest);
    }

    public Stream<NodeStatus<I, C>> getQuorumByKey(K key) {
        return ((BalancerConsistentHash<K, NodeStatus<I, C>>)getBalancer()).getQuorum(key);
    }

    public Optional<Stream<NodeStatus<I, C>>> getQuorumByKeyRecovery(K key) {
        BalancerConsistentHash<K, NodeStatus<I, C>> recovery = (BalancerConsistentHash<K, NodeStatus<I, C>>) getBalancerRecovery();
        NodeStatus<I, C>[] array = recovery.getQuorum(key).toArray(NodeStatus[]::new);
        boolean shouldTest = false;
        boolean available = true;
        for (NodeStatus<I, C> status : array) {
            if (status.shouldSendTestRequest()) {
                shouldTest = true;
            } else if (!status.isInRing()) {
                available = false;
            }
        }

        if (shouldTest && available) {
            return Optional.of(Stream.of(array));
        } else {
            return Optional.empty();
        }
    }
}
