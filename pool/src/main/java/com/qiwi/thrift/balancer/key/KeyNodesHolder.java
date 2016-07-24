package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.NodesHolder;
import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;
import com.qiwi.thrift.balancer.load.ThriftDcBalancer;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;

import java.util.function.Function;
import java.util.function.ToLongFunction;

public class KeyNodesHolder<K, I, C extends ThriftClient<I>> extends NodesHolder<I, C> {
    private final ToLongFunction<K> keyMapper;

    KeyNodesHolder(
            Function<ThriftClientConfig, C> clientFactory,
            String serviceName,
            ToLongFunction<K> keyMapper
    ) {
        super(clientFactory, serviceName);
        this.keyMapper = keyMapper;
    }

    @Override
    protected ThriftDcBalancer<I, C> createDcBalancer(String name) {
        return new ThriftDcKeyBalancer<>(
                getServiceName(),
                name,
                (ThriftKeyBalancerConfig) getConfig(),
                keyMapper
        );
    }


    @Override
    public void reconfigure(ThriftBalancerConfig config) {
        super.reconfigure((ThriftKeyBalancerConfig)config);
    }
}
