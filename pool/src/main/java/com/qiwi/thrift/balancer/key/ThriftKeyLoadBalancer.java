package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;
import com.qiwi.thrift.balancer.load.ThriftDcBalancer;
import com.qiwi.thrift.balancer.load.ThriftLoadBalancer;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Usage examples in com.qiwi.thrift.demo.BalancerSyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 * @param <I>
 * @param <C>
 */
public class ThriftKeyLoadBalancer<K, I, C extends ThriftClient<I>>
        extends ThriftLoadBalancer<I, C>
        implements KeyBasedBalancer<K, NodeStatus<I, C>> {

    public ThriftKeyLoadBalancer(
            String serviceName,
            ThriftBalancerConfig config,
            ScheduledExecutorService reBalanceScheduler,
            ScheduledExecutorService reloadScheduler,
            KeyNodesHolder<K, I, C> nodesHolder
    ) {
        super(serviceName, config, reBalanceScheduler, reloadScheduler, nodesHolder);
    }



    @Override
    public Optional<NodeStatus<I, C>> get(K key) {
        Optional<NodeStatus<I, C>> optional = getRecoveryBalancer().get().flatMap(
                dc -> ((ThriftDcKeyBalancer<K, I, C>)dc).getByKeyRecovery(key)
        );
        if (optional.isPresent()) {
            return optional;
        }
        return getBalancer().get().flatMap(dc -> ((ThriftDcKeyBalancer<K, I, C>)dc).getByKey(key));
    }

    @Override
    public Stream<NodeStatus<I, C>> getQuorum(K key) {
        Optional<Stream<NodeStatus<I, C>>> optional = getRecoveryBalancer().get().flatMap(
                dc -> ((ThriftDcKeyBalancer<K, I, C>)dc).getQuorumByKeyRecovery(key)
        );
        if (optional.isPresent()) {
            return optional.get();
        }

        Optional<ThriftDcBalancer<I, C>> balancer = getBalancer().get();
        if (balancer.isPresent()) {
            return ((ThriftDcKeyBalancer<K, I, C>)balancer.get()).getQuorumByKey(key);
        } else {
            return Stream.empty();
        }
    }


    @Named
    @Singleton
    public static class Factory {
        public <K, I, C extends ThriftClient<I>> ThriftKeyLoadBalancer<K, I, C> create(
                String serviceName,
                ThriftKeyBalancerConfig config,
                Function<ThriftClientConfig, C> clientFactory,
                ToLongFunction<K> keyMapper
        ){
            ScheduledExecutorService reload = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "{\"balancer reload\":\"" + serviceName + "\"}")
            );
            ScheduledExecutorService reBalance = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "{\"balancer reBalance\":\"" + serviceName + "\"}")
            );
            KeyNodesHolder<K, I, C> nodesHolder = new KeyNodesHolder<>(clientFactory, serviceName, keyMapper);
            return newBalancer(serviceName, config, reBalance, reload, nodesHolder);
         }

        protected <K, I, C extends ThriftClient<I>> ThriftKeyLoadBalancer<K, I, C> newBalancer(
                String serviceName,
                ThriftKeyBalancerConfig config,
                ScheduledExecutorService reBalanceScheduler,
                ScheduledExecutorService reloadScheduler,
                KeyNodesHolder<K, I, C> nodesHolder
        ){
            return new ThriftKeyLoadBalancer<K, I, C>(
                     serviceName,
                     config,
                     reBalanceScheduler,
                     reloadScheduler,
                     nodesHolder
            );
       }
    }


}
