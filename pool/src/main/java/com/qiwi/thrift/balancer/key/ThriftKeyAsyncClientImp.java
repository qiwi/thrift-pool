package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.balancer.load.ThriftBalancerAsyncClient;
import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;
import com.qiwi.thrift.pool.ThriftAsyncFunction;
import com.qiwi.thrift.pool.ThriftPoolAsyncClient;
import com.qiwi.thrift.utils.ThriftConnectionException;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.thrift.async.TAsyncClient;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThriftKeyAsyncClientImp<K, I, T extends TAsyncClient>
        extends ThriftBalancerAsyncClient<I, T>
        implements ThriftKeyAsyncClient<K, I> {
    private final ThriftKeyLoadBalancer<K, I, ThriftPoolAsyncClient<I, T>> keyBalancer;

    ThriftKeyAsyncClientImp(
            ThriftKeyLoadBalancer<K, I, ThriftPoolAsyncClient<I, T>> balancer,
            ThriftBalancerConfig config
    ) {
        super(balancer, config);
        this.keyBalancer = balancer;
    }

    @Override
    public <R> CompletableFuture<R> execOnKey(
            K key,
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) throws NoSuchElementException, ThriftConnectionException {
        NodeStatus<I, ThriftPoolAsyncClient<I, T>> status = keyBalancer.get(key)
                .orElseThrow(() -> new NoSuchElementException(
                        "No nodes available. Failed nodes: "
                        + balancer.getFailedNodes().collect(Collectors.toList()))
                );
        return status.getClient().execAsync(
                resultType,
                function,
                status.isWorking()? getMaxWaitMillis(): 0
        );
    }

    @Override
    public <R> CompletableFuture<R> execOnQuorum(
            K key,
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) throws NoSuchElementException, ThriftConnectionException {
        try {
            Stream<CompletableFuture<R>> futures = keyBalancer.getQuorum(key)
                    .map(status -> status.getClient().execAsync(
                            resultType,
                            function,
                            status.isWorking() ? getMaxWaitMillis() : 0
                    ));
            return ThriftUtils.firstSuccess(futures);
        } catch (NoSuchElementException ex) {
            throw new NoSuchElementException(
                            "No nodes available. Failed nodes: "
                            + balancer.getFailedNodes().collect(Collectors.toList())
                    );
        }
    }
}
