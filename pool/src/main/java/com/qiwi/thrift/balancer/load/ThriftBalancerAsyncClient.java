package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.pool.ThriftAsyncClient;
import com.qiwi.thrift.pool.ThriftAsyncFunction;
import com.qiwi.thrift.pool.ThriftPoolAsyncClient;
import org.apache.thrift.async.TAsyncClient;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ThriftBalancerAsyncClient<I, T extends TAsyncClient> extends ThriftBalancerAbstractClient<I, ThriftPoolAsyncClient<I, T>>
        implements ThriftAsyncClient<I> {
    protected ThriftBalancerAsyncClient(
            ThriftLoadBalancer<I, ThriftPoolAsyncClient<I, T>> balancer,
            ThriftBalancerConfig config
    ) {
        super(balancer, config);
    }

    @Override
    public <R> CompletableFuture<R> execAsync(
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) {
        NodeStatus<I, ThriftPoolAsyncClient<I, T>> status = balancer.get()
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
}
