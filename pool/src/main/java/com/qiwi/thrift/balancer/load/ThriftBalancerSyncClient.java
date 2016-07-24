package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.pool.ThriftSyncClient;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Usage examples in com.qiwi.thrift.demo.BalancerSyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 * @param <I>
 * @param <T>
 */
public class ThriftBalancerSyncClient<I, T extends TServiceClient>
        extends ThriftBalancerAbstractClient<I, ThriftSyncClient<I, T>>
        implements InvocationHandler {

    ThriftBalancerSyncClient(
            ThriftLoadBalancer<I, ThriftSyncClient<I, T>> balancer,
            ThriftBalancerConfig config
    ) {
        super(balancer, config);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        switch (method.getName()) {
            case "toString":
                if (args == null || args.length == 0) {
                    return "BalancerFor:" + balancer.getServiceName();
                }
                break;
            case "close":
                if (args == null || args.length == 0) {
                    close();
                    return null;
                }
                break;
            case "hashCode":
                if (args == null || args.length == 0) {
                    return hashCode();
                }
                break;
            case "equals":
                if (args.length == 1) {
                    return proxy == args[0];
                }
                break;
        }
        Optional<NodeStatus<I, ThriftSyncClient<I, T>>> statusOpt = balancer.get();
        if (!statusOpt.isPresent()) {
            throw new TTransportException(
                    "No nodes available. Failed nodes: "
                    + balancer.getFailedNodes().collect(Collectors.toList())
            );
        }
        NodeStatus<I, ThriftSyncClient<I, T>> status = statusOpt.get();

        return status.getClient().invoke(
                method,
                args,
                status.isWorking()? getMaxWaitMillis(): 0
        );
    }

}
