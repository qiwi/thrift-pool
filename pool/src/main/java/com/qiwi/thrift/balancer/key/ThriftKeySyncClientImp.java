package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.balancer.load.ThriftBalancerAbstractClient;
import com.qiwi.thrift.pool.ThriftSyncClient;
import com.qiwi.thrift.utils.LambdaUtils;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.lang.reflect.*;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThriftKeySyncClientImp<K, I, T extends TServiceClient>
        extends ThriftBalancerAbstractClient<I, ThriftSyncClient<I, T>>
        implements ThriftKeySyncClient<K, I> {
    private final Constructor<? extends I> proxyConstructor;
    private final ThriftKeyLoadBalancer<K, I, ThriftSyncClient<I, T>> keyBalancer;

    public ThriftKeySyncClientImp(
            ThriftKeyLoadBalancer<K, I, ThriftSyncClient<I, T>> balancer,
            ThriftKeyBalancerConfig config,
            Class<I> interfaceClass
    ) {
        super(balancer, config);
        keyBalancer = balancer;
        Class<? extends I> proxyClass = (Class<? extends I>) Proxy.getProxyClass(
                interfaceClass.getClassLoader(),
                interfaceClass,
                Closeable.class //  Need because other thrift proxies implement this
        );
        try {
            proxyConstructor = proxyClass.getConstructor(InvocationHandler.class);
            proxyConstructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Proxy class not have proxy constructor, JVM error?");
        }
    }

    /**
     *
     * @return guaranteed what all invocations on returned proxy, executed on same node
     * @throws TTransportException - if no nodes available
     */
    @Override
    public I get() throws TTransportException{
        Optional<NodeStatus<I, ThriftSyncClient<I, T>>> statusOpt = balancer.get();
        if (!statusOpt.isPresent()) {
            throw new TTransportException(
                    "No nodes available. Failed nodes: "
                    + balancer.getFailedNodes().collect(Collectors.toList())
            );
        }
        return createProxy(statusOpt.get());
    }


    /**
     *
     * @return guaranteed what all invocations on returned proxy executed on same node
     * @throws TTransportException - if no nodes available
     */
    public I get(K key) throws TTransportException {
        Optional<NodeStatus<I, ThriftSyncClient<I, T>>> statusOpt = keyBalancer.get(key);
        if (!statusOpt.isPresent()) {
            throw new TTransportException(
                    "No nodes available for key " + key + ". Failed nodes: "
                    + balancer.getFailedNodes().collect(Collectors.toList())
            );
        }
        return createProxy(statusOpt.get());
    }

    @Override
    public Stream<I> getQuorum(K key) throws TTransportException {
        NodeStatus<I, ThriftSyncClient<I, T>>[] array = keyBalancer.getQuorum(key).toArray(size -> new NodeStatus[size]);
        if (array.length == 0) {
            throw new TTransportException(
                    "No nodes available for key " + key + ". Failed nodes: "
                    + balancer.getFailedNodes().collect(Collectors.toList())
            );
        }
        return Stream.of(array).map(this::createProxy);
    }

    private I createProxy(NodeStatus<I, ThriftSyncClient<I, T>> status) {
        Handler<I, T> handler = new Handler<>(status, getMaxWaitMillis());
        try {
            return proxyConstructor.newInstance(handler);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            throw LambdaUtils.propagate(ex);
        }
    }

    private static class Handler<I, T extends TServiceClient> implements InvocationHandler{
        private final NodeStatus<I, ThriftSyncClient<I, T>> nodeStatus;
        private final long maxWaitMillis;

        private Handler(NodeStatus<I, ThriftSyncClient<I, T>> nodeStatus, long maxWaitMillis) {
            this.nodeStatus = nodeStatus;
            this.maxWaitMillis = maxWaitMillis;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                return null;
            }
            return nodeStatus.getClient().invoke(method, args, nodeStatus.isWorking()? maxWaitMillis: 0);
        }
    }
}
