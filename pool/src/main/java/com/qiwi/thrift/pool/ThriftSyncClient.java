package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.tracing.TCompactTracedProtocol;
import com.qiwi.thrift.tracing.ThriftTraceMode;
import com.qiwi.thrift.utils.TTimeoutException;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Usage examples in com.qiwi.thrift.demo.SyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 * @param <I>
 * @param <T>
 */
public class ThriftSyncClient<I, T extends TServiceClient>
        extends ThriftPoolAbstractClient<I, ThriftClientSyncContainer<I>>
        implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ThriftSyncClient.class);

    private final PoolObjectFactory<I, T> poolObjectFactory;
    private final ThriftCallType callType;

    ThriftSyncClient(
            Class<I> interfaceClass,
            String serviceName,
            GenericObjectPool<ThriftClientSyncContainer<I>> pool,
            Supplier<ThriftClientAddress> addressSupplier,
            ThriftClientConfig config,
            Predicate<I> validator,
            PoolObjectFactory<I, T> poolObjectFactory
    ) {
        super(interfaceClass, serviceName, pool, addressSupplier, config, validator);
        this.poolObjectFactory = poolObjectFactory;
        this.callType = config.isBalancerClient()? ThriftCallType.SYNC_BALANCER: ThriftCallType.SYNC_CLIENT;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        switch (method.getName()) {
            case "toString":
                if (args == null || args.length == 0) {
                    return toString();
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
        return invoke(method, args, pool.getMaxWaitMillis());
    }

    public Object invoke(Method method, Object[] args, long timeoutMillis) throws Throwable {
        String methodName = method.getName();
        config.getRequestReporter().requestBegin(serviceName, methodName, callType);
        ThriftClientSyncContainer<I> container;
        try {
            container = pool.borrowObject(timeoutMillis);
        } catch (NoSuchElementException ex) {
            config.getRequestReporter().requestEnd(
                    serviceName,
                    methodName,
                    callType,
                    ThriftRequestStatus.CONNECTION_ERROR,
                    TimeUnit.MILLISECONDS.toNanos(pool.getMaxWaitMillis()),
                    Optional.of(ex)
            );
            if (ex.getMessage() != null && ex.getMessage().contains("Timeout")) {
                throw new TTimeoutException("No connection available " + addressSupplier.get(), ex);
            } else {
                throw new TTransportException("Unable connect to " + addressSupplier.get(), ex);
            }
        } catch (Exception ex) {
            config.getRequestReporter().requestEnd(
                    serviceName,
                    methodName,
                    callType,
                    ThriftRequestStatus.CONNECTION_ERROR,
                    TimeUnit.MILLISECONDS.toNanos(pool.getMaxWaitMillis()),
                    Optional.of(ex)
            );
            throw new TTransportException("Unable connect to " + addressSupplier.get(), ex);
        }
        try {
            return container.invoke(method, args, config.getRequestReporter());
        } finally {
            container.close();
        }
    }

    public static <I> I makeProxy(Class<I> interfaceClass, InvocationHandler handler){
        @SuppressWarnings("unchecked")
        I instance = (I) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class[]{interfaceClass, Closeable.class},
                handler
        );
        return instance;
    }

    @Override
    public void reconfigure(ThriftClientConfig clientConfig) {
        poolObjectFactory.reconfigure(clientConfig);
        pool.setConfig(clientConfig.createPoolConfig(
                serviceName,
                getAddress(),
                clientConfig.isBalancerClient()? ThriftCallType.SYNC_BALANCER: ThriftCallType.SYNC_CLIENT
        ));
    }

    static class PoolObjectFactory<I, T extends TServiceClient>
            extends BasePooledObjectFactory<ThriftClientSyncContainer<I>> {
        private static final Logger log = LoggerFactory.getLogger(PoolObjectFactory.class);

        private final Class<?> clientInterfaceClazz;
        private final Supplier<ThriftClientAddress> clientAddress;
        private final TServiceClientFactory<T> factory;
        private final Predicate<I> validator;
        private final String thriftServiceName;
        private volatile ThriftClientConfig thriftClientConfig;
        private GenericObjectPool<ThriftClientSyncContainer<I>> poolReference = null;

        public PoolObjectFactory(
                Class<?> clientInterfaceClazz,
                Supplier<ThriftClientAddress> clientAddress,
                TServiceClientFactory<T> factory,
                Predicate<I> validator,
                ThriftClientConfig thriftClientConfig,
                String thriftServiceName
        ) {
            this.clientInterfaceClazz = clientInterfaceClazz;
            this.clientAddress = clientAddress;
            this.factory = factory;
            this.validator = validator;
            this.thriftClientConfig = thriftClientConfig;
            this.thriftServiceName = thriftServiceName;
        }


        public void setPoolReference(GenericObjectPool<ThriftClientSyncContainer<I>> poolReference) {
            this.poolReference = poolReference;
        }

        @Override
        public ThriftClientSyncContainer<I> create() throws Exception {
            ThriftClientAddress address = clientAddress.get();
            TSocket socket = null;
            try {

                socket = new TSocket(address.getHost(), address.getPort());
                socket.setConnectTimeout((int) thriftClientConfig.getConnectTimeout().toMillis());
                socket.setSocketTimeout((int) thriftClientConfig.getRequestTimeout().toMillis());
                if (thriftClientConfig.getSocketReceiveBufferSize() > 0) {
                    socket.getSocket().setReceiveBufferSize(thriftClientConfig.getSocketReceiveBufferSize());
                }
                if (thriftClientConfig.getSocketSendBufferSize() > 0) {
                    socket.getSocket().setSendBufferSize(thriftClientConfig.getSocketSendBufferSize());
                }
                socket.open();
                TFastFramedTransport framedTransport = new TFastFramedTransport(
                        socket,
                        TFastFramedTransport.DEFAULT_BUF_CAPACITY,
                        thriftClientConfig.getMaxFrameSizeBytes()
                );
                TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                        new TCompactTracedProtocol(
                                framedTransport,
                                thriftClientConfig.getMaxFrameSizeBytes(),
                                thriftClientConfig.getMaxCollectionItemCount(),
                                thriftClientConfig.getTraceMode()
                                        .orElseGet(() -> address.getTraceMode().orElse(ThriftTraceMode.DISABLED))
                        ),
                        thriftServiceName
                );
                I client = (I)factory.getClient(protocol);
                log.info("Connected to {} for interface {}", address, clientInterfaceClazz.getName());
                return new ThriftClientSyncContainer<I>(
                        framedTransport,
                        poolReference,
                        thriftClientConfig.getNeedCircuitBreakOnException(),
                        address,
                        thriftServiceName,
                        client,
                        thriftClientConfig.isBalancerClient()? ThriftCallType.SYNC_BALANCER: ThriftCallType.SYNC_CLIENT
                );
            } catch (Exception e){
                log.warn("Connection failed to {} for interface {}", address, clientInterfaceClazz.getName());
                if (socket != null) {
                    socket.close();
                }
                throw e;
            }
        }

        @Override
        public PooledObject<ThriftClientSyncContainer<I>> wrap(ThriftClientSyncContainer<I> obj) {
            return new DefaultPooledObject<>(obj);
        }

        @Override
        public void destroyObject(PooledObject<ThriftClientSyncContainer<I>> p) throws Exception {
            super.destroyObject(p);
            p.getObject().closeClient();
        }

        @Override
        public boolean validateObject(PooledObject<ThriftClientSyncContainer<I>> p) {
            ThriftClientSyncContainer<I> object = p.getObject();
            try {
                boolean result = object.validate()
                        && validator.test(object.client())
                        && super.validateObject(p);
                if (!result) {
                    log.info("Validation of connection to {} failed", object.getClientAddress());
                }
                return result;
            } catch (Exception e) {
                log.warn("Connection unexpectedly closed to address {}", object.getClientAddress(), e);
                return false;
            }
        }

        public void reconfigure(ThriftClientConfig newConfig) {
            thriftClientConfig = newConfig;
        }
    }
}
