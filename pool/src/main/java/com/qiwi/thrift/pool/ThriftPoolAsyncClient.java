package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.pool.imp.MultiplexProtocolFactory;
import com.qiwi.thrift.tracing.TCompactTracedProtocol;
import com.qiwi.thrift.tracing.ThriftTraceMode;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftConnectionException;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * Создаётся через AsyncClientFactory
 * @param <T>
 */
public class ThriftPoolAsyncClient<I, T extends TAsyncClient>
    extends ThriftPoolAbstractClient<I, ThriftClientAsyncContainer<I>>
        implements ThriftAsyncClient<I> {
    private static final Logger log = LoggerFactory.getLogger(ThriftPoolAsyncClient.class);

    private final AsyncPoolObjectFactory<I, T> factory;
    private final TAsyncClientManager manager;
    private final ThriftCallType callType;

    ThriftPoolAsyncClient(
            Class<I> interfaceClazz,
            String serviceName,
            GenericObjectPool<ThriftClientAsyncContainer<I>> pool,
            Supplier<ThriftClientAddress> addressSupplier,
            ThriftClientConfig config,
            ThriftAsyncVerifier<I> validator,
            AsyncPoolObjectFactory<I, T> factory,
            TAsyncClientManager manager
    ) {
        super(interfaceClazz, serviceName, pool, addressSupplier, config, validator);
        this.factory = factory;
        this.manager = manager;
        this.callType = config.isBalancerClient()? ThriftCallType.ASYNC_BALANCER: ThriftCallType.ASYNC_CLIENT;
    }

    @Override
    public <R> CompletableFuture<R> execAsync(
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function
    ) {
        return execAsync(resultType, function, pool.getMaxWaitMillis());
    }

    public <R> CompletableFuture<R> execAsync(
            Class<R> resultType,
            ThriftAsyncFunction<I, ?> function,
            long timeoutMillis
    ) {
        config.getRequestReporter().requestBegin(serviceName, "async", callType);
        ThriftClientAsyncContainer<I> client;
        try {
            client = pool.borrowObject(timeoutMillis);
        } catch (RuntimeException e) {
            config.getRequestReporter().requestEnd(
                    serviceName,
                    function.getClass().getSimpleName(),
                    callType,
                    ThriftRequestStatus.CONNECTION_ERROR,
                    TimeUnit.MILLISECONDS.toNanos(pool.getMaxWaitMillis()),
                    Optional.of(e)
            );
            throw e;
        } catch (Exception e) {
            config.getRequestReporter().requestEnd(
                    serviceName,
                    function.getClass().getSimpleName(),
                    callType,
                    ThriftRequestStatus.CONNECTION_ERROR,
                    TimeUnit.MILLISECONDS.toNanos(pool.getMaxWaitMillis()),
                    Optional.of(e)
            );
            throw new ThriftConnectionException("Unable to connect to server " + addressSupplier.get(), e);
        }
        try {
            return client.execAsync(resultType, function, config.getRequestReporter());
        } catch (Exception e) {
            client.close();
            throw new ThriftConnectionException("Unable to connect to server " + client, e);
        } catch (Error e) {
            client.close();
            throw e;
        }
    }


    @Override
    public void reconfigure(ThriftClientConfig clientConfig) {
        factory.reconfigure(clientConfig);
        pool.setConfig(clientConfig.createPoolConfig(
                getServiceName(),
                getAddress(),
                clientConfig.isBalancerClient()? ThriftCallType.ASYNC_BALANCER: ThriftCallType.ASYNC_CLIENT
        ));
    }

    @Override
    public void close() {
        manager.stop();
        super.close();
    }

    static class AsyncPoolObjectFactory<I, T extends TAsyncClient> extends BasePooledObjectFactory<ThriftClientAsyncContainer<I>> {
        private static final Logger log = LoggerFactory.getLogger(AsyncPoolObjectFactory.class);

        private final Supplier<ThriftClientAddress> clientAddress;
        private final String serviceName;// только для логирования
        private final TAsyncClientManager manager;
        private final BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientCreator;
        private final Predicate<I> validator;
        private volatile ThriftClientConfig config;
        private volatile GenericObjectPool<ThriftClientAsyncContainer<I>> poolReference;

        public AsyncPoolObjectFactory(
                Supplier<ThriftClientAddress> clientAddress,
                String serviceName,
                TAsyncClientManager manager,
                BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientCreator,
                ThriftAsyncVerifier<I> validator,
                ThriftClientConfig config
        ) {
            this.clientAddress = clientAddress;
            this.serviceName = serviceName;
            this.manager = manager;
            this.clientCreator = clientCreator;
            this.validator = validator;
            this.config = config;
        }

        public void setPoolReference(GenericObjectPool<ThriftClientAsyncContainer<I>> poolReference) {
            this.poolReference = poolReference;
        }

        public void reconfigure(ThriftClientConfig config) {
            this.config = config;
        }

        @Override
        public ThriftClientAsyncContainer<I> create() throws Exception {
            ThriftClientAddress address = clientAddress.get();
            TNonblockingSocket socket = new TNonblockingSocket(address.getHost(), address.getPort());


            TProtocolFactory protocolFactory = new MultiplexProtocolFactory(
                    // Ддя асинхронного клиента применяется только ограничение на самый большой блоб/массив
                    new TCompactTracedProtocol.Factory(
                            config.getMaxFrameSizeBytes(),
                            config.getMaxCollectionItemCount(),
                            config.getTraceMode()
                                    .orElseGet(() -> address.getTraceMode().orElse(ThriftTraceMode.DISABLED))
                    ),
                    serviceName
            );
            TAsyncClientFactory<T> factory = clientCreator.apply(manager, protocolFactory);
            T asyncClient = factory.getAsyncClient(socket);
            asyncClient.setTimeout(config.getRequestTimeout().toMillis());
            I client = (I) asyncClient;

            return new ThriftClientAsyncContainer<I>(
                    socket,
                    poolReference,
                    config.getNeedCircuitBreakOnException(),
                    address,
                    serviceName,
                    client,
                    config.isBalancerClient()? ThriftCallType.ASYNC_BALANCER: ThriftCallType.ASYNC_CLIENT
            );
        }

        @Override
        public PooledObject<ThriftClientAsyncContainer<I>> wrap(ThriftClientAsyncContainer<I> obj) {
            return new DefaultPooledObject<>(obj);
        }

        @Override
        public void destroyObject(PooledObject<ThriftClientAsyncContainer<I>> p) throws Exception {
            super.destroyObject(p);
            p.getObject().closeClient();
        }

        @Override
        public boolean validateObject(PooledObject<ThriftClientAsyncContainer<I>> p) {
            ThriftClientAsyncContainer<I> object = p.getObject();
            TAsyncClient clientA = (TAsyncClient) object.client();
            long oldTimeout = clientA.getTimeout();
            clientA.setTimeout(config.getConnectTimeout().toMillis());
            try {
                boolean result = object.validate()
                        && validator.test(object.client())
                        && super.validateObject(p);
                if (!result) {
                    log.info("Validation of connection to {} failed", object.getClientAddress());
                }
                return result;
            } catch (Exception e) {
                log.warn("Connection unexpectedly closed {}", object.getClientAddress(), e);
                return false;
            } finally {
                clientA.setTimeout(oldTimeout);
            }
        }
    }
}
