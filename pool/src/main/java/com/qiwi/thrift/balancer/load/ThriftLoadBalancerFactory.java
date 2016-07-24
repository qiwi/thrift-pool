package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.pool.*;
import com.qiwi.thrift.reflect.AsyncClientClassInfo;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.reflect.SyncClientClassInfo;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Usage examples in com.qiwi.thrift.demo.BalancerSyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 */
@Named
@Singleton
public class ThriftLoadBalancerFactory {
    private ThriftLoadBalancer.Factory balancerFactory;
    private SyncClientFactory syncClientFactory;
    private AsyncClientFactory asyncClientFactory;
    private ReflectConfigurator reflectConfigurator;

    @Inject
    public ThriftLoadBalancerFactory(
            ThriftLoadBalancer.Factory balancerFactory,
            SyncClientFactory syncClientFactory,
            AsyncClientFactory asyncClientFactory,
            ReflectConfigurator reflectConfigurator
    ) {
        this.balancerFactory = balancerFactory;
        this.syncClientFactory = syncClientFactory;
        this.asyncClientFactory = asyncClientFactory;
        this.reflectConfigurator = reflectConfigurator;
    }


    public <I, T extends TServiceClient> I createSyncClient(
            Class<I> clientInterfaceClazz,
            ThriftBalancerConfig.Builder configBuilder
    ){
        SyncClientClassInfo<I, T> sync = reflectConfigurator.createSync(clientInterfaceClazz);
        return createSyncClient(
                clientInterfaceClazz,
                configBuilder,
                sync.getClientFactory(),
                sync.getValidator().orElseThrow(() -> new IllegalArgumentException(
                        "Class " + clientInterfaceClazz.getName()
                        + " not have method boolean healthCheck();"
                ))
        );
    }

    /**
     *
     * @param clientInterfaceClazz
     * @param configBuilder
     * @param factoryCreate
     * @param validator
     * @param <I>
     * @param <T>
     * @return
     * @deprecated Use {@link #createSyncClient(Class, ThriftBalancerConfig.Builder)} if healCheck method has on
     *   of flowing name  healthCheck(), ping(), isUp(), health(), test(). Name healthCheck is recommended
     */
    @Deprecated// Use createSyncClient(Class<I>, ThriftBalancerConfig.Builder) instead
    public <I, T extends TServiceClient> I createSyncClient(
            Class<I> clientInterfaceClazz,
            ThriftBalancerConfig.Builder configBuilder,
            TServiceClientFactory<T> factoryCreate,
            Predicate<I> validator
    ){
        ThriftBalancerSyncClient<I, T> handler = createSyncBalancer(
                clientInterfaceClazz,
                configBuilder,
                factoryCreate,
                validator
        );
        return ThriftSyncClient.makeProxy(clientInterfaceClazz, handler);
    }

    /**
     * Method to extending thrift pool
     * @param clientInterfaceClazz
     * @param configBuilder
     * @param factoryCreate
     * @param validator
     * @param <I>
     * @param <T>
     * @return
     */
    public <I, T extends TServiceClient> ThriftBalancerSyncClient<I, T> createSyncBalancer(
            Class<I> clientInterfaceClazz,
            ThriftBalancerConfig.Builder configBuilder,
            TServiceClientFactory<T> factoryCreate,
            Predicate<I> validator
    ){
        Function<ThriftClientConfig, ThriftSyncClient<I, T>> clientFactory =
                clientConfig -> syncClientFactory.createRawClient(
                        clientInterfaceClazz,
                        clientConfig,
                        factoryCreate,
                        Optional.of(validator)
                );
        ThriftBalancerConfig config = configBuilder.build();
        ThriftLoadBalancer<I, ThriftSyncClient<I, T>> balancer = balancerFactory.create(
                ThriftUtils.getThriftServiceName(clientInterfaceClazz, config.getSubServiceName()),
                config,
                clientFactory
        );
        ThriftBalancerSyncClient<I, T> client = new ThriftBalancerSyncClient<>(balancer, config);
        balancer.scheduleConfigReload(configBuilder, client::reconfigureAsBalancer);
        return client;
    }

    public <I, T extends TAsyncClient> ThriftAsyncClient<I> createAsyncClient(
            Class<I> clientInterfaceClazz,
            ThriftBalancerConfig.Builder configBuilder
    ){
        AsyncClientClassInfo<I, T> classInfo = reflectConfigurator.createAsync(clientInterfaceClazz);
        return createAsyncClient(
                clientInterfaceClazz,
                configBuilder,
                classInfo.getClientFactory(),
                classInfo.getValidator().orElseThrow(() -> new IllegalArgumentException(
                        "Class " + clientInterfaceClazz.getName()
                        + " not have method boolean healthCheck();"
                ))
        );
    }

    @Deprecated// Use createAsyncClient(Class<I>, ThriftBalancerConfig.Builder) instead
    public <I, T extends TAsyncClient> ThriftAsyncClient<I> createAsyncClient(
            Class<I> interfaceClass,
            ThriftBalancerConfig.Builder configBuilder,
            BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientCreator,
            ThriftAsyncVerifier<I> validator
    ){
        ThriftBalancerConfig config = configBuilder.build();
        Function<ThriftClientConfig, ThriftPoolAsyncClient<I, T>> clientFactory =
                clientConfig -> (ThriftPoolAsyncClient<I, T>)asyncClientFactory.create(
                        interfaceClass,
                        clientConfig,
                        clientCreator,
                        Optional.of(validator)
                );
        ThriftLoadBalancer<I, ThriftPoolAsyncClient<I, T>> balancer = balancerFactory.create(
                ThriftUtils.getThriftServiceName(interfaceClass, config.getSubServiceName()),
                config,
                clientFactory
        );
        ThriftBalancerAsyncClient<I, T> client = new ThriftBalancerAsyncClient<>(balancer, config);
        balancer.scheduleConfigReload(configBuilder, client::reconfigureAsBalancer);
        return client;
    }

}
