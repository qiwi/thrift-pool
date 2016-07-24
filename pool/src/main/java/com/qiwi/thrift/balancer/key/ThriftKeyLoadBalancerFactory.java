package com.qiwi.thrift.balancer.key;

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
import java.util.function.ToLongFunction;

/**
 * Usage examples in com.qiwi.thrift.demo.BalancerSyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 */
@Named
@Singleton
public class ThriftKeyLoadBalancerFactory {
    private final ThriftKeyLoadBalancer.Factory balancerFactory;
    private final SyncClientFactory syncClientFactory;
    private final AsyncClientFactory asyncClientFactory;
    private final ReflectConfigurator reflectConfigurator;

    @Inject
    public ThriftKeyLoadBalancerFactory(
            ThriftKeyLoadBalancer.Factory balancerFactory,
            SyncClientFactory syncClientFactory,
            AsyncClientFactory asyncClientFactory,
            ReflectConfigurator reflectConfigurator
    ) {
        this.balancerFactory = balancerFactory;
        this.syncClientFactory = syncClientFactory;
        this.asyncClientFactory = asyncClientFactory;
        this.reflectConfigurator = reflectConfigurator;
    }

    public <K, I, T extends TServiceClient> ThriftKeySyncClient<K, I> createSyncClientWithStringKey(
            Class<I> interfaceClass,
            ThriftKeyBalancerConfig.Builder configBuilder,
            Function<K, String> keyMapper
    ){
        return createSyncClient(
                interfaceClass,
                configBuilder,
                key -> ConsistentHash.halfMd5Hash(keyMapper.apply(key))
        );
    }

    public <K, I, T extends TServiceClient> ThriftKeySyncClient<K, I> createSyncClient(
            Class<I> interfaceClass,
            ThriftKeyBalancerConfig.Builder configBuilder,
            ToLongFunction<K> keyMapper
    ){
        SyncClientClassInfo<I, T> classInfo = reflectConfigurator.createSync(interfaceClass);
        TServiceClientFactory<T> clientCreator = classInfo.getClientFactory();
        Predicate<I> validator = classInfo.getValidator().orElseThrow(() -> new IllegalArgumentException(
                                "Class " + interfaceClass.getName()
                                + " not have method boolean healthCheck();"
                        ));

        ThriftKeyBalancerConfig config = configBuilder.build();
        Function<ThriftClientConfig, ThriftSyncClient<I, T>> clientFactory = clientConfig -> syncClientFactory.createRawClient(
                interfaceClass,
                clientConfig,
                clientCreator,
                Optional.of(validator)
        );
        ThriftKeyLoadBalancer<K, I, ThriftSyncClient<I, T>> balancer = balancerFactory.create(
                ThriftUtils.getThriftServiceName(interfaceClass, config.getSubServiceName()),
                config,
                clientFactory,
                keyMapper
        );
        ThriftKeySyncClientImp<K, I, T> client = new ThriftKeySyncClientImp<K, I, T>(balancer, config, interfaceClass);
        balancer.scheduleConfigReload(configBuilder, client::reconfigureAsBalancer);
        return client;
    }

    public <K, I, T extends TAsyncClient> ThriftKeyAsyncClient<K, I> createAsyncClientWithStringKey(
            Class<I> interfaceClass,
            ThriftKeyBalancerConfig.Builder configBuilder,
            Function<K, String> keyMapper
    ){
        return createAsyncClient(
                interfaceClass,
                configBuilder,
                key -> ConsistentHash.halfMd5Hash(keyMapper.apply(key))
        );
    }

    public <K, I, T extends TAsyncClient> ThriftKeyAsyncClient<K, I> createAsyncClient(
            Class<I> interfaceClass,
            ThriftKeyBalancerConfig.Builder configBuilder,
            ToLongFunction<K> keyMapper
    ){
        AsyncClientClassInfo<I, T> classInfo = reflectConfigurator.createAsync(interfaceClass);
        BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientCreator = classInfo.getClientFactory();
        ThriftAsyncVerifier<I> validator = classInfo.getValidator().orElseThrow(() -> new IllegalArgumentException(
                        "Class " + interfaceClass.getName()
                                + " not have method boolean healthCheck();"
                ));

        ThriftKeyBalancerConfig config = configBuilder.build();
        Function<ThriftClientConfig, ThriftPoolAsyncClient<I, T>> clientFactory = clientConfig -> (ThriftPoolAsyncClient<I, T>)asyncClientFactory.create(
                interfaceClass,
                clientConfig,
                clientCreator,
                Optional.of(validator)
        );
        ThriftKeyLoadBalancer<K, I, ThriftPoolAsyncClient<I, T>> balancer = balancerFactory.create(
                ThriftUtils.getThriftServiceName(interfaceClass, config.getSubServiceName()),
                config,
                clientFactory,
                keyMapper
        );
        ThriftKeyAsyncClientImp<K, I, T> client = new ThriftKeyAsyncClientImp<K, I, T>(balancer, config);
        balancer.scheduleConfigReload(configBuilder, client::reconfigureAsBalancer);
        return client;
    }


}
