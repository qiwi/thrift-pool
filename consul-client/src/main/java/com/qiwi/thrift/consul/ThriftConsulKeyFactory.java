package com.qiwi.thrift.consul;

import com.qiwi.thrift.balancer.key.ThriftKeyAsyncClient;
import com.qiwi.thrift.balancer.key.ThriftKeyBalancerConfig;
import com.qiwi.thrift.balancer.key.ThriftKeyLoadBalancerFactory;
import com.qiwi.thrift.balancer.key.ThriftKeySyncClient;
import com.qiwi.thrift.consul.ThriftServiceDescription.Builder;
import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftClientAddress;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Function;
import java.util.function.ToLongFunction;

@Named
@Singleton
public class ThriftConsulKeyFactory {

    private final ConsulService consulService;
    private final ThriftKeyLoadBalancerFactory factory;


    @Inject
    public ThriftConsulKeyFactory(ConsulService consulService,
                                  ThriftKeyLoadBalancerFactory factory) {
        this.consulService = consulService;
        this.factory = factory;
    }


    public <K, I> ThriftKeySyncClient<K, I> create(Class<I> clientInterfaceClazz,
                                                   String clusterName,
                                                   ThriftKeyBalancerConfig.Builder builder,
                                                   ToLongFunction<K> keyMapper) {
        return create(new Builder<>(clientInterfaceClazz).setClusterName(clusterName)
                                                         .setSubServiceName(builder.getSubServiceName())
                                                         .build(),
                      builder,
                      keyMapper);
    }

    public <K, I> ThriftKeySyncClient<K, I> create(ThriftServiceDescription<I> service,
                                                   ThriftKeyBalancerConfig.Builder configBuilder,
                                                   ToLongFunction<K> keyMapper) {
        setupParameterSource(service, configBuilder);
        return factory.createSyncClient(service.getInterfaceClass(), configBuilder, keyMapper);
    }


    public <K, I> ThriftKeySyncClient<K, I> createWithStringKey(Class<I> clientInterfaceClazz,
                                                                String clusterName,
                                                                ThriftKeyBalancerConfig.Builder builder,
                                                                Function<K, String> keyMapper) {
        return createWithStringKey(new Builder<>(clientInterfaceClazz).setClusterName(clusterName)
                                                                      .setSubServiceName(builder.getSubServiceName())
                                                                      .build(),
                                   builder,
                                   keyMapper);
    }

    public <K, I> ThriftKeySyncClient<K, I> createWithStringKey(ThriftServiceDescription<I> service,
                                                                ThriftKeyBalancerConfig.Builder configBuilder,
                                                                Function<K, String> keyMapper) {
        setupParameterSource(service, configBuilder);
        return factory.createSyncClientWithStringKey(service.getInterfaceClass(), configBuilder, keyMapper);
    }


    public <K, I> ThriftKeyAsyncClient<K, I> createAsync(Class<I> clientInterfaceClazz,
                                                         String clusterName,
                                                         ThriftKeyBalancerConfig.Builder builder,
                                                         ToLongFunction<K> keyMapper) {
        return createAsync(new Builder<>(clientInterfaceClazz).setClusterName(clusterName)
                                                              .setSubServiceName(builder.getSubServiceName())
                                                              .build(),
                           builder,
                           keyMapper);
    }

    public <K, I> ThriftKeyAsyncClient<K, I> createAsync(ThriftServiceDescription<I> service,
                                                         ThriftKeyBalancerConfig.Builder configBuilder,
                                                         ToLongFunction<K> keyMapper) {
        setupParameterSource(service, configBuilder);
        return factory.createAsyncClient(service.getInterfaceClass(), configBuilder, keyMapper);
    }


    public <K, I> ThriftKeyAsyncClient<K, I> createAsyncWithStringKey(Class<I> clientInterfaceClazz,
                                                                      String clusterName,
                                                                      ThriftKeyBalancerConfig.Builder builder,
                                                                      Function<K, String> keyMapper) {
        return createAsyncWithStringKey(new Builder<>(clientInterfaceClazz).setClusterName(clusterName)
                                                                           .setSubServiceName(builder.getSubServiceName())
                                                                           .build(),
                                        builder,
                                        keyMapper);
    }

    public <K, I> ThriftKeyAsyncClient<K, I> createAsyncWithStringKey(ThriftServiceDescription<I> service,
                                                                      ThriftKeyBalancerConfig.Builder configBuilder,
                                                                      Function<K, String> keyMapper) {
        setupParameterSource(service, configBuilder);
        return factory.createAsyncClientWithStringKey(service.getInterfaceClass(), configBuilder, keyMapper);
    }


    private <I> void setupParameterSource(ThriftServiceDescription<I> service,
                                          ThriftKeyBalancerConfig.Builder builder) {
        ParameterSource parameterSource = consulService.getParameters(service, builder.getParameterSource());
        builder.fromParameters(ParameterSource.subpath("thrift.", parameterSource));
        builder.setServersSupplier(() -> consulService.getServices(service));
        if (ThriftClientAddress.DEFAULT_DC_NAME_CLIENT.equals(builder.getPreferredDc())) {
            builder.setPreferredDc(consulService.getDcName().orElse(ThriftClientAddress.DEFAULT_DC_NAME_CLIENT));
        }
    }

}
