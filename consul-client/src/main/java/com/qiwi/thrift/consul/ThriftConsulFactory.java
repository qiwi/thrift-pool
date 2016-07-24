package com.qiwi.thrift.consul;

import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;
import com.qiwi.thrift.balancer.load.ThriftLoadBalancerFactory;
import com.qiwi.thrift.consul.ThriftServiceDescription.Builder;
import com.qiwi.thrift.pool.ThriftAsyncClient;
import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.async.TAsyncClient;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;


/**
 * Demo code in ThriftConsulDemoTest
 */
@Named
@Singleton
public class ThriftConsulFactory {
    private final ConsulService consulService;
    private final ThriftLoadBalancerFactory factory;

    @Inject
    public ThriftConsulFactory(
            ConsulService consulService,
            ThriftLoadBalancerFactory factory
    ) {
        this.consulService = consulService;
        this.factory = factory;
    }

    public <I, T extends TServiceClient> I create(
            Class<I> clientInterfaceClazz,
            ThriftBalancerConfig.MethodOfFailureHandling failureHandling
    ) {
        return create(
                new Builder<>(clientInterfaceClazz).build(),
                new ThriftBalancerConfig.Builder(failureHandling)
        );
    }

    public <I, T extends TServiceClient> I create(
            Class<I> clientInterfaceClazz,
            String clusterName,
            ThriftBalancerConfig.Builder builder
    ) {
        return create(
                new Builder<>(clientInterfaceClazz)
                        .setClusterName(clusterName)
                        .setSubServiceName(builder.getSubServiceName())
                        .build(),
                builder
        );
    }


    public <I, T extends TServiceClient> I create(
            ThriftServiceDescription<I> service,
            ThriftBalancerConfig.Builder configBuilder
    ) {
        setupParameterSource(service, configBuilder);
        return factory.createSyncClient(service.getInterfaceClass(), configBuilder);
    }


    public <I, T extends TAsyncClient> ThriftAsyncClient<I> createAsync(
            Class<I> clientInterfaceClazz,
            ThriftBalancerConfig.MethodOfFailureHandling failureHandling
    ) {
        return createAsync(
                new Builder<>(clientInterfaceClazz)
                        .build(),
                new ThriftBalancerConfig.Builder(failureHandling)
        );
    }

    public <I, T extends TAsyncClient> ThriftAsyncClient<I> createAsync(
            Class<I> clientInterfaceClazz,
            String clusterName,
            ThriftBalancerConfig.Builder builder
    ) {
        return createAsync(
                new Builder<>(clientInterfaceClazz)
                        .setClusterName(clusterName)
                        .setSubServiceName(builder.getSubServiceName())
                        .build(),
                builder
        );
    }

    public <I, T extends TAsyncClient> ThriftAsyncClient<I> createAsync(
            ThriftServiceDescription<I> service,
            ThriftBalancerConfig.Builder configBuilder
    ) {
        setupParameterSource(service, configBuilder);
        return factory.createAsyncClient(service.getInterfaceClass(), configBuilder);
    }

    private <I> void setupParameterSource(ThriftServiceDescription<I> service, ThriftBalancerConfig.Builder builder){
        ParameterSource parameterSource = consulService.getParameters(
                service,
                builder.getParameterSource()
        );
        builder.fromParameters(ParameterSource.subpath("thrift.", parameterSource));
        builder.setServersSupplier(() -> consulService.getServices(service));
        if (ThriftClientAddress.DEFAULT_DC_NAME_CLIENT.equals(builder.getPreferredDc())) {
            builder.setPreferredDc(consulService.getDcName().orElse(ThriftClientAddress.DEFAULT_DC_NAME_CLIENT));
        }
     }
}
