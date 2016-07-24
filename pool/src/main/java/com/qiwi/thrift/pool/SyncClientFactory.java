package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.reflect.SyncClientClassInfo;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Usage examples in com.qiwi.thrift.demo.SyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 */
@Named
@Singleton
public class SyncClientFactory {
    private static final Logger log = LoggerFactory.getLogger(SyncClientFactory.class);

    private final ReflectConfigurator reflectConfigurator;

    @Inject
    public SyncClientFactory(ReflectConfigurator reflectConfigurator) {
        this.reflectConfigurator = reflectConfigurator;
    }

    public <I, T extends TServiceClient> I create(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig
    ) {
        SyncClientClassInfo<I, TServiceClient> classInfo = reflectConfigurator.createSync(clientInterfaceClazz);
        classInfo.getValidator().orElseThrow(() -> new IllegalArgumentException("Class " + clientInterfaceClazz.getName() + " not have method boolean healthCheck();"));
        return create(
                classInfo.getClientInterfaceClazz(),
                clientConfig,
                classInfo.getClientFactory(),
                classInfo.getValidator()
        );
    }

    public <I, T extends TServiceClient> I create(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig,
            Optional<Predicate<I>> validator
    ) {
        SyncClientClassInfo<I, TServiceClient> classInfo = reflectConfigurator.createSync(clientInterfaceClazz);
        return create(
                classInfo.getClientInterfaceClazz(),
                clientConfig,
                classInfo.getClientFactory(),
                validator
        );
    }

    /**
     * @param clientInterfaceClazz Интерфейс сервера
     * @param clientConfig конфигурация сервера. Адрес сервера может менятся на лету
     * @param factoryCreate фабрика для создания трифтовых клиентов
     * @param validator тестирует коннекты в пуле при создании
     *                     и периодически. Без него велик риск спонтанных ошибок подлючения.
     * @param <T>
     * @return
     * @deprecated Use {@link #create(Class, ThriftClientConfig)} if healCheck method has on
     *   of flowing name  healthCheck(), ping(), isUp(), health(), test(). Name healthCheck is recommended
     */
    @Deprecated // Use create(Class<I>, ThriftClientConfig) instead
    public <I, T extends TServiceClient> I create(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig,
            TServiceClientFactory<T> factoryCreate,
            Optional<Predicate<I>> validator
    ) {
        ThriftSyncClient<I, T> syncClient = createRawClient(
                clientInterfaceClazz,
                clientConfig,
                factoryCreate,
                validator
        );
        return ThriftSyncClient.makeProxy(clientInterfaceClazz, syncClient);
    }

    public <I, T extends TServiceClient> ThriftSyncClient<I, T> createRawClient(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig,
            TServiceClientFactory<T> factoryCreate,
            Optional<Predicate<I>> validator
    ){
        try {
            String thriftServiceName = ThriftUtils.getThriftServiceName(
                    clientInterfaceClazz,
                    clientConfig.getSubServiceName()
            );
            Predicate<I> validatorImp = validator.orElse(client -> true);
            ThriftSyncClient.PoolObjectFactory<I, T> clientFactory =
                    new ThriftSyncClient.PoolObjectFactory<>(
                        clientInterfaceClazz,
                        clientConfig.getAddressSupplier(),
                        factoryCreate,
                        validatorImp,
                        clientConfig,
                        thriftServiceName
            );

            ThriftClientAddress clientAddress = clientConfig.getAddressSupplier().get();
            GenericObjectPool<ThriftClientSyncContainer<I>> pool = new GenericObjectPool<>(
                    clientFactory,
                    clientConfig.createPoolConfig(
                            thriftServiceName,
                            clientAddress,
                            clientConfig.isBalancerClient()? ThriftCallType.SYNC_BALANCER: ThriftCallType.SYNC_CLIENT
                    ));
            clientFactory.setPoolReference(pool);

            ThriftSyncClient<I, T> syncClient = new ThriftSyncClient<>(
                    clientInterfaceClazz,
                    thriftServiceName,
                    pool,
                    clientConfig.getAddressSupplier(),
                    clientConfig,
                    validatorImp,
                    clientFactory
            );
            log.info("Success creating pool size: {}, address {}, class {}",
                    clientConfig.getMaxConnections(),
                    clientAddress,
                    clientInterfaceClazz.getName()
            );
            return syncClient;
        } catch (Exception e) {
            throw new RuntimeException("Unable to create pool", e);
        }
    }


}
