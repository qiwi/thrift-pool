package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.reflect.AsyncClientClassInfo;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;


/**
 * Usage examples in com.qiwi.thrift.demo.AsyncDemoTest
 */
@Named
@Singleton
public class AsyncClientFactory {
    private static final Logger log = LoggerFactory.getLogger(AsyncClientFactory.class);

    private final ReflectConfigurator reflectConfigurator;

    @Inject
    public AsyncClientFactory(ReflectConfigurator reflectConfigurator) {
        this.reflectConfigurator = reflectConfigurator;
    }

    public <I, T extends TAsyncClient> ThriftAsyncClient<I> create(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig
    ) {
        AsyncClientClassInfo<I, TAsyncClient> classInfo = reflectConfigurator.createAsync(clientInterfaceClazz);
        classInfo.getValidator().orElseThrow(() -> new IllegalArgumentException("Class " + clientInterfaceClazz.getName() + " not have method boolean healthCheck();"));
        return create(
                classInfo.getClientInterfaceClazz(),
                clientConfig,
                classInfo.getClientFactory(),
                classInfo.getValidator()
        );
    }

    public <I, T extends TAsyncClient> ThriftAsyncClient<I> create(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig,
            Optional<ThriftAsyncVerifier<I>> validator
    ) {
        AsyncClientClassInfo<I, TAsyncClient> classInfo = reflectConfigurator.createAsync(clientInterfaceClazz);
        return create(
                classInfo.getClientInterfaceClazz(),
                clientConfig,
                classInfo.getClientFactory(),
                validator
        );
    }


    /**
     * @param clientInterfaceClazz Позволяет получить имя сервера для мультиплексинга
     * @param clientConfig конфигурация сервера. Адрес сервера может меняется на лету
     * @param clientCreator фабрика для создания трифтовых клиентов
     * @param validator тестирует коннекты в пуле при создании и периодически. Без него велик риск спонтанных ошибок
     * @param <T>
     * @return
     * @deprecated Use {@link #create(Class, ThriftClientConfig)} if healCheck method has on
     *   of flowing name  healthCheck(), ping(), isUp(), health(), test(). Name healthCheck is recommended
     */
    @Deprecated // Use create(Class<I>, ThriftClientConfig)
    public <I, T extends TAsyncClient> ThriftAsyncClient<I> create(
            Class<I> clientInterfaceClazz,
            ThriftClientConfig clientConfig,
            BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientCreator,
            Optional<ThriftAsyncVerifier<I>> validator
    ){
        TAsyncClientManager manager = null;
        try {
            manager = new TAsyncClientManager();

            String thriftServiceName = ThriftUtils.getThriftServiceName(clientInterfaceClazz, clientConfig.getSubServiceName());

            ThriftAsyncVerifier<I> validatorImp = validator.orElse(client -> true);
            ThriftPoolAsyncClient.AsyncPoolObjectFactory<I, T>  clientFactory =
                    new ThriftPoolAsyncClient.AsyncPoolObjectFactory<>(
                            clientConfig.getAddressSupplier(),
                            thriftServiceName,
                            manager,
                            clientCreator,
                            validatorImp,
                            clientConfig
            );
            ThriftClientAddress clientAddress = clientConfig.getAddressSupplier().get();
            GenericObjectPool<ThriftClientAsyncContainer<I>> pool = new GenericObjectPool<>(
                    clientFactory,
                    clientConfig.createPoolConfig(
                            thriftServiceName,
                            clientAddress,
                            clientConfig.isBalancerClient()? ThriftCallType.ASYNC_BALANCER: ThriftCallType.ASYNC_CLIENT
                    ));
            clientFactory.setPoolReference(pool);
            log.info("Success creating pool size: {}, address {}, class {}",
                    clientConfig.getMaxConnections(),
                    clientAddress,
                    clientInterfaceClazz.getName()
            );
            return new ThriftPoolAsyncClient<>(
                    clientInterfaceClazz,
                    thriftServiceName,
                    pool,
                    clientConfig.getAddressSupplier(),
                    clientConfig,
                    validatorImp,
                    clientFactory,
                    manager
            );
        } catch (IOException | RuntimeException e) {
            if (manager != null) {
                manager.stop();
            }
            throw new RuntimeException("Unable to create pool", e);
        }
    }



}
