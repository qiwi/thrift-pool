package com.qiwi.thrift.reflect;

import com.qiwi.thrift.pool.ThriftAsyncFunction;
import com.qiwi.thrift.pool.ThriftAsyncVerifier;
import com.qiwi.thrift.utils.LambdaUtils;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientFactory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

@Named
@Singleton
public class ReflectConfigurator {
    private static final Logger log = LoggerFactory.getLogger(ReflectConfigurator.class);

    private static final MethodType SYNC_VALIDATOR_BOOLEAN_VALUE = MethodType.methodType(boolean.class);
    private static final MethodType SYNC_VALIDATOR_BOOLEAN_OBJECT = MethodType.methodType(Boolean.class);
    private static final MethodType SYNC_VALIDATOR_VOID = MethodType.methodType(void.class);
    private static final List<MethodType> VALIDATOR_METHOD_TYPES = Collections.unmodifiableList(Arrays.asList(
            SYNC_VALIDATOR_BOOLEAN_VALUE,
            SYNC_VALIDATOR_BOOLEAN_OBJECT,
            SYNC_VALIDATOR_VOID
    ));
    private static final List<String> VALIDATOR_METHOD_NAMES = Collections.unmodifiableList(Arrays.asList(
            "healthCheck",
            "ping",
            "isUp",
            "health",
            "test"
    ));
    private static final MethodType ASYNC_VALIDATOR = MethodType.methodType(void.class, AsyncMethodCallback.class);

    static MethodHandles.Lookup lookup = MethodHandles.publicLookup();


    private <I> Optional<MethodHandle> findValidatorMethod(Class<I> thriftInterface){
        for (MethodType type : VALIDATOR_METHOD_TYPES) {
            for (String name : VALIDATOR_METHOD_NAMES) {
                try {
                    return Optional.of(lookup.findVirtual(thriftInterface, name, type));
                } catch (NoSuchMethodException | IllegalAccessException e) {
                    log.debug("Try method {} {} fail", type, name, e);

                }
            }
        }
        return Optional.empty();
    }

    public <I, T extends TServiceClient> SyncClientClassInfo<I, T> createSync(Class<I> thriftInterface){
        Class<?> root = thriftInterface.getDeclaringClass();
        if (!"Iface".equals(thriftInterface.getSimpleName()) || root == null){
            throw new IllegalArgumentException(thriftInterface.getName() + " isn't thrift async client interface");
        }
        Class<?> clientClass = ThriftUtils.getClassByName(root, "Client");
        Class<?> factoryClass = ThriftUtils.getClassByName(clientClass, "Factory");
        Class<? extends TServiceClientFactory> factoryCast = factoryClass.asSubclass(TServiceClientFactory.class);
        TServiceClientFactory<T> clientFactory;
        try {
            clientFactory = factoryCast.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            throw new IllegalArgumentException(
                    "Unable to create client factory " + factoryClass.getName() + " for " + thriftInterface.getName(),
                    ex
            );
        }

        Optional<MethodHandle> validatorMethod = findValidatorMethod(thriftInterface);
        Optional<Predicate<I>> validator = validatorMethod.map(method -> {

            if (method.type().returnType().equals(void.class)) {
                 LambdaUtils.ThrowingPredicate<I> predicate = client -> {
                     method.invoke(client);
                     return true;
                };
                return LambdaUtils.uncheckedP(predicate);// javac 1.8.40 compilation fail after inline
             } else {
                return LambdaUtils.uncheckedP(client -> (Boolean) method.invoke(client));
            }
        });

        return new SyncClientClassInfo<I, T>(thriftInterface, clientFactory, validator);
    }

    public <I, T extends TAsyncClient> AsyncClientClassInfo<I, T> createAsync(Class<I> thriftInterface){
        Class<?> root = thriftInterface.getDeclaringClass();
        if (!"AsyncIface".equals(thriftInterface.getSimpleName()) || root == null){
            throw new IllegalArgumentException(thriftInterface.getName() + " isn't thrift sync client interface");
        }
        Class<?> clientClass = ThriftUtils.getClassByName(root, "AsyncClient");
        Class<?> factoryClass = ThriftUtils.getClassByName(clientClass, "Factory");
        Class<? extends TAsyncClientFactory> factoryCast = factoryClass.asSubclass(TAsyncClientFactory.class);
        BiFunction<TAsyncClientManager, TProtocolFactory, TAsyncClientFactory<T>> clientFactory;
        try {
            Constructor<? extends TAsyncClientFactory> constructor = factoryCast.getConstructor(TAsyncClientManager.class, TProtocolFactory.class);
            clientFactory = LambdaUtils.unchecked((manager, factory) -> constructor.newInstance(manager, factory));
        } catch (NoSuchMethodException ex) {
            throw new IllegalArgumentException(
                    "Unable to find constructor of client factory " + factoryClass.getName() + " for " + thriftInterface.getName(),
                    ex
            );
        }
        Class<?> ifaceClass = ThriftUtils.getClassByName(root, "Iface");
        Optional<MethodHandle> validatorMethod = findValidatorMethod(ifaceClass);
        Optional<ThriftAsyncVerifier<I>> validator = validatorMethod.flatMap(method -> {
            try {
                MethodHandle async = lookup.findVirtual(
                        thriftInterface,
                        lookup.revealDirect(method).getName(),
                        ASYNC_VALIDATOR
                );
                ThriftAsyncFunction<I, ?> function = (client, callback) -> {
                    try {
                        async.invoke(client, callback);
                    } catch (Throwable throwable) {
                        throw LambdaUtils.propagate(throwable);
                    }
                };

                if (method.type().returnType().equals(void.class)) {
                    return Optional.of(ThriftAsyncVerifier.of(
                            Void.class,
                            function,
                            any -> true
                    ));
                } else {
                    return Optional.of(ThriftAsyncVerifier.of(
                            Boolean.class,
                            function,
                            bool -> bool
                    ));
                }

            } catch (NoSuchMethodException | IllegalAccessException e) {
                log.error("Unable attach to method what exist in Sync interface", e);
                return Optional.empty();
            }
        });

        return new AsyncClientClassInfo<>(thriftInterface, clientFactory, validator);
    }

}
