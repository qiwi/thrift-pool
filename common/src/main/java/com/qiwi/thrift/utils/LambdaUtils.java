package com.qiwi.thrift.utils;


import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TSaslTransportException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.*;

/**
 * Этот лямбда-утилс нужен, чтобы не было зависимостей от других проектов
 * Разные утилитки полезные для использования лябмда
 *
 * unchecked - заворачивает любые checked исключения в unchecked. При этом SQLException заворачивается в DBException
 * В большинстве случаев компилятор может выбрать функцию для правильного функционального интерфейса,
 * но если возникают проблемы, ему придётся подсказать, выбрав правильную версию функции.
 *
 * uncheckedS - Supplier
 * uncheckedF - Function
 * uncheckedC - Consumer
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class LambdaUtils {
    private static final Logger log = LoggerFactory.getLogger(LambdaUtils.class);

//    private static ConcurrentMap<Th>
    private static final ConcurrentMap<
            Class<? extends Throwable>,
            Function<? extends Throwable, RuntimeException>
        > mappers = new ConcurrentHashMap<>(32);
    private static final ConcurrentMap<
            Class<? extends Throwable>,
            Function<? extends Throwable, RuntimeException>
            > mappersCache = new ConcurrentHashMap<>(512);


    private LambdaUtils(){}

    public static <T extends Throwable> void registerMapper(Class<T> clazz, Function<T, RuntimeException> function){
        Function<? extends Throwable, RuntimeException> put = mappers.put(clazz, function);
        if (put != null) {
            log.warn(
                    "Mapper already for exception {} already registered as {}. New mapper {}",
                    clazz.getName(),
                    put.getClass(),
                    function.getClass()
            );
        }
        mappersCache.clear();
    }

    /**
     * unchecked - заворачивает любые checked исключения в unchecked.
     *
     * Если вы получаете ошибку вида:
     * reference to unchecked is ambiguous или ambiguous method call
     * Используйте метод c 'S' на конце
     */
    public static <T> Supplier<T> unchecked(Callable<T> func){
        return () -> {
            try {
                return func.call();
            } catch (Exception ex) {
                throw propagate(ex);
            }
        };
    }

    public static <T> Supplier<T> uncheckedS(Callable<T> func){
        return unchecked(func);
    }

    /**
     * unchecked - заворачивает любые checked исключения в unchecked.
     *
     * Если вы получаете ошибку вида:
     * reference to unchecked is ambiguous или ambiguous method call
     * Используйте метод c 'F' на конце
     */
    public static <T, R> Function<T, R> unchecked(ThrowingFunction<T, R> func){
        return t -> {
            try {
                return func.apply(t);
            } catch (Throwable ex) {
                throw propagate(ex);
            }
        };
    }

    public static <T, R> Function<T, R> uncheckedF(ThrowingFunction<T, R> func){
        return unchecked(func);
    }

    /**
     * unchecked - заворачивает любые checked исключения в unchecked.
     *
     * Если вы получаете ошибку вида:
     * reference to unchecked is ambiguous или ambiguous method call
     * Используйте метод c 'F' на конце
     */
    public static <T, U, R> BiFunction<T, U, R> unchecked(ThrowingBiFunction<T, U, R> func){
        return (t, u) -> {
            try {
                return func.apply(t, u);
            } catch (Throwable ex) {
                throw propagate(ex);
            }
        };
    }

    public static <T, U, R> BiFunction<T, U, R> uncheckedBF(ThrowingBiFunction<T, U, R> func){
        return unchecked(func);
    }

    /**
     * unchecked - заворачивает любые checked исключения в unchecked.
     *
     * Если вы получаете ошибку вида:
     * reference to unchecked is ambiguous или ambiguous method call
     * Используйте метод c 'C' на конце
     */
    public static <T> Consumer<T> unchecked(ThrowingConsumer<T> func){
        return t -> {
            try {
                func.accept(t);
            } catch (Throwable ex) {
                throw propagate(ex);
            }
        };
    }

    public static <T> Consumer<T> uncheckedC(ThrowingConsumer<T> func){
        return unchecked(func);
    }

    /**
     * unchecked - заворачивает любые checked исключения в unchecked.
     *
     * @param func
     * @param <T>
     * @return
     */
    public static <T> Predicate<T> uncheckedP(ThrowingPredicate<T> func){
        return t -> {
            try {
                return func.test(t);
            } catch (Throwable ex) {
                throw propagate(ex);
            }
        };
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {
        R apply(T t) throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowingBiFunction<T, U, R> {
        R apply(T t, U u) throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowingPredicate<T> {
        boolean test(T t) throws Throwable;
    }



    static {
        registerMapper(TSimpleJSONProtocol.CollectionMapKeyException.class, ThriftConnectionException::new);
        registerMapper(TApplicationException.class, ThriftConnectionException::new);
        registerMapper(TProtocolException.class, ex -> {
            if (ex.getType() == TProtocolException.SIZE_LIMIT) {
                return new ThriftTooLongMessageException("Object contain too long field", ex);
            } else {
                return new ThriftConnectionException(ex);
            }
        });
        registerMapper(TSaslTransportException.class, ThriftConnectionException::new);
        registerMapper(TTransportException.class, ex -> {
            if (ex.getMessage() != null && ex.getMessage().contains("larger")){
                return new ThriftTooLongMessageException("To long frame size. Not thrift protocol?", ex);
            } else {
                return new ThriftConnectionException(ex);
            }
        });
        registerMapper(TTimeoutException.class, ThriftTimeoutException::new);
        registerMapper(TException.class, ex-> {
            if (ex.getClass().equals(TException.class)) {
                return new ThriftConnectionException(ex);
            } else {
                return new ThriftApplicationException(ex);
            }
        });


        registerMapper(ExecutionException.class, ex ->
                ex.getCause() == null
                        ? new RuntimeException(ex)
                        : getPropagation(ex.getCause()
        ));
        registerMapper(InvocationTargetException.class, ex ->
                ex.getCause() == null
                        ? new RuntimeException(ex)
                        : getPropagation(ex.getCause()
        ));
        registerMapper(RuntimeException.class, ex -> ex);
    }

    public static RuntimeException propagate(Throwable throwable) {
        if (throwable instanceof Error) {
            throw (Error)throwable;
        } else {
            throw getPropagation(throwable);
        }
    }

    public static RuntimeException getPropagation(Throwable throwable){
        Function<? extends Throwable, RuntimeException> function = mappersCache.computeIfAbsent(throwable.getClass(), clazz -> {
                    Class<?> currentClazz = clazz;
                    do {
                        Function<? extends Throwable, RuntimeException> mapper = mappers.get(currentClazz);
                        if (mapper != null) {
                            return mapper;
                        }
                        currentClazz = currentClazz.getSuperclass();
                    } while (currentClazz != null);
                    return RuntimeException::new;
                });
        @SuppressWarnings("unchecked")
        Function<Throwable, RuntimeException> function1 = (Function<Throwable, RuntimeException>) function;
        return function1.apply(throwable);
    }
}
