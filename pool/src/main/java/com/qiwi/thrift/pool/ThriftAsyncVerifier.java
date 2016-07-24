package com.qiwi.thrift.pool;

import com.qiwi.thrift.utils.ThriftTimeoutException;
import org.apache.thrift.async.TAsyncMethodCall;

import java.util.function.Predicate;

/**
 * Интерфейс для тестирования подключения.
 * Должен вызвать удалённый метод. Если validate возвращает false или бросает исключение, то считается, что подключение оборвано
 * @param <I>
 */
@FunctionalInterface
public interface ThriftAsyncVerifier<I> extends Predicate<I> {
    boolean test(I client);

    static <I> ThriftAsyncVerifier<I> ofBoolean(ThriftAsyncFunction<I, ?> function){
        return of(Boolean.class, function, booleanResult -> booleanResult);
    }

    static <I, R, C extends TAsyncMethodCall> ThriftAsyncVerifier<I> of(
            Class<R> resultClass,
            ThriftAsyncFunction<I, C> function,
            Predicate<R> validator
    ){
        return client -> {
            ThriftCallbackFuture<R, C> future = new ThriftCallbackFuture<>(resultClass);
            try {
                function.call(client, future);
                R result = future.get();
                return validator.test(result);
            } catch (Exception ex) {
                throw new ThriftTimeoutException("Verify method result error", ex);
            }
        };
    }
}
