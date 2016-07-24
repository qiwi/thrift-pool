package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import com.qiwi.thrift.utils.ThriftRuntimeException;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncMethodCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ThriftCallbackFuture<R, C extends TAsyncMethodCall> extends CompletableFuture<R> implements AsyncMethodCallback<C> {
    private static final Logger log = LoggerFactory.getLogger(ThriftCallbackFuture.class);

    private static ConcurrentHashMap<Class<? extends TAsyncMethodCall>, Method> methodCache = new ConcurrentHashMap<>();

    private final Class<R> resultClass;
    private final Map<String, String> diagnosticContext;

    public ThriftCallbackFuture(
            Class<R> resultClass
    ) {
        this.resultClass = resultClass;
        diagnosticContext = MDC.getCopyOfContextMap();
    }

    @Override
    public void onComplete(C response) {
        if (diagnosticContext != null) {
            MDC.setContextMap(diagnosticContext);
        }
        try {
            complete(response);
        } finally {
            MDC.clear();
        }
    }

    private void complete(C response) {
        String methodName = ThriftMonitoring.getMethodName(response);
        Optional<Throwable> exception = Optional.empty();
        ThriftRequestStatus status = ThriftRequestStatus.SUCCESS;
        try {
            Method method = methodCache.computeIfAbsent(response.getClass(), clazz -> {
                try {
                    Method result = clazz.getMethod("getResult");
                    result.setAccessible(true);
                    return result;
                } catch (NoSuchMethodException e) {
                    throw new ThriftRuntimeException(clazz + " does not have getResult method", e);
                }
            });
            Object result;
            try {
                result = method.invoke(response);
            } catch (IllegalAccessException | IllegalArgumentException  e) {
                exception = Optional.of(e);
                log.warn("Invalid reflection operation on {}.getResult", response.getClass().getName(), e);
                invalidate();
                completeExceptionally(e);
                status = ThriftRequestStatus.INTERNAL_ERROR;
                return;
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause() == null? e: e.getCause();
                exception = Optional.ofNullable(cause);
                if (ThriftUtils.isApplicationLevelException(cause)) {
                    log.info("Server respond with application-level error when call {}", response.getClass().getSimpleName(), cause);
                    returnToPool();
                    completeExceptionally(cause);
                    status = ThriftRequestStatus.UNEXPECTED_ERROR;
                } else {
                    log.warn("Connection broken, may server code throw RuntimeException when call {}", response.getClass().getSimpleName(), cause);
                    invalidate();
                    completeExceptionally(cause);
                    status = ThriftRequestStatus.CONNECTION_ERROR;
                }
                return;
            }
            try {
                complete(resultClass.cast(result));
                returnToPool();
            } catch (ClassCastException e) {
                exception = Optional.of(e);
                log.error("Error when do reflection with method {}, wrong result type {}", methodName, resultClass.getName(), e);
                returnToPool();
                completeExceptionally(new ClassCastException(e.getMessage() + " method " + methodName));
                status = ThriftRequestStatus.INTERNAL_ERROR;
            }
        } catch (Exception e) {
            exception = Optional.of(e);
            log.error("Error when obtain result by {}.getResult", response.getClass().getName(), e);
            invalidate();
            completeExceptionally(e);
            status = ThriftRequestStatus.INTERNAL_ERROR;
        } finally {
            requestEnd(methodName, status, response, exception);
        }
    }



    @Override
    public void onError(Exception exception) {
        if (diagnosticContext != null) {
            MDC.setContextMap(diagnosticContext);
        }
        invalidate();
        try {
            completeExceptionally(exception);
        } catch (Throwable e) {
            log.error("Error when completing future", e);
        } finally {
            requestEnd(null, ThriftRequestStatus.INTERNAL_ERROR, null, Optional.of(exception));
            MDC.clear();
        }
    }

    protected void requestEnd(
            String methodName,
            ThriftRequestStatus requestStatus,
            Object response,
            Optional<Throwable> exception
    ) {
    }


    protected void returnToPool() {
    }

    protected void invalidate(){
    }
}
