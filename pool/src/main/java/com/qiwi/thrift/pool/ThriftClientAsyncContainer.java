package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftConnectionException;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import com.qiwi.thrift.utils.ThriftRuntimeException;
import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

class ThriftClientAsyncContainer<I>
        extends ThriftClientAbstractContainer<I, ThriftClientAsyncContainer<I>> {
    private static final Logger log = LoggerFactory.getLogger(ThriftClientAsyncContainer.class);

    private final I client;
    private final ThriftCallType callType;

    private volatile boolean connected = false;
    private volatile CompletableFuture<?> currentFuture;

    public ThriftClientAsyncContainer(
            TNonblockingTransport transport,
            ObjectPool<ThriftClientAsyncContainer<I>> pool,
            Predicate<TException> circuitBreakerChecker,
            ThriftClientAddress address,
            String serviceName,
            I client,
            ThriftCallType callType
    ) {
        super(transport, pool, circuitBreakerChecker, address, serviceName);
        this.client = Objects.requireNonNull(client, "client");
        this.callType = callType;
    }

    public boolean validate() {
        if (connected) {
            TTransport transport = getTransport();
            return transport != null && transport.isOpen();
        } else {
            return true;
        }
    }

    public <R, C extends TAsyncMethodCall> CompletableFuture<R> execAsync(
            Class<R> resultType,
            ThriftAsyncFunction<I, C> function,
            ThriftRequestReporter timeReporter
    ){
        try {
            CallbackFuture<R, C> future = new CallbackFuture<R, C>(
                    resultType,
                    function.getClass(),
                    timeReporter
            );
            currentFuture = future;
            connected = true;
            if (log.isTraceEnabled()) {
                log.trace("call to client {} functor {}", this, function.getClass().getName());
            }
            I client = client();
            function.call(client, future);
            return future;
        } catch (RuntimeException ex) {
            invalidateConnection();
            log.error("Error when invoking function {}", this, ex);
            throw ex;
        } catch (TException ex) {
            invalidateConnection();
            log.error("Error when invoking function {}", this, ex);
            throw new ThriftRuntimeException("Error when try to call " + this, ex);
        }

    }

    public I client() {
        return client;
    }

    @Override
    public void closeClient() {
        super.closeClient();
        CompletableFuture future = currentFuture;
        if (future != null) {
            future.completeExceptionally(new ThriftConnectionException("Connection closed before receiving response"));
            this.currentFuture = null;
        }
    }

    @Override
    public Logger getLog() {
        return log;
    }

    private class CallbackFuture<R, C extends TAsyncMethodCall> extends ThriftCallbackFuture<R, C> {
        private final Class<?> functorClass;// для логирования
        private final ThriftRequestReporter timeReporter;
        private final long callStartNanosecond;

        private CallbackFuture(
                Class<R> resultClass,
                Class<?> functorClass,
                ThriftRequestReporter timeReporter
        ) {
            super(resultClass);
            this.functorClass = functorClass;
            this.timeReporter = timeReporter;
            this.callStartNanosecond = System.nanoTime();
        }

        @Override
        protected void requestEnd(
                String methodName,
                ThriftRequestStatus requestStatus,
                Object response,
                Optional<Throwable> exception
        ) {
            if (methodName == null) {
                methodName = functorClass.getSimpleName();
            }

            if (exception.isPresent() && exception.get() instanceof TException && requestStatus == ThriftRequestStatus.UNEXPECTED_ERROR) {
                if (!getNeedCircuitBreakOnException().test((TException) exception.get())) {
                    requestStatus = ThriftRequestStatus.APP_ERROR;
                }
            }
            long duration = System.nanoTime() - callStartNanosecond;
            ThriftMonitoring.getMonitor().logMethodCall(
                    callType,
                    clientAddress,
                    serviceName,
                    methodName,
                    duration,
                    requestStatus
            );

            timeReporter.requestEnd(serviceName, methodName, callType, requestStatus, duration, exception);
            if (log.isDebugEnabled()) {
                log.debug(
                        "server {} respond {} in {} ms to functor {} method {}.{} result {}",
                        clientAddress,
                        response,
                        duration / 1000_000d,
                        functorClass.getClass(),
                        serviceName,
                        methodName,
                        requestStatus,
                        exception.orElse(null)
                );
            }

        }

        @Override
        protected void returnToPool() {
            close();
        }

        @Override
        protected void invalidate() {
            invalidateConnection();
            close();
        }
    }

}
