package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.LambdaUtils;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

class ThriftClientSyncContainer<I>
        extends ThriftClientAbstractContainer<I, ThriftClientSyncContainer<I>> {
    private static final Logger log = LoggerFactory.getLogger(ThriftClientSyncContainer.class);

    private final I client;
    private final ThriftCallType callType;

    public ThriftClientSyncContainer(
            TTransport transport,
            GenericObjectPool<ThriftClientSyncContainer<I>> pool,
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
        return getTransport().isOpen();
    }

    @Override
    public I client() {
        return client;
    }

    @Override
    public Logger getLog() {
        return log;
    }

    public Object invoke(Method method, Object[] args, ThriftRequestReporter timeReporter) throws TException {
        long startNanos = System.nanoTime();
        ThriftRequestStatus status = ThriftRequestStatus.SUCCESS;
        Optional<Throwable> exception = Optional.empty();
        Object response = null;
        try {
            response = method.invoke(client, args);
            return response;
        } catch (IllegalAccessException | IllegalArgumentException ex) {
            exception = Optional.of(ex);
            status = ThriftRequestStatus.INTERNAL_ERROR;
            log.error("Internal error when call {} at {}", method, this, ex);
            throw new TTransportException("Unable to invoke method " + method, ex);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            exception = Optional.of(cause);
            if (ThriftUtils.isApplicationLevelException(cause)) {
                if (cause instanceof TException && !getNeedCircuitBreakOnException().test((TException) cause)) {
                    status = ThriftRequestStatus.APP_ERROR;
                    log.info("Server respond with application-level error when {} at {}", method, this, cause);
                } else {
                    status = ThriftRequestStatus.UNEXPECTED_ERROR;
                    log.info("Internal server error when call {} at {}", method, this, cause);
                }
            } else {
                status = ThriftRequestStatus.CONNECTION_ERROR;
                log.warn("Connection broken, may server code throw RuntimeException when call {} at {}", method, this, cause);
                invalidateConnection();
            }
            if (cause instanceof TException) {
                throw (TException) cause;
            } else {
                throw new TTransportException("Error when call " + method + " at " + this, cause);
            }
        } catch (Throwable ex) {
            exception = Optional.of(ex);
            invalidateConnection();
            status = ThriftRequestStatus.INTERNAL_ERROR;
            log.error("Internal error when call {} at {}", method, this, ex);
            throw LambdaUtils.propagate(ex);
        } finally {
            long duration = System.nanoTime() - startNanos;
            String methodName = method.getName();
            ThriftMonitoring.getMonitor().logMethodCall(
                    callType,
                    clientAddress,
                    serviceName,
                    methodName,
                    duration,
                    status
            );
            timeReporter.requestEnd(serviceName, methodName, callType, status, duration, exception);
            if (log.isDebugEnabled()) {
                log.debug(
                        "server {} respond {} in {} ms to method {}.{} with args {}",
                        this,
                        response,
                        (duration) / 1_000_000d,
                        serviceName,
                        method,
                        Arrays.toString(args),
                        exception.orElse(null)
                );
            }
        }
    }

}
