package com.qiwi.thrift.pool;

import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.Objects;
import java.util.function.Predicate;

abstract class ThriftClientAbstractContainer<I, C extends ThriftClientAbstractContainer<I, C>> implements Closeable {
    private TTransport transport;
    private final ObjectPool<C> pool;
    private final Predicate<TException> needCircuitBreakOnException;
    protected final ThriftClientAddress clientAddress;
    protected final String serviceName;
    private boolean invalidState = false;

    public ThriftClientAbstractContainer(
            TTransport transport,
            ObjectPool<C> pool,
            Predicate<TException> needCircuitBreakOnException,
            ThriftClientAddress clientAddress,
            String serviceName
    ) {
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(needCircuitBreakOnException, "clientConfig");
        Objects.requireNonNull(clientAddress, "ClientAddress");

        this.transport = transport;
        this.pool = pool;
        this.needCircuitBreakOnException = needCircuitBreakOnException;
        this.clientAddress = clientAddress;
        this.serviceName = serviceName;
    }

    public abstract I client();

    public TTransport getTransport() {
        return transport;
    }

    public ThriftClientAddress getClientAddress() {
        return clientAddress;
    }

    public Predicate<TException> getNeedCircuitBreakOnException() {
        return needCircuitBreakOnException;
    }

    @Override
    public void close() {
        if (invalidState) {
            if (transport == null) {
                getLog().error("Try to double close client{}", this, new Throwable());
            }
            try {
                getLog().debug("remove object because error happened {}", this);
                pool.invalidateObject((C)this);
            } catch (Exception e) {
                getLog().warn("return object failed, close", e);
                closeClient();
            }
        } else {
            try {
                if (transport == null) {
                   getLog().error("Try to return client after close {}", this, new Throwable());
                   pool.invalidateObject((C)this);
                   return;
               }
               getLog().trace("return object to pool: {}", this);
               pool.returnObject((C)this);
            } catch (Exception e) {
                getLog().warn("return object failed, close", e);
                closeClient();
            }
        }
    }

    public abstract Logger getLog();


    public void invalidateConnection(){
        invalidState = true;
    }


    void closeClient() {
        getLog().debug("closing client {}", this);
        TTransport transportCopy = transport;
        transport = null;
        if (transportCopy != null) {
            transportCopy.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (transport != null) {
            getLog().error("Not closed client collected by GC");
            closeClient();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{" + clientAddress
                + '/' + serviceName + '}';
    }

}
