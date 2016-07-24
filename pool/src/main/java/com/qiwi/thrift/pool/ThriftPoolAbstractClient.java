package com.qiwi.thrift.pool;

import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;
import java.util.function.Supplier;

public abstract class ThriftPoolAbstractClient<I, C extends ThriftClientAbstractContainer<I, C>>
        implements ThriftClient<I> {
    private static final Logger log = LoggerFactory.getLogger(ThriftPoolAbstractClient.class);

    private final Class<I> interfaceClass;
    protected final String serviceName;
    protected final GenericObjectPool<C> pool;
    protected final Supplier<ThriftClientAddress> addressSupplier;
    protected final ThriftClientConfig config;
    private final Predicate<I> validator;

    ThriftPoolAbstractClient(
            Class<I> interfaceClass,
            String serviceName,
            GenericObjectPool<C> pool,
            Supplier<ThriftClientAddress> addressSupplier,
            ThriftClientConfig config,
            Predicate<I> validator
    ) {
        this.interfaceClass = interfaceClass;
        this.serviceName = serviceName;
        this.pool = pool;
        this.addressSupplier = addressSupplier;
        this.config = config;
        this.validator = validator;
    }

    public String getServiceName() {
        return serviceName;
    }

    public ThriftClientAddress getAddress() {
        return addressSupplier.get();
    }

    @Override
    public int getUsedConnections() {
        return pool.getNumActive();
    }

    @Override
    public int getNumWaiters() {
        return pool.getNumWaiters();
    }

    @Override
    public int getOpenConnections() {
        return pool.getNumActive() + pool.getNumIdle();
    }

    @Override
    public boolean isHealCheckOk() {
        try (C client = pool.borrowObject()) {
            try {
                boolean validateResult = validator.test(client.client());
                if (!validateResult) {
                    client.invalidateConnection();
                }
                return validateResult;
            } catch (Exception e) {
                client.invalidateConnection();
                log.warn("Unable query server {}", client, e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Unable to connect to server {}", addressSupplier.get(), e);
            return false;
        }
    }

    public Class<I> getInterfaceClass() {
        return interfaceClass;
    }

    @Override
    public void close() {
        pool.close();
    }

    @Override
    public int getMaxConnections() {
        return pool.getMaxTotal();
    }

    @Override
    public void setMaxConnections(int maxConnections) {
        pool.setMaxIdle(maxConnections);
        pool.setMaxTotal(maxConnections);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + addressSupplier.get() + '/' + serviceName +'}';
    }

    @Override
    public void evict() {
        // Evict from the pool
        try {
            pool.evict();
        } catch(Exception e) {
            log.warn("Error when evicting connection {}", this, e);
        }
        try {
            pool.preparePool();
        } catch (Exception e) {
            log.warn("Error add idle connections {}", this, e);
        }
    }
}
