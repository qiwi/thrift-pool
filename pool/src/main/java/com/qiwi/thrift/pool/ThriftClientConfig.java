package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.pool.imp.IdleTimeEvictionPolicy;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRuntimeException;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class ThriftClientConfig extends ThriftAbstractClientConfig {
    private static final Logger log = LoggerFactory.getLogger(ThriftClientConfig.class);

    private final Supplier<ThriftClientAddress> addressSupplier;
    private final boolean balancerClient;

    private ThriftClientConfig(
            ThriftClientConfig.Builder builder,
            boolean balancerClient
    ) {
        super(builder);
        this.addressSupplier = builder.getAddressSupplier();
        this.balancerClient = balancerClient;
    }

    public Supplier<ThriftClientAddress> getAddressSupplier() {
        return addressSupplier;
    }

    public int getMaxCollectionItemCount() {
        return getMaxFrameSizeBytes() / 8;
    }

    public boolean isBalancerClient() {
        return balancerClient;
    }

    /**
     *
     * Прошу обратить внимание на метод fromParameters, который позволяет брать настрокий из абстрактного
     * текстового конфига
     */
    public static class Builder extends ThriftAbstractClientConfig.Builder<Builder> {
        /**
         * В процессе жизни пула, адрес сервера может меняться.
         */
        private volatile Supplier<ThriftClientAddress> addressSupplier;
        private boolean balancerClient = false;

        public Builder() {
        }

        protected Builder(
                 boolean balancerClient
        ) {
            this.balancerClient = balancerClient;
        }

        public Supplier<ThriftClientAddress> getAddressSupplier() {
            return () -> {
                Supplier<ThriftClientAddress> supplier = ThriftClientAddress.supplier(source);
                try {
                     return supplier.get();
                 } catch (ThriftRuntimeException ex) {
                    return addressSupplier.get();
                 }
            };
        }

        public Builder setAddress(Supplier<ThriftClientAddress> addressSupplier) {
            this.addressSupplier = addressSupplier;
            return getThis();
        }

        public Builder setAddress(ThriftClientAddress address) {
            this.addressSupplier = () -> address;
            return getThis();
        }

        public ThriftClientConfig build() {
            Supplier<ThriftClientAddress> addressSupplier = getAddressSupplier();
            try {
                addressSupplier.get();
            } catch (Exception ex) {
                throw new IllegalStateException("No client address provided", ex);
            }
            return new ThriftClientConfig(
                    this,
                    balancerClient
            );
        }

    }


    public GenericObjectPoolConfig createPoolConfig(
            String thriftServiceName,
            ThriftClientAddress clientAddress,
            ThriftCallType callType
    ) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMinIdle(getMinIdleConnections());
        config.setMaxIdle(getMaxConnections());
        config.setMaxTotal(getMaxConnections());
        config.setEvictionPolicyClassName(IdleTimeEvictionPolicy.class.getName());
        config.setMinEvictableIdleTimeMillis(getIdleEvictTime().toMillis());
        // через SoftMinEvictableIdleTime передаётся maxLiveTime и обрабатывается в специальном
        // обработчике IdleTimeEvictionPolicy
        config.setSoftMinEvictableIdleTimeMillis(getMaxConnectLiveTime().toMillis());
        // Balancer do eviction in it's own executor
        if (callType.isBalancer()) {
            config.setTimeBetweenEvictionRunsMillis(-1);
        } else {
            config.setTimeBetweenEvictionRunsMillis(getConnectionCheckPeriod().toMillis());
        }
        config.setMaxWaitMillis(getMaxWaitForConnection().toMillis());
        config.setNumTestsPerEvictionRun(getMaxConnections());
        config.setLifo(isLifo());
        config.setFairness(true);
        config.setJmxNameBase(ThriftMonitoring.POOL_JMX_BASE);

        if (!isBalancerClient() || callType.isAsync()){
            config.setTestOnCreate(true);
        }
        config.setTestWhileIdle(true);
        config.setJmxNamePrefix(ThriftMonitoring.formatPoolJmxPrefix(
                thriftServiceName,
                clientAddress,
                callType
        ));
        return config;
    }

}
