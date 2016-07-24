package com.qiwi.thrift.consul;

import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftUtils;

import java.util.Optional;

public class ThriftServiceDescription<I> {
    private final Class<I> interfaceClass;
    private final Class<?> serviceClass;
    private final Optional<String> subServiceName;
    private final String clusterName;
    private final String fullServiceName;

    private ThriftServiceDescription(Builder<I> builder) {
        this.interfaceClass = builder.getInterfaceClass();
        this.serviceClass = builder.getServiceClass();
        this.subServiceName = builder.getSubServiceName();
        this.clusterName = builder.getClusterName();
        this.fullServiceName = ThriftUtils.getThriftFullServiceName(serviceClass, subServiceName);
    }

    public Class<I> getInterfaceClass() {
        return interfaceClass;
    }

    public Class<?> getServiceClass() {
        return serviceClass;
    }

    public Optional<String> getSubServiceName() {
        return subServiceName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getFullServiceName() {
        return fullServiceName;
    }

    public static class Builder<I> {
        private final Class<I> interfaceClass;
        private final Class<?> serviceClass;
        private Optional<String> subServiceName = Optional.empty();
        private String clusterName = ThriftClientAddress.DEFAULT_CLUSTER_NAME;

        public Builder(Class<I> interfaceClass) {
            this.interfaceClass = interfaceClass;
            serviceClass = ThriftUtils.getThriftRootClass(interfaceClass);
        }

        public Class<I> getInterfaceClass() {
            return interfaceClass;
        }

        public Class<?> getServiceClass() {
            return serviceClass;
        }

        public Optional<String> getSubServiceName() {
            return subServiceName;
        }

        public Builder<I> setSubServiceName(Optional<String> subServiceName) {
            this.subServiceName = subServiceName;
            return this;
        }

        public String getClusterName() {
            return clusterName;
        }

        public Builder<I> setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public ThriftServiceDescription<I> build(){
            return new ThriftServiceDescription<>(this);
        }
    }
}
