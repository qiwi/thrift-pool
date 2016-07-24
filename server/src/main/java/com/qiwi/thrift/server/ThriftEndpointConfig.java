package com.qiwi.thrift.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ThriftEndpointConfig {
    private final Collection<ThriftEndpoint<?>> endpoints;

    private ThriftEndpointConfig(Builder builder) {
        this.endpoints = new ArrayList<>(builder.getEndpoints());
    }

    public Collection<ThriftEndpoint<?>> getEndpoints() {
        return endpoints;
    }

    public static class Builder{
        private Collection<ThriftEndpoint<?>> endpoints = new ArrayList<>();

        public Collection<ThriftEndpoint<?>> getEndpoints() {
            return Collections.unmodifiableList(new ArrayList<>(endpoints));
        }

        public Builder setEndpoints(Collection<ThriftEndpoint<?>> endpoints) {
            this.endpoints = new ArrayList<>(endpoints);
            return this;
        }

        public Builder addEndpoint(ThriftEndpoint<?> endpoint) {
            endpoints.add(endpoint);
            return this;
        }

        public <I> Builder addEndpoint(Class<I> interfaceClass, I implementation) {
            addEndpoint(new ThriftEndpoint.Builder<>(interfaceClass, implementation).build());
            return this;
        }

        public ThriftEndpointConfig build() {
            return new ThriftEndpointConfig(this);
        }
    }
}
