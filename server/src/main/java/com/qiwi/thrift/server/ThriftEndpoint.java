package com.qiwi.thrift.server;

import com.qiwi.thrift.tracing.ThriftTraceMode;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.thrift.TProcessor;

import java.util.*;
import java.util.function.Function;

public class ThriftEndpoint<I> {
    private final Class<I> interfaceClass;
    private final I implementation;
    private final Class<?> serviceClass;
    private final Optional<String> subServiceName;
    private final String clusterName;
    private final Set<String> endpointNames;
    private final Function<I, TProcessor> endpointProcessorFactory;
    private final Map<String, String> tags;

    private ThriftEndpoint(Builder<I> builder) {
        this.endpointNames = builder.getEndpointNames();
        this.serviceClass = builder.getServiceClass();
        this.implementation = builder.getImplementation();
        this.subServiceName = builder.getSubServiceName();
        this.clusterName = builder.getClusterName();
        this.interfaceClass = builder.getInterfaceClass();
        this.endpointProcessorFactory = builder.getEndpointProcessorFactory();
        this.tags = builder.getTags();
    }

    public Class<I> getInterfaceClass() {
            return interfaceClass;
    }

    public I getImplementation() {
        return implementation;
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

    public Set<String> getEndpointNames() {
        return endpointNames;
    }

    public Function<I, TProcessor> getEndpointProcessorFactory() {
        return endpointProcessorFactory;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public static class Builder<I> {
        private final Class<I> interfaceClass;
        private final I implementation;
        private final Class<?> serviceClass;
        private Optional<String> subServiceName = Optional.empty();
        private Optional<Set<String>> endpointNames = Optional.empty();
        private Function<I, TProcessor> endpointProcessorFactory;
        private Map<String, String> tags = new HashMap<>();

        public Builder(Class<I> interfaceClass, I implementation) {
            this.interfaceClass = Objects.requireNonNull(interfaceClass);
            this.implementation = Objects.requireNonNull(implementation);
            serviceClass = ThriftUtils.getThriftRootClass(interfaceClass);
            tags.put(ThriftClientAddress.CLUSTER_NAME_PARAMETER, ThriftClientAddress.DEFAULT_CLUSTER_NAME);
        }

        public Class<I> getInterfaceClass() {
            return interfaceClass;
        }

        public I getImplementation() {
            return implementation;
        }

        public Class<?> getServiceClass() {
            return serviceClass;
        }

        public Optional<String> getSubServiceName() {
            return subServiceName;
        }

        /**
         * Allow few implementation of same interface on single server
         * @param subServiceName
         * @return
         */
        public Builder<I> setSubServiceName(Optional<String> subServiceName) {
            this.subServiceName = Objects.requireNonNull(subServiceName);
            return this;
        }

        public Set<String> getEndpointNames() {
            if (endpointNames.isPresent()) {
                return endpointNames.get();
            } else {
                return Collections.singleton(ThriftUtils.getThriftServiceName(serviceClass, subServiceName));
            }
         }

        public Builder<I> setEndpointNames(Set<String> endpointNames) {
            this.endpointNames = Optional.of(Collections.unmodifiableSet(new LinkedHashSet<>(endpointNames)));
            return this;
        }

        public Function<I, TProcessor> getEndpointProcessorFactory() {
            if (endpointProcessorFactory == null) {
                return ServerReflectionUtils.getProcessorFactory(interfaceClass, serviceClass);
            } else {
                return endpointProcessorFactory;
            }
        }

        public Builder<I> setEndpointProcessorFactory(Optional<Function<I, TProcessor>> endpointProcessorFactory) {
            this.endpointProcessorFactory = endpointProcessorFactory.orElse(null);
            return this;
        }

        public Map<String, String> getTags() {
            return Collections.unmodifiableMap(new HashMap<>(tags));
        }

        public Builder<I> setTags(Map<String, String> tags) {
            this.tags = new HashMap<>(tags);
            return this;
        }

        public Builder<I> addTag(String name, String value) {
            tags.put(name, value);
            return this;
        }

        public String getClusterName() {
            return tags.get(ThriftClientAddress.CLUSTER_NAME_PARAMETER);
        }

        public Builder<I> setClusterName(String clusterName) {
            tags.put(ThriftClientAddress.CLUSTER_NAME_PARAMETER, clusterName);
            return this;
        }

        public Optional<ThriftTraceMode> getTraceMode() {
            return ThriftTraceMode.parse(tags.get(ThriftClientAddress.TRACE_TYPE_PARAMETER));
        }

        public Builder<I> setTraceMode(ThriftTraceMode traceType) {
            tags.put(ThriftClientAddress.TRACE_TYPE_PARAMETER, traceType.name());
            return this;
        }

        public ThriftEndpoint<I> build() {
            return new ThriftEndpoint<I>(this);
        }
    }
}
