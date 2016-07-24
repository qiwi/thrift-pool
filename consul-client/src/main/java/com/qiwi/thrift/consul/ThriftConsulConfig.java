package com.qiwi.thrift.consul;

import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftUtils;

import java.util.Objects;
import java.util.Optional;

public class ThriftConsulConfig {
    private final String applicationName;
    private final Optional<String> dcName;
    private final String hostName;
    private final String consulHost;
    private final int consulPort;
    private final Optional<String> aclToken;
    private final ParameterSource source;

    private ThriftConsulConfig(Builder builder) {
        this.applicationName = Objects.requireNonNull(builder.getApplicationName(), "applicationName");
        this.dcName = builder.getDcName();
        this.source = Objects.requireNonNull(builder.getParameterSource());
        String hostName = builder.getHostName();
        if (hostName == null) {
            this.hostName = ThriftUtils.getHostName();
        } else {
            this.hostName = hostName;
        }
        this.consulHost = builder.getHost();
        this.consulPort = builder.getPort();
        this.aclToken = builder.getAclToken();
    }

    public String getApplicationName() {
        return applicationName;
    }

    public Optional<String> getDcName() {
        return dcName;
    }

    public String getHostName() {
        return hostName;
    }

    public String getConsulHost() {
        return consulHost;
    }

    public int getConsulPort() {
        return consulPort;
    }

    public Optional<String> getAclToken() {
        return aclToken;
    }

    public ParameterSource getParameterSource() {
        return source;
    }

    public static class Builder {
        private String applicationName;
        private Optional<String> dcName = Optional.empty();
        private String hostName;
        private String host = "localhost";
        private int port = 8500;
        private Optional<String> aclToken = Optional.empty();
        private ParameterSource source = ParameterSource.EMPTY;

        public Builder() {
        }

        private Builder getThis() {
            return this;
        }

        public String getApplicationName() {
            return source.getString("application.name", applicationName);
        }

        public Builder setApplicationName(String applicationName) {
            this.applicationName = applicationName;
            return getThis();
        }

        public Optional<String> getDcName() {
            String propDcName = System.getProperty("dcName");
            if (propDcName != null) {
                return Optional.of(propDcName);
            }
            return Optional.ofNullable(source.getString("dcName", dcName.orElse(null)));
        }

        public Builder setDcName(Optional<String> dcName) {
            this.dcName = dcName;
            return getThis();
        }

        public String getHostName() {
            return source.getString("hostName", hostName);
        }

        public Builder setHostName(String hostName) {
            this.hostName = hostName;
            return getThis();
        }

        public String getHost() {
            return source.getString("consul.host", this.host);
        }

        public Builder setHost(String host) {
            this.host = host;
            return getThis();
        }

        public int getPort() {
            return source.getInteger("consul.port", port);
        }

        public Builder setPort(int port) {
            this.port = port;
            return getThis();
        }

        public Optional<String> getAclToken() {
            return Optional.ofNullable(source.getString("consul.token", aclToken.orElse(null)));
        }

        public Builder setAclToken(Optional<String> aclToken) {
            this.aclToken = aclToken;
            return getThis();
        }

        public ParameterSource getParameterSource() {
            return source;
        }

        public Builder fromParameterSource(ParameterSource source) {
            this.source = Objects.requireNonNull(source);
            return getThis();
        }

        public ThriftConsulConfig build(){
            return new ThriftConsulConfig(this);
        }

    }


}
