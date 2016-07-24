package com.qiwi.thrift.metrics;

import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

public class ThriftGraphiteConfig {
    private static final Logger log = LoggerFactory.getLogger(ThriftGraphiteConfig.class);

    private final boolean graphiteEnabled;
    private final String graphiteHost;
    private final int graphitePort;
    private final Duration graphitePushInterval;
    private final String cluster;
    private final String hostName;

    private ThriftGraphiteConfig(
            Builder builder
    ) {
        this.graphiteEnabled = builder.isGraphiteEnabled();
        String graphiteHost = builder.getGraphiteHost();
        if (graphiteEnabled) {
            this.graphiteHost = Objects.requireNonNull(
                    graphiteHost,
                    "graphiteHost not set or specified in config"
            );
        } else {
            this.graphiteHost = graphiteHost == null? "localhost": graphiteHost;
        }
        this.graphitePort = builder.getGraphitePort();
        this.graphitePushInterval = builder.getGraphitePushInterval();
        this.cluster = Objects.requireNonNull(builder.getCluster(), "Application name not set");
        this.hostName = builder.getHostName();
    }

    public boolean isGraphiteEnabled() {
        return graphiteEnabled;
    }

    public String getGraphiteHost() {
        return graphiteHost;
    }

    public int getGraphitePort() {
        return graphitePort;
    }

    public Duration getGraphitePushInterval() {
        return graphitePushInterval;
    }

    public String getCluster() {
        return cluster;
    }

    public String getHostName() {
        return hostName;
    }

    public static class Builder {
        private boolean graphiteEnabled = true;
        private String graphiteHost;
        private int graphitePort = 2003;
        private Duration graphitePushInterval = Duration.ofSeconds(60);
        private String cluster;
        private String hostName;

        private ParameterSource source = ParameterSource.EMPTY;

        public Builder() {
        }

        public boolean isGraphiteEnabled() {
            return source.getBoolean("graphite", graphiteEnabled);
        }

        /**
         *
         * @param graphiteEnabled graphite enabled or not
         */
        public Builder setGraphiteEnabled(boolean graphiteEnabled) {
            this.graphiteEnabled = graphiteEnabled;
            return this;
        }

        public String getGraphiteHost() {
            return source.getString("graphite.host", graphiteHost);
        }

        public Builder setGraphiteHost(String graphiteHost) {
            this.graphiteHost = graphiteHost;
            return this;
        }

        public int getGraphitePort() {
            return source.getInteger("graphite.port", graphitePort);
        }

        public Builder setGraphitePort(int graphitePort) {
            this.graphitePort = graphitePort;
            return this;
        }

        public Duration getGraphitePushInterval() {
            return Duration.ofSeconds(source.getInteger(
                    "graphite.pushIntervalSec",
                    (int) graphitePushInterval.getSeconds()
            ));
        }

        public Builder setGraphitePushInterval(Duration graphitePushInterval) {
            this.graphitePushInterval = graphitePushInterval;
            return this;
        }

        public String getCluster() {
            return cluster;
        }

        public Builder setCluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public String getHostName() {
            String result = source.getString("hostName", this.hostName);
            if (result == null) {
                return ThriftUtils.getHostName();
            } else {
                return result;
            }
        }

        public Builder setHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder fromParameterSource(ParameterSource source){
            this.source = source;
            return this;
        }

        public ThriftGraphiteConfig build(){
            return new ThriftGraphiteConfig(
                    this
            );
        }

    }

}
