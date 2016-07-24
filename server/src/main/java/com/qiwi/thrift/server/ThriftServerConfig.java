package com.qiwi.thrift.server;

import com.qiwi.thrift.tracing.ThriftLogContext;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.tracing.ThriftTraceMode;
import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftClientAddress;

import java.util.Objects;
import java.util.Optional;

public class ThriftServerConfig {

    public static final int DEFAULT_THRIFT_POOL_SIZE = 128;
    public static final int DEFAULT_THRIFT_SELECTOR_THREADS = -1;
    public static final int DEFAULT_MAX_FRAME_SIZE = 1 * 1024 * 1024;

    private final int thriftPoolSize;
    private final int thriftSelectorThreads;
    private final int serverThriftPort;
    private final int maxFrameSizeBytes;
    private final ThriftTraceMode traceMode;
    private final ThriftRequestReporter requestReporter;

    public int getThriftPoolSize() {
        return thriftPoolSize;
    }

    public int getThriftSelectorThreads() {
        return thriftSelectorThreads;
    }

    public int getServerThriftPort() {
        return serverThriftPort;
    }

    public int getMaxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    public int getMaxCollectionItemCount() {
        return getMaxFrameSizeBytes() / 8;
    }

    public ThriftRequestReporter getRequestReporter() {
        return requestReporter;
    }


    public ThriftTraceMode getTraceMode() {
        return traceMode;
    }

    private ThriftServerConfig(
            Builder builder
    ) {
        this.thriftPoolSize = builder.getThriftPoolSize();
        this.thriftSelectorThreads = builder.getThriftSelectorThreads();
        this.serverThriftPort = builder.getServerThriftPort();
        this.maxFrameSizeBytes = builder.getMaxFrameSizeBytes();
        this.requestReporter = builder.getRequestReporter();
        this.traceMode = builder.getTraceMode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThriftServerConfig that = (ThriftServerConfig) o;
        return thriftPoolSize == that.thriftPoolSize &&
                thriftSelectorThreads == that.thriftSelectorThreads &&
                serverThriftPort == that.serverThriftPort &&
                maxFrameSizeBytes == that.maxFrameSizeBytes &&
                traceMode == that.traceMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                thriftPoolSize,
                thriftSelectorThreads,
                serverThriftPort,
                maxFrameSizeBytes,
                traceMode
        );
    }

    public static class Builder{
        private int thriftPoolSize = DEFAULT_THRIFT_POOL_SIZE;
        private int thriftSelectorThreads = DEFAULT_THRIFT_SELECTOR_THREADS;
        private int serverThriftPort = ThriftClientAddress.DEFAULT_SERVER_THRIFT_PORT;
        private int maxFrameSizeBytes = DEFAULT_MAX_FRAME_SIZE;
        private ThriftTraceMode traceMode = ThriftTraceMode.BASIC;
        private ThriftRequestReporter requestReporter = ThriftLogContext.getDefaultServerReporter();
        private ParameterSource source = ParameterSource.EMPTY;

        public int getThriftPoolSize() {
            return source.getInteger("pool_size", thriftPoolSize);
        }

        /**
         * Name: pool_size
         * @param thriftPoolSize - number of tread to process incoming requests
         * @return
         */
        public Builder setThriftPoolSize(int thriftPoolSize) {
            this.thriftPoolSize = thriftPoolSize;
            return this;
        }

        public int getThriftSelectorThreads() {
            return source.getInteger("selector_threads", thriftSelectorThreads);
        }

        /**
         * Name: selector_threads
         * @param thriftSelectorThreads - number of thread to accept incoming requests, and send it to execution
         * @return
         */
        public Builder setThriftSelectorThreads(int thriftSelectorThreads) {
            this.thriftSelectorThreads = thriftSelectorThreads;
            return this;
        }

        public int getServerThriftPort() {
            return source.getInteger("port", serverThriftPort);
        }


        /**
         * Name: port
         * Default: 9090
         * @param serverThriftPort - network port to listening incoming connections
         * @return
         */
        public Builder setServerThriftPort(int serverThriftPort) {
            this.serverThriftPort = serverThriftPort;
            return this;
        }

        public int getMaxFrameSizeBytes() {
            return source.getInteger("max_frame_size_bytes", maxFrameSizeBytes);
        }

        public ThriftTraceMode getTraceMode() {
            return ThriftTraceMode.parse(source, "trace_mode", Optional.of(traceMode)).orElse(ThriftTraceMode.BASIC);
        }

        /**
         *
         * @param traceMode trace mode used on server
         *
         */
        public void setTraceMode(ThriftTraceMode traceMode) {
            this.traceMode = Objects.requireNonNull(traceMode);
        }

        /**
         * Name: max_frame_size_bytes
         * Default: 1 megabyte
         * @param maxFrameSizeBytes - maximal incoming request size, what accepted by server.
         *                          Need to prevent dos attack, and out of memory
         * @return
         */
        public Builder setMaxFrameSizeBytes(int maxFrameSizeBytes) {
            this.maxFrameSizeBytes = maxFrameSizeBytes;
            return this;
        }

        public ThriftRequestReporter getRequestReporter() {
            return requestReporter;
        }

        /**
         *
         * @param requestReporter - Allow report requests to zipkin or another trace service
         *                        Logging context inside this call already sed, and can be acceded thought ThriftLogContext
         */
        public Builder setRequestReporter(ThriftRequestReporter requestReporter) {
            this.requestReporter = Objects.requireNonNull(requestReporter);
            return this;
        }

        public Builder fromParameters(ParameterSource source){
            this.source = Objects.requireNonNull(source);
            return this;
        }

        public ThriftServerConfig build(){
            return new ThriftServerConfig(
                    this
            );
        }

    }
}
