package com.qiwi.thrift.pool;

import com.qiwi.thrift.tracing.ThriftLogContext;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.tracing.ThriftTraceMode;
import com.qiwi.thrift.utils.ParameterSource;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

public abstract class ThriftAbstractClientConfig {
    private static final Logger log = LoggerFactory.getLogger(ThriftAbstractClientConfig.class);
    private final Duration maxWaitForConnection;
    private final Duration connectionCheckPeriod;
    private final Duration idleEvictTime;
    private final Duration maxConnectLiveTime;
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final int minIdleConnections;
    private final int maxConnections;
    private final int maxFrameSizeBytes;
    private final Predicate<TException> needCircuitBreakOnException;
    private final Optional<ThriftTraceMode> traceMode;
    private final boolean lifo;
    private final int socketReceiveBufferSize;
    private final int socketSendBufferSize;
    private final ThriftRequestReporter requestReporter;
    private final Optional<String> subServiceName;

    protected ThriftAbstractClientConfig(
            Builder<?> builder
    ) {
        this.maxWaitForConnection = builder.getMaxWaitForConnection();
        this.connectionCheckPeriod = builder.getConnectionCheckPeriod();
        this.idleEvictTime = builder.getIdleEvictTime();
        this.maxConnectLiveTime = builder.getMaxConnectLiveTime();
        this.connectTimeout = builder.getConnectTimeout();
        this.requestTimeout = builder.getRequestTimeout();
        this.minIdleConnections = builder.getMinIdleConnections();
        this.maxConnections = builder.getMaxConnections();
        this.maxFrameSizeBytes = builder.getMaxFrameSizeBytes();
        this.needCircuitBreakOnException = builder.getNeedCircuitBreakOnException();
        this.traceMode = builder.getTraceMode();
        this.lifo = builder.isLifo();
        this.socketReceiveBufferSize = builder.getSocketReceiveBufferSize();
        this.socketSendBufferSize = builder.getSocketSendBufferSize();
        this.requestReporter = builder.getRequestReporter();
        this.subServiceName = builder.getSubServiceName();
    }

    public Duration getMaxWaitForConnection() {
        return maxWaitForConnection;
    }

    public Duration getConnectionCheckPeriod() {
        return connectionCheckPeriod;
    }

    public Duration getIdleEvictTime() {
        return idleEvictTime;
    }

    public Duration getMaxConnectLiveTime() {
        return maxConnectLiveTime;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public int getMinIdleConnections() {
        return minIdleConnections;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getMaxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    public int getMaxCollectionItemCount() {
        return getMaxFrameSizeBytes() / 8;
    }

    protected Predicate<TException> getNeedCircuitBreakOnException() {
        return needCircuitBreakOnException;
    }

    public Optional<ThriftTraceMode> getTraceMode() {
        return traceMode;
    }

    public boolean isLifo() {
        return lifo;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    public int getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    public ThriftRequestReporter getRequestReporter() {
        return requestReporter;
    }

    public Optional<String> getSubServiceName() {
        return subServiceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ThriftAbstractClientConfig that = (ThriftAbstractClientConfig) o;

        if (minIdleConnections != that.minIdleConnections) {
            return false;
        }
        if (maxConnections != that.maxConnections) {
            return false;
        }
        if (maxFrameSizeBytes != that.maxFrameSizeBytes) {
            return false;
        }
        if (lifo != that.lifo) {
            return false;
        }
        if (socketReceiveBufferSize != that.socketReceiveBufferSize) {
            return false;
        }
        if (socketSendBufferSize != that.socketSendBufferSize) {
            return false;
        }
        if (!maxWaitForConnection.equals(that.maxWaitForConnection)) {
            return false;
        }
        if (!connectionCheckPeriod.equals(that.connectionCheckPeriod)) {
            return false;
        }
        if (!idleEvictTime.equals(that.idleEvictTime)) {
            return false;
        }
        if (!maxConnectLiveTime.equals(that.maxConnectLiveTime)) {
            return false;
        }
        if (!connectTimeout.equals(that.connectTimeout)) {
            return false;
        }
        if (!requestTimeout.equals(that.requestTimeout)) {
            return false;
        }
        if (!needCircuitBreakOnException.equals(that.needCircuitBreakOnException)) {
            return false;
        }
        if (!traceMode.equals(that.traceMode)) {
            return false;
        }
        return requestReporter.equals(that.requestReporter);
    }

    @Override
    public int hashCode() {
        int result = maxWaitForConnection.hashCode();
        result = 31 * result + connectionCheckPeriod.hashCode();
        result = 31 * result + idleEvictTime.hashCode();
        result = 31 * result + maxConnectLiveTime.hashCode();
        result = 31 * result + connectTimeout.hashCode();
        result = 31 * result + requestTimeout.hashCode();
        result = 31 * result + minIdleConnections;
        result = 31 * result + maxConnections;
        result = 31 * result + maxFrameSizeBytes;
        result = 31 * result + needCircuitBreakOnException.hashCode();
        result = 31 * result + traceMode.hashCode();
        result = 31 * result + (lifo ? 1 : 0);
        result = 31 * result + socketReceiveBufferSize;
        result = 31 * result + socketSendBufferSize;
        result = 31 * result + requestReporter.hashCode();
        return result;
    }

    /**
     *
     * Прошу обратить внимание на метод fromParameters, который позволяет конфигурировать пулл
     * из текстового конфига
     */
    public static class Builder<B extends Builder<B>> {
        /**
         * Максимальное время ожидания пока освободится коннект в пуле
         */
        private volatile Duration maxWaitForConnection = Duration.ofSeconds(20);
        /**
         * Частота, с которой коннеты проверяются на рабоспособность
         */
        private volatile Duration connectionCheckPeriod = Duration.ofSeconds(10);
        /**
         * время за которое неиспользуемое подключение будет удлено из пула
         */
        private volatile Duration idleEvictTime = Duration.ofMinutes(1);
        /**
         * Максимальное время на подключение до сервера
         */
        private volatile Duration connectTimeout = Duration.ofSeconds(1);
        /**
         * Максимальное время выполеннение запроса.
         * Если запрос не будет выполнен за это время, соединение будет разорвано
         */
        private volatile Duration requestTimeout = Duration.ofSeconds(30);
        /**
         * Максимальное время жизни коннекта. Нужно для перебалансировки коннектов на новые сервера
         * Поскольку новые коннекты открываются на случайный сервер, то через maxConnectLiveTime
         * все коннекты перейдут на новый сервер
         */
        private volatile Duration maxConnectLiveTime = Duration.ofMinutes(5);
        /**
         * Минимальное количество конетктов, если пул не используется.
         * Уменшает время обработки первого запроса
         */
        private volatile int minIdleConnections = 0;
        /**
         * Максимальное число подключений до всех серверов в пуле
         */
        private volatile int maxConnections = 64;
        /**
         * Максимальный размер одного запроса, в байтах
         */
        private volatile int maxFrameSizeBytes = 2 * 1024 * 1024;

        private Predicate<TException> needCircuitBreakOnException = (ex) -> true;

        private Optional<ThriftTraceMode> traceMode = Optional.empty();

        private boolean lifo = true;
        private int socketReceiveBufferSize = -1;
        private int socketSendBufferSize = -1;

        private ThriftRequestReporter requestReporter = ThriftLogContext.getDefaultClientReporter();
        private Optional<String> subServiceName = Optional.empty();

        protected volatile ParameterSource source = ParameterSource.EMPTY;

        protected Builder() {
        }

        @SuppressWarnings("unchecked")
        protected final B getThis() {
            return (B) this;
        }


        public Duration getMaxWaitForConnection() {
            return source.getDuration("max_wait_for_connection_millis", maxWaitForConnection);
        }

        /**
         * Name: max_wait_for_connection_millis
         * @param maxWaitForConnection - the maximum amount of time pool wait for free connection before throw
         *                             connection exception
         *                             By default: 20 second
         * @return
         */
        public B setMaxWaitForConnection(Duration maxWaitForConnection) {
            this.maxWaitForConnection = Objects.requireNonNull(maxWaitForConnection);
            return getThis();
        }

        public Duration getConnectionCheckPeriod() {
            return source.getDuration("connection_check_period_millis", connectionCheckPeriod);
        }

        /**
         * Name: connection_check_period_millis
         * @param connectionCheckPeriod - period to check non used connection - to verify what it not broken
         *                             By default: 10 second
         * @return
         */
        public B setConnectionCheckPeriod(Duration connectionCheckPeriod) {
            this.connectionCheckPeriod = Objects.requireNonNull(connectionCheckPeriod);
            return getThis();
        }

        public Duration getIdleEvictTime() {
            return source.getDuration("idle_evict_time_millis", idleEvictTime);
        }

        /**
         * Name: idle_evict_time_millis
         * @param idleEvictTime - time before non used connection removed from pool
         * @return
         */
        public B setIdleEvictTime(Duration idleEvictTime) {
            this.idleEvictTime = Objects.requireNonNull(idleEvictTime);
            return getThis();
        }


        public Duration getConnectTimeout() {
            return source.getDuration("connect_timeout_millis", connectTimeout);
        }

        /**
         * Name: connect_timeout_millis
         * @param connectTimeout - if connection not success in this time error ot caller returned
         * @return
         */
        public B setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return getThis();
        }


        public Duration getRequestTimeout() {
            return source.getDuration("request_timeout_millis", requestTimeout);
        }

        /**
         * Name: request_timeout_millis
         * @param requestTimeout - if no response received to request in this time, connection to server will be closed
         * @return
         */
        public B setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return getThis();
        }

        public Duration getMaxConnectLiveTime() {
            return source.getDuration("max_connect_live_time_millis", maxConnectLiveTime);
        }

        /**
         * Name: max_connect_live_time_millis
         * @param maxConnectLiveTime - maximum time before connection closed. Allow re-balance connections to new servers,
         *                           when external balancer used.
         * @return
         */
        public B setMaxConnectLiveTime(Duration maxConnectLiveTime) {
            this.maxConnectLiveTime = Objects.requireNonNull(maxConnectLiveTime);
            return getThis();
        }
        
        public int getMinIdleConnections() {
            return source.getInteger("min_idle_connections", minIdleConnections);
        }

        /**
         * Name: min_idle_connections
         * @param minIdleConnections - minimum number of idle connection maintain in the pool
         * @return
         */
        public B setMinIdleConnections(int minIdleConnections) {
            this.minIdleConnections = minIdleConnections;
            return getThis();
        }

        public int getMaxConnections() {
            return source.getInteger("max_connections", maxConnections);
        }

        /**
         * Name: max_connections
         * @param maxConnections - maximum number of connections (maximum number of parallel requests).
         * @return
         */
        public B setMaxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return getThis();
        }

        public int getMaxFrameSizeBytes() {
            return source.getInteger("max_frame_size_bytes", maxFrameSizeBytes);
        }

        public ParameterSource getParameterSource() {
            return source;
        }
        /**
         * Name: max_frame_size_bytes
         * @param maxFrameSizeBytes - maximum possible request size in bytes
         * @return
         */
        public B setMaxFrameSizeBytes(int maxFrameSizeBytes) {
            this.maxFrameSizeBytes = maxFrameSizeBytes;
            return getThis();
        }

        protected Predicate<TException> getNeedCircuitBreakOnException() {
            return needCircuitBreakOnException;
        }

        protected B setNeedCircuitBreakOnException(Predicate<TException> needCircuitBreakOnException) {
            this.needCircuitBreakOnException = needCircuitBreakOnException;
            return getThis();
        }

        public Optional<ThriftTraceMode> getTraceMode() {
            return ThriftTraceMode.parse(source, "trace_mode", traceMode);
        }

        /**
         * Name: trace_type
         * @param traceMode - add to protocol sending trace_id inside every thrift message
         *                    Server must use TCompactTracedProtocol to activate this feature.
         *                    To unset value try to get configuration from client address
         * @return
         */
        public B setTraceMode(Optional<ThriftTraceMode> traceMode) {
            this.traceMode = Objects.requireNonNull(traceMode);
            return getThis();
        }

        public boolean isLifo() {
            return source.getBoolean("lifo", lifo);
        }

        public void setLifo(boolean lifo) {
            this.lifo = lifo;
        }

        public int getSocketReceiveBufferSize() {
            return source.getInteger("socket_receive_buffer_size", socketReceiveBufferSize);
        }

        public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
            this.socketReceiveBufferSize = socketReceiveBufferSize;
        }

        public int getSocketSendBufferSize() {
            return source.getInteger("socket_send_buffer_size", socketSendBufferSize);
        }

        public void setSocketSendBufferSize(int socketSendBufferSize) {
            this.socketSendBufferSize = socketSendBufferSize;
        }

        public ThriftRequestReporter getRequestReporter() {
            return requestReporter;
        }

        /**
         *
         * @param requestReporter - Allow report requests to zipkin or another trace servece
         */
        public B setRequestReporter(ThriftRequestReporter requestReporter) {
            this.requestReporter = Objects.requireNonNull(requestReporter);
            return getThis();
        }


        public Optional<String> getSubServiceName() {
            return Optional.ofNullable(source.getString("sub_service_name", subServiceName.orElse(null)));
        }

        /**
         * Name: sub_service_name
         * @param subServiceName allow register few services with same interface inside one thrift server
         */
        public B setSubServiceName(Optional<String> subServiceName) {
            this.subServiceName = subServiceName;
            return getThis();
        }

        public B fromParameters(ParameterSource source){
            this.source = source;
            return getThis();
        }

        public B fromAbstractConfig(ThriftAbstractClientConfig config){
            this.maxWaitForConnection = config.getMaxWaitForConnection();
            this.connectionCheckPeriod = config.getConnectionCheckPeriod();
            this.idleEvictTime = config.getIdleEvictTime();
            this.connectTimeout = config.getConnectTimeout();
            this.requestTimeout = config.getRequestTimeout();
            this.maxConnectLiveTime = config.getMaxConnectLiveTime();
            this.minIdleConnections = config.getMinIdleConnections();
            this.maxConnections = config.getMaxConnections();
            this.maxFrameSizeBytes = config.getMaxFrameSizeBytes();
            this.needCircuitBreakOnException = config.getNeedCircuitBreakOnException();
            this.traceMode = config.getTraceMode();
            this.lifo = config.isLifo();
            this.socketReceiveBufferSize = config.getSocketReceiveBufferSize();
            this.socketSendBufferSize = config.getSocketReceiveBufferSize();
            this.requestReporter = config.getRequestReporter();
            this.subServiceName = config.getSubServiceName();
            return getThis();
        }

    }

}
