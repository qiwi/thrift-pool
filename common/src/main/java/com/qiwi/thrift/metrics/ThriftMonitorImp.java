package com.qiwi.thrift.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;


class ThriftMonitorImp implements ThriftMonitor {
    private static final Logger log = LoggerFactory.getLogger(ThriftMonitorImp.class);
    public static final char SEPARATOR = '.';
    public static final String ALL = "all";


    private final MetricRegistry metrics = new MetricRegistry();
    private final GroupJmxReporter jmxReporter;
    private Closeable graphiteReporter = null;

    ThriftMonitorImp() {
        jmxReporter = GroupJmxReporter.forRegistry(metrics)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .inDomain("thrift")
                .build();
        jmxReporter.start();

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    try {
                        close();
                    } catch (Throwable ex) {
                        log.error("Unable to stop reporter", ex);
                    }
                }
        ));
    }

    @Override
    public MetricEnabledStatus getEnableStatus() {
        return MetricEnabledStatus.ENABLED;
    }

    private static StringBuilder getServicePath(ThriftCallType type, String serviceName){
        StringBuilder path = new StringBuilder(64);
        path.append(type)
                .append(SEPARATOR)
                .append(serviceName)
                .append(SEPARATOR);
        return path;
    }

    private static void getNodePath(StringBuilder path, String node){
        path.append(node)
                .append(SEPARATOR);
    }

    private static void getMethodPath(StringBuilder path, String method){
        path.append("method")// Нужно на случай если имя метода совпадет с доп-параметром, напрмер error
                .append(SEPARATOR)
                .append(method)
                .append(SEPARATOR);
    }

    @Override
    public void logMethodCall(
            ThriftCallType type,
            ThriftClientAddress address,
            String serviceName,
            String method,
            long nanos,
            ThriftRequestStatus requestStatus
    ) {
        try {
            StringBuilder path = getServicePath(type, serviceName);
            int length = path.length();
            path.setLength(length);
            getNodePath(path, address.toEscapedString());
            logCall(path, method, nanos, requestStatus);

            path.setLength(length);
            getNodePath(path, address.toEscapedString());
            logCall(path, null, nanos, requestStatus);

            if (type.isLogAllHostStat()) {
                path.setLength(length);
                getNodePath(path, ALL);
                logCall(path, null, nanos, requestStatus);

                path.setLength(length);
                getNodePath(path, ALL);
                logCall(path, method, nanos, requestStatus);
            }
        } catch (Throwable ex) {
            ThriftMonitoring.disable();
            log.error("Thrift metric fail to run", ex);
        }
    }


    private void logCall(StringBuilder path, String method, long nanos, ThriftRequestStatus requestStatus){
        if (method != null) {
            getMethodPath(path, method);
        }
        int length = path.length();

        path.append("requests");
        metrics.timer(path.toString()).update(nanos, TimeUnit.NANOSECONDS);

        switch (requestStatus) {
            case SUCCESS:
                break;
            case UNEXPECTED_ERROR:
                path.setLength(length);
                path.append("unexpectedError");
                metrics.meter(path.toString()).mark();
                path.setLength(length);
                path.append("error");
                metrics.meter(path.toString()).mark();
                break;
            case APP_ERROR:
                path.setLength(length);
                path.append("appError");
                metrics.meter(path.toString()).mark();
                break;
            case CONNECTION_ERROR:
                path.setLength(length);
                path.append("connectionError");
                metrics.meter(path.toString()).mark();
                path.setLength(length);
                path.append("error");
                metrics.meter(path.toString()).mark();
                break;
            case INTERNAL_ERROR:
                path.setLength(length);
                path.append("error");
                metrics.meter(path.toString()).mark();
                break;
        }
    }

    @Override
    public synchronized boolean registerPool(
            String serviceName,
            DoubleSupplier load,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            LongSupplier failNodes,
            Supplier<List<ThriftClientAddress>> failNodeList
    ){
        try {
            StringBuilder path = getServicePath(ThriftCallType.SYNC_BALANCER, serviceName);
            getNodePath(path, ALL);
            int length = path.length();

            path.append("load");
            if (metrics.getMetrics().containsKey(path.toString())) {
                log.error("Double register pool for service: {}, some metric not available", serviceName);
                return false;
            }
            path.setLength(length);
            registerGauge(path, "load", load::getAsDouble);
            registerGauge(path, "usedConnections", usedConnections::getAsLong);
            registerGauge(path, "openConnections", openConnections::getAsLong);
            registerGauge(path, "failNodes", failNodes::getAsLong);
            registerGauge(path, "failNodeList", () -> failNodeList.get().stream()
                    .map(ThriftClientAddress::toString)
                    .collect(Collectors.joining(","))
            );
            registerMeterRatio(path, "errorRatio", "error", "requests");
            return true;
        } catch (Throwable ex) {
            ThriftMonitoring.disable();
            log.error("Thrift metric fail to register for service: {}", serviceName, ex);
            return false;
        }
    }

    @Override
    public void unRegisterPool(
            String serviceName
    ) {
        try {
            StringBuilder path = getServicePath(ThriftCallType.SYNC_BALANCER, serviceName);
            getNodePath(path, ALL);
            remove(path, "load");
            remove(path, "failNodes");
            remove(path, "usedConnections");
            remove(path, "openConnections");
            remove(path, "failNodeList");
            remove(path, "errorRatio");
        } catch (Throwable ex) {
            log.error("Thrift metric fail to register for service: {}", serviceName, ex);
        }
    }

    @Override
    public void registerDc(
            String serviceName,
            String dcName,
            DoubleSupplier load,
            DoubleSupplier weight,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            LongSupplier failNodes,
            Supplier<String> status
    ) {
        try {
            StringBuilder path = getServicePath(ThriftCallType.SYNC_BALANCER, serviceName);
            getNodePath(path, ALL + '.' + dcName);
            registerGauge(path, "load", load::getAsDouble);
            registerGauge(path, "weight", weight::getAsDouble);
            registerGauge(path, "usedConnections", usedConnections::getAsLong);
            registerGauge(path, "openConnections", openConnections::getAsLong);
            registerGauge(path, "failNodes", failNodes::getAsLong);
            registerGauge(path, "status", status::get);
        } catch (Throwable ex) {
            log.error("Thrift metric fail to register for service: {}", serviceName, ex);
        }
    }

    @Override
    public void unRegisterDc(
            String serviceName,
            String dcName
    ) {
        try {
            StringBuilder path = getServicePath(ThriftCallType.SYNC_BALANCER, serviceName);
            getNodePath(path, ALL + '.' + dcName);
            remove(path, "load");
            remove(path, "weight");
            remove(path, "usedConnections");
            remove(path, "openConnections");
            remove(path, "failNodes");
            remove(path, "status");
        } catch (Throwable ex) {
            log.error("Thrift metric fail to register for service: {}", serviceName, ex);
        }
    }


    @Override
    public void registerNode(
            String serviceName,
            ThriftClientAddress address,
            DoubleSupplier load,
            DoubleSupplier weight,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            Supplier<String> status,
            LongSupplier statusId,
            DoubleSupplier latencyMs
    ) {
        try {
            StringBuilder path = getServicePath(ThriftCallType.SYNC_BALANCER, serviceName);
            getNodePath(path, address.toEscapedString());
            registerGauge(path, "load", load::getAsDouble);
            registerGauge(path, "weight", weight::getAsDouble);
            registerGauge(path, "usedConnections", usedConnections::getAsLong);
            registerGauge(path, "openConnections", openConnections::getAsLong);
            registerGauge(path, "status", status::get);
            registerGauge(path, "statusId", statusId::getAsLong);
            registerGauge(path, "latency", latencyMs::getAsDouble);
            registerMeterRatio(path, "errorRatio", "error", "requests");
            registerMeterRatio(path, "appErrorRatio", "appError", "requests");
        } catch (Throwable ex) {
            log.error("Thrift metric fail to register for service: {}", serviceName, ex);
        }
    }

    @Override
    public void unRegisterNode(
            String serviceName,
            ThriftClientAddress address
    ){
        try {
            StringBuilder path = getServicePath(ThriftCallType.SYNC_BALANCER, serviceName);
            getNodePath(path, address.toEscapedString());
            remove(path, "load");
            remove(path, "weight");
            remove(path, "usedConnections");
            remove(path, "openConnections");
            remove(path, "status");
            remove(path, "statusId");
            remove(path, "latency");
            remove(path, "errorRatio");
        } catch (Throwable ex) {
            log.error("Thrift metric fail to register for service: {}", serviceName, ex);
        }
    }


    @Override
    public void connectToGraphite(ThriftGraphiteConfig config){
        try {
            if (!config.isGraphiteEnabled()) {
                return;
            }
            if (graphiteReporter != null) {
                return;
            }
            String prefix = String.format(
                    "$type.app.$cluster.%s.$host.%s.$metric.",
                    ThriftMonitoring.escapeDots(config.getCluster()),
                    ThriftMonitoring.escapeDots(config.getHostName())
            );
            GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith(prefix)
                    .build(new Graphite(new InetSocketAddress(
                            config.getGraphiteHost(),
                            config.getGraphitePort()
                    )));
            this.graphiteReporter = reporter;
            reporter.start(config.getGraphitePushInterval().toMillis(), TimeUnit.MILLISECONDS);

            log.info("graphite metrics reporter started");
        } catch (Throwable ex) {
            log.error("Unable to start graphite", ex);
        }
    }

    @Override
    public void close() {
        try {
            jmxReporter.close();
            Closeable graphiteReporterCopy = graphiteReporter;
            if (graphiteReporterCopy != null) {
                    graphiteReporterCopy.close();
                graphiteReporter = null;
            }
        } catch (IOException e) {
            log.error("Unable to close monitoring", e);
        }
    }

    public <T> void registerGauge(StringBuilder path, String name, Gauge<T> gauge){
        int length = path.length();
        path.append(name);
        metrics.register(path.toString(), gauge);
        path.setLength(length);
    }

    public void registerMeterRatio(
            StringBuilder path,
            String name,
            String numeratorName,
            String denominatorName
    ){
        int length = path.length();
        path.append(numeratorName);
        String numeratorPath = path.toString();
        path.setLength(length);
        path.append(denominatorName);
        String denominatorPath = path.toString();
        path.setLength(length);

        path.append(name);
        metrics.register(path.toString(), new MeterRatio(numeratorPath, denominatorPath, metrics));
        path.setLength(length);
    }

    public void remove(StringBuilder path, String name){
        int length = path.length();
        path.append(name);
        metrics.remove(path.toString());
        path.setLength(length);
    }

    private static class MeterRatio extends Meter {
        public final String numeratorPath;
        public final String denominatorPath;
        public final MetricRegistry registry;

        private MeterRatio(String numeratorPath, String denominatorPath, MetricRegistry registry) {
            this.numeratorPath = numeratorPath;
            this.denominatorPath = denominatorPath;
            this.registry = registry;
        }

        public Optional<Metered> getMetered(String path){
            return Optional.ofNullable((Metered) registry.getMetrics().get(path));
        }

        public double getRate(ToDoubleFunction<Metered> getter) {
            Optional<Metered> denominatorMetered = getMetered(denominatorPath);
            double denominator = denominatorMetered.isPresent()
                    ? getter.applyAsDouble(denominatorMetered.get())
                    : 0;
            if (!Double.isFinite(denominator) || denominator < 0.000001) {
                return 0;
            }
            Optional<Metered> numeratorMetered = getMetered(numeratorPath);
            double numerator = numeratorMetered.isPresent()
                    ? getter.applyAsDouble(numeratorMetered.get())
                    : 0;
            if (!Double.isFinite(numerator)) {
                return 0;
            }
            return numerator / denominator;
        }

        @Override
        public void mark() {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void mark(long n) {
            throw new UnsupportedOperationException("mark(long n)");
        }

        @Override
        public long getCount() {
            return registry.meter(numeratorPath).getCount();
        }

        @Override
        public double getFifteenMinuteRate() {
            return getRate(Metered::getFifteenMinuteRate);
        }

        @Override
        public double getFiveMinuteRate() {
            return getRate(Metered::getFiveMinuteRate);
        }

        @Override
        public double getOneMinuteRate() {
            return getRate(Metered::getOneMinuteRate);
        }

        @Override
        public double getMeanRate() {
            return getRate(Metered::getMeanRate);
        }

    }

}
