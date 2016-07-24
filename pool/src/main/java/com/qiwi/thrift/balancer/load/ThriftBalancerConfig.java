package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.pool.ThriftAbstractClientConfig;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ThriftBalancerConfig extends ThriftAbstractClientConfig {
    private static final Logger log = LoggerFactory.getLogger(ThriftBalancerConfig.class);

    /**
     * Если при при попытке подать нагрузку - нода падает, то время до того как нода в следующий раз
     * будет возвращена в пул растет по экспоненте.
     * Эти два коэффициента определяют во сколько раз вырастет время при падении ноды
     * SOFT_FAIL - если нода упала во время тестов
     * HARD_FAIL - если нода упала когда была под полной нагрузкой
     */
    public static final double TIMEOUT_GROW_FACTOR_HARD_FAIL = 4;
    public static final double TIMEOUT_GROW_FACTOR_SOFT_FAIL = 1.5;
    /**
     * Загрузка ноды при которой сработает Circuit Breaker
     */
    public static final double MAX_NODE_LOAD = 2.0;
    /**
     * Максимальный период между попытками подключить ноду.
     */
    public static final Duration MAX_NODE_DISABLE_TIME = Duration.ofHours(1);
    /**
     * Максимальное время (деленное на два) которое нода может находится в состоянии Test, прежде чем
     * признается поднявшейся автоматически.
     */
    public static final Duration MAX_NODE_TEST_TIME = Duration.ofMinutes(2);
    /**
     * Нужно для того чтобы нагрузка на ноды выравнивалась со временем
     */
    public static final int LATENCY_OFFSET_NANOS = 500_000;

    private final Supplier<Set<ThriftClientAddress>> serversSupplier;
    private final Duration nodesHealthCheckPeriod;
    private final Duration ringReBalancePeriod;
    private final Duration ringNodesReloadPeriod;
    private final Duration nodeDisableTime;
    private final double circuitBreakerErrorRatio;
    private final double thresholdConnectionErrorRatio;
    private final double maxNodeLoadGap;
    private final double minNodesInDcRatio;
    private final int minAliveNodes;
    private final int maxFailNodes;
    private final Duration nodeCloseTime;
    private final String preferredDc;
    private final MethodOfFailureHandling failureHandling;

    private final double dcLoadFilterFactor;
    private final double dcLoadPredictFactor;
    private final double dcWeightFilterFactor;

    private final double nodeLoadFilterFactor;
    private final double nodeLoadPredictFactor;
    private final double nodeWeightFilterFactor;

    protected ThriftBalancerConfig(
            UntypedBuilder<?> builder
    ) {
        super(builder);
        this.serversSupplier = builder.getServersSupplier();
        this.nodesHealthCheckPeriod = builder.getNodesHealthCheckPeriod();
        this.ringReBalancePeriod = builder.getRingReBalancePeriod();
        this.ringNodesReloadPeriod = builder.getRingNodesReloadPeriod();
        this.nodeDisableTime = builder.getNodeDisableTime();
        this.circuitBreakerErrorRatio = builder.getCircuitBreakerErrorRatio();
        this.thresholdConnectionErrorRatio = builder.getThresholdConnectionErrorRatio();
        this.maxNodeLoadGap = builder.getMaxNodeLoadGap();
        this.minNodesInDcRatio = builder.getMinNodesInDcRatio();
        this.minAliveNodes = builder.getMinAliveNodes();
        this.maxFailNodes = builder.getMaxFailNodes();
        this.nodeCloseTime = builder.getNodeCloseTime();
        this.preferredDc = builder.getPreferredDc();
        this.failureHandling = builder.getFailureHandling();
        this.dcLoadFilterFactor = builder.getDcLoadFilterFactor();
        this.dcLoadPredictFactor = builder.getDcLoadPredictFactor();
        this.dcWeightFilterFactor = builder.getDcWeightFilterFactor();
        this.nodeLoadFilterFactor = builder.getNodeLoadFilterFactor();
        this.nodeLoadPredictFactor = builder.getNodeLoadPredictFactor();
        this.nodeWeightFilterFactor = builder.getNodeWeightFilterFactor();
    }

    public Supplier<Set<ThriftClientAddress>> getServersSupplier() {
        return serversSupplier;
    }

    public Duration getNodesHealthCheckPeriod() {
        return nodesHealthCheckPeriod;
    }

    public Duration getRingReBalancePeriod() {
        return ringReBalancePeriod;
    }

    public Duration getRingNodesReloadPeriod() {
        return ringNodesReloadPeriod;
    }

    public Duration getNodeDisableTime() {
        return nodeDisableTime;
    }

    public double getCircuitBreakerErrorRatio() {
        return circuitBreakerErrorRatio;
    }

    public double getThresholdConnectionErrorRatio() {
        return thresholdConnectionErrorRatio;
    }

    public double getMaxNodeLoadGap() {
        return maxNodeLoadGap;
    }

    public double getMinNodesInDcRatio() {
        return minNodesInDcRatio;
    }

    public int getMinAliveNodes() {
        return minAliveNodes;
    }

    public int getMaxFailNodes() {
        return maxFailNodes;
    }

    public Duration getNodeCloseTime() {
        return nodeCloseTime;
    }

    public String getPreferredDc() {
        return preferredDc;
    }

    public MethodOfFailureHandling getFailureHandling() {
        return failureHandling;
    }

    public ThriftClientConfig createClientConfig(ThriftClientAddress address, int maxConnectionsCount, ThriftRequestReporter requestReporter){
        return new ThriftClientConfig.Builder(true) {}
                .fromAbstractConfig(this)
                .setAddress(address)
                .setMaxConnections(Math.max(getMinIdleConnections(), maxConnectionsCount))
                .setRequestReporter(requestReporter)
                .build();
    }

    public double getDcLoadFilterFactor() {
        return dcLoadFilterFactor;
    }

    public double getDcLoadPredictFactor() {
        return dcLoadPredictFactor;
    }

    public double getDcWeightFilterFactor() {
        return dcWeightFilterFactor;
    }

    public double getNodeLoadFilterFactor() {
        return nodeLoadFilterFactor;
    }

    public double getNodeLoadPredictFactor() {
        return nodeLoadPredictFactor;
    }

    public double getNodeWeightFilterFactor() {
        return nodeWeightFilterFactor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ThriftBalancerConfig that = (ThriftBalancerConfig) o;

        if (Double.compare(that.circuitBreakerErrorRatio, circuitBreakerErrorRatio) != 0) {
            return false;
        }
        if (Double.compare(that.thresholdConnectionErrorRatio, thresholdConnectionErrorRatio) != 0) {
            return false;
        }
        if (Double.compare(that.maxNodeLoadGap, maxNodeLoadGap) != 0) {
            return false;
        }
        if (Double.compare(that.minNodesInDcRatio, minNodesInDcRatio) != 0) {
            return false;
        }
        if (minAliveNodes != that.minAliveNodes) {
            return false;
        }
        if (maxFailNodes != that.maxFailNodes) {
            return false;
        }
        if (Double.compare(that.dcLoadFilterFactor, dcLoadFilterFactor) != 0) {
            return false;
        }
        if (Double.compare(that.dcLoadPredictFactor, dcLoadPredictFactor) != 0) {
            return false;
        }
        if (Double.compare(that.dcWeightFilterFactor, dcWeightFilterFactor) != 0) {
            return false;
        }
        if (Double.compare(that.nodeLoadFilterFactor, nodeLoadFilterFactor) != 0) {
            return false;
        }
        if (Double.compare(that.nodeLoadPredictFactor, nodeLoadPredictFactor) != 0) {
            return false;
        }
        if (Double.compare(that.nodeWeightFilterFactor, nodeWeightFilterFactor) != 0) {
            return false;
        }
        if (!nodesHealthCheckPeriod.equals(that.nodesHealthCheckPeriod)) {
            return false;
        }
        if (!ringReBalancePeriod.equals(that.ringReBalancePeriod)) {
            return false;
        }
        if (!ringNodesReloadPeriod.equals(that.ringNodesReloadPeriod)) {
            return false;
        }
        if (!nodeDisableTime.equals(that.nodeDisableTime)) {
            return false;
        }
        if (!nodeCloseTime.equals(that.nodeCloseTime)) {
            return false;
        }
        if (!preferredDc.equals(that.preferredDc)) {
            return false;
        }
        return failureHandling == that.failureHandling;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        result = 31 * result + nodesHealthCheckPeriod.hashCode();
        result = 31 * result + ringReBalancePeriod.hashCode();
        result = 31 * result + ringNodesReloadPeriod.hashCode();
        result = 31 * result + nodeDisableTime.hashCode();
        temp = Double.doubleToLongBits(circuitBreakerErrorRatio);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(thresholdConnectionErrorRatio);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxNodeLoadGap);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minNodesInDcRatio);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + minAliveNodes;
        result = 31 * result + maxFailNodes;
        result = 31 * result + nodeCloseTime.hashCode();
        result = 31 * result + preferredDc.hashCode();
        result = 31 * result + failureHandling.hashCode();
        temp = Double.doubleToLongBits(dcLoadFilterFactor);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(dcLoadPredictFactor);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(dcWeightFilterFactor);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(nodeLoadFilterFactor);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(nodeLoadPredictFactor);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(nodeWeightFilterFactor);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static class Builder extends UntypedBuilder<Builder> {

        /**
         * private final double minNodesInRingRatio;
         * private final int minAliveNodes;
         * private final int maxFailNodes;
         *
         * @param failureHandling - CIRCUIT_BREAK if application can work without this pool
         *                        TRY_CONTINUE - if pool critical to current application.
         */
        public Builder(MethodOfFailureHandling failureHandling) {
            super(failureHandling);
        }

        public ThriftBalancerConfig build() {
            return new ThriftBalancerConfig(this);
        }
    }
    /**
     * Полезные настройки
     * preferredDc - чтобы запросы не летали между датацентров без лишней необходимости.
     * addressList - пул умеет грузить список адресов не только из консула
     */
    @SuppressWarnings({"MagicNumber", "ClassNameSameAsAncestorName", "ClassWithTooManyFields", "ClassWithTooManyMethods"})
    public abstract static class UntypedBuilder<B extends UntypedBuilder<B>> extends ThriftAbstractClientConfig.Builder<B> {
        private volatile MethodOfFailureHandling failureHandling;
        private volatile Supplier<Set<ThriftClientAddress>> serversSupplier = null;
        private volatile Duration nodesHealthCheckPeriod = Duration.ofSeconds(10);
        private volatile Duration ringReBalancePeriod = Duration.ofSeconds(5);
        private volatile Duration ringNodesReloadPeriod = Duration.ofSeconds(60);
        private volatile Duration nodeDisableTime = Duration.ofSeconds(30);
        private volatile double circuitBreakerErrorRatio = 0.90;
        private volatile double thresholdConnectionErrorRatio = 0.25;
        private volatile double maxNodeLoadGap = 0.25;
        private volatile double minNodesInDcRatio = 0.405;// Из пяти нод, должны быть две
        private volatile int minAliveNodes = 1;
        private volatile int maxFailNodes = -1;
        private volatile Duration nodeCloseTime = Duration.ofMinutes(30);
        private volatile String preferredDc = ThriftClientAddress.DEFAULT_DC_NAME_CLIENT;

        /**
         * Параметр подобраны по результатам мат-моделирования см BalancerLoadTest и balance_model.xmcd (Mathcad)
         * С этими уровнями фильтрации нагрузка достаточно быстро сходится к оптимальной (2-3 минуты), хорошо реагирует
         * на катастрофические падения.
         *
         * Конфидент фильтрации (DC_LOAD_FILTER_FACTOR) уменьшать нельзя так как у нас несколько клиентов, и между
         * ними могут возникать явления самовозбуждения. Возможно он даже слишком маленький?
         *
         * В принципе моделирование показывает - что при нагрузке больше 100% само-возбуждение все-же возможно.
         * Но при этом все сервера будут уже лежать, так что разницы ни какой нет. Считаю что применение более
         * продвинутого фильтра уже over-engineering
         */
        private volatile double dcLoadFilterFactor = 0.90;
        private volatile double dcLoadPredictFactor = 0.93;
        private volatile double dcWeightFilterFactor = 0.85;

        private volatile double nodeLoadFilterFactor = 0.94;
        private volatile double nodeLoadPredictFactor = 0.96;
        private volatile double nodeWeightFilterFactor = 0.90;

        /**
         private final double minNodesInRingRatio;
         private final int minAliveNodes;
         private final int maxFailNodes;
         *
         * @param failureHandling CIRCUIT_BREAK - if application can work without this pool
         *                        TRY_CONTINUE  - if pool critical to current application.
         */

        public UntypedBuilder(MethodOfFailureHandling failureHandling) {
            this.failureHandling = failureHandling;
        }

        public MethodOfFailureHandling getFailureHandling() {
            String val = source.getString("failure_handling", failureHandling.name());
            try {
                return MethodOfFailureHandling.valueOf(val);
            } catch (IllegalArgumentException ex) {
                log.error("Unable to parse parameter {} with value {}", source.getFullPath("failure_handling"), val, ex);
                return failureHandling;
            }
        }

        /**
         * Name: failure_handling
         * @param failureHandling  CIRCUIT_BREAK - if application can work without this pool
         *                         TRY_CONTINUE - if pool critical to current application.
         */
        public void setFailureHandling(MethodOfFailureHandling failureHandling) {
            this.failureHandling = failureHandling;
        }

        public Supplier<Set<ThriftClientAddress>> getServersSupplier() {
            Supplier<Set<ThriftClientAddress>> supplierCopy = this.serversSupplier;
            return new Supplier<Set<ThriftClientAddress>>() {
                Supplier<Set<ThriftClientAddress>> serversSupplier1;
                @Override
                public Set<ThriftClientAddress> get() {
                    String servers = source.getString("servers", null);
                    if (servers == null) {
                        if (supplierCopy != null) {
                            return supplierCopy.get();
                        }
                        throw new IllegalStateException(
                                "Parameters " + source.getFullPath("servers") + " not found");
                    }
                    return new HashSet<>(ThriftClientAddress.parseList(servers));
                }
            };
        }

        /**
         * @param addressList Список нод, ноды должны иметь прописанный дата-центр
         * @return
         */
         public B setServers(Collection<ThriftClientAddress> addressList) {
            Set<ThriftClientAddress> copy = new HashSet<>(addressList);
            this.serversSupplier = ()-> copy;
            return getThis();
        }

        /**
          * Name: servers
          * @param serversSupplier - Supplier to load nodes from external source. For example consul.
          *                         See thrift-pool-consul module.
          * @return
          */
        public B setServersSupplier(Supplier<Set<ThriftClientAddress>> serversSupplier) {
            this.serversSupplier = serversSupplier;
            return getThis();
        }

        public Duration getNodesHealthCheckPeriod() {
            return source.getDuration("nodes_health_check_period_ms", nodesHealthCheckPeriod);
        }

        /**
         * Name: ring_re_balance_period_ms
         * @param nodesHealthCheckPeriod - period between isHealthCheckOk call to detect node steel online (10 second, by default)
         *                               Node fault detection take nodesHealthCheckPeriod + ringReBalancePeriod
         *                               (15 sec by default)
         * @return
         */
        public B setNodesHealthCheckPeriod(Duration nodesHealthCheckPeriod) {
            this.nodesHealthCheckPeriod = Objects.requireNonNull(nodesHealthCheckPeriod);
            return getThis();
        }

        public Duration getRingReBalancePeriod() {
            return source.getDuration("ring_re_balance_period_ms", ringReBalancePeriod);
        }

        /**
         * Name: ring_re_balance_period_ms
         * @param ringReBalancePeriod period between re-balancing operation run. Recommended 100-10_000 ms
         *                            <b>At normal load 95% request Must complete in this period!</b>
         *                            Circuit breaker activated after 3 period 15 sec by default.
         *
         *                            Re-ballsing on node fault, competed after 4-6 periods (20-30 second, by default).
         *                            Optimal chose:
         *                            Period should be enough for node to make 100 request to remote servers in normal load
         *                            For example: test send 1000 request per second, 100 request per 0.1
         *                            Optimal value 100 ms.
         * @return
         */
        public B setRingReBalancePeriod(Duration ringReBalancePeriod) {
            this.ringReBalancePeriod = Objects.requireNonNull(ringReBalancePeriod);
            return getThis();
        }

        public Duration getRingNodesReloadPeriod() {
            return source.getDuration("ring_nodes_reload_period_ms", ringNodesReloadPeriod);
        }


        /**
         * Name: ring_nodes_reload_period_ms
         * @param ringNodesReloadPeriod - period between list of node queried from external service. Consul for example
         * @return
         */
        public B setRingNodesReloadPeriod(Duration ringNodesReloadPeriod) {
            this.ringNodesReloadPeriod = Objects.requireNonNull(ringNodesReloadPeriod);
            return getThis();
        }

        public Duration getNodeDisableTime() {
            return source.getDuration("node_disable_time_ms", nodeDisableTime);
        }

        /**
         * Name: node_disable_time_ms
         * @param nodeDisableTime - period to disable node after circuit breaker activated
         *                        This period grow exponentially if error repeat.
         * @return
         */
        public B setNodeDisableTime(Duration nodeDisableTime) {
            this.nodeDisableTime = Objects.requireNonNull(nodeDisableTime);
            return getThis();
        }

        public double getCircuitBreakerErrorRatio() {
            return source.getDouble("circuit_breaker_error_ratio", circuitBreakerErrorRatio);
        }

        /**
         * Name: max_error_ratio
         * @param circuitBreakerErrorRatio max application side error ratio to activate circuit breaker
         *                      But at least eight*maxErrorRatio errors per ringReBalancePeriod*4
         *                      By default: 50%, but not at least four error per 20 second.
         * @return
         */
        public B setCircuitBreakerErrorRatio(double circuitBreakerErrorRatio) {
            this.circuitBreakerErrorRatio = circuitBreakerErrorRatio;
            return getThis();
        }

        public double getThresholdConnectionErrorRatio() {
            return source.getDouble("threshold_connection_error_ratio", thresholdConnectionErrorRatio);
        }

        /**
         * Name: threshold_connection_error_ratio
         * @param thresholdConnectionErrorRatio max connection related error before circuit breaker activated
         *                                  By default: 0.25
         * @return
         */
        public B setThresholdConnectionErrorRatio(double thresholdConnectionErrorRatio) {
            this.thresholdConnectionErrorRatio = thresholdConnectionErrorRatio;
            return getThis();
        }

        public double getMaxNodeLoadGap() {
            return source.getDouble("max_node_load_gap", maxNodeLoadGap);
        }

        /**
         * Name: max_node_load_gap
         * @param maxNodeLoadGap - maximal difference between node load and dc load before circuit breaker activated
         *                        By default: 35%
         *                        For example node load 85% percent, dc load 60% - circuit beaked - because
         *                            node seem crashed.
         *                        Gap - is low, because load balancer compensate load disproportion
         * @return
         */
        public B setMaxNodeLoadGap(double maxNodeLoadGap) {
            this.maxNodeLoadGap = maxNodeLoadGap;
            return getThis();
        }

        public double getMinNodesInDcRatio() {
            return source.getDouble("min_nodes_in_dc_ratio", minNodesInDcRatio);
        }

        /**
         * Name: min_nodes_in_dc_ratio
         * @param minNodesInDcRatio - minimal nodes ratio remaining ring, before circuit breaker activated
         *
         *                          Note: if node address service not save node addresses after nodes fail,
         *                          this ratio reset after nodeCloseTime (30 minutes by default) expired, or after server restart.
         *                          I'm recommend use minAliveNodes, and minNodesInDcRatio just fail-back for users,
         *                          that forgot set it.
         *
         * @return
         */
        public B setMinNodesInDcRatio(double minNodesInDcRatio) {
            this.minNodesInDcRatio = minNodesInDcRatio;
            return getThis();
        }

        public int getMinAliveNodes() {
            return source.getInteger("min_alive_nodes", minAliveNodes);
        }

        /**
         * Name: min_alive_nodes
         * @param minAliveNodes - minimal alive nodes in dc, before in added to balance. Prevent node overload and crash,
         *                      when starting first nodes.
         *
         */

        public void setMinAliveNodes(int minAliveNodes) {
            if (minAliveNodes < 1) {
                throw new IllegalArgumentException("minAliveNodes " + minAliveNodes);
            }
            this.minAliveNodes = minAliveNodes;
        }


        public int getMaxFailNodes() {
            return source.getInteger("max_fail_nodes", maxFailNodes);
        }

        /**
         * Name: node_close_time
         * @param maxFailNodes - max fail nodes before decenter removed from balancer.
         *                       Used for consistency hash balancer, to get nodes time for reload missing keys.
         *
         *                       Note: if node address service not save node addresses after nodes fail,
         *                       this ratio reset after nodeCloseTime (30 minutes by default) expired, or after server restart.
         *                       I'm recommend use minAliveNodes, and minNodesInDcRatio just fail-back for users,
         *                       that forgot set it.
         *
         * @return
         */
        public B setMaxFailNodes(int maxFailNodes) {
            this.maxFailNodes = maxFailNodes;
            return getThis();
        }

        public Duration getNodeCloseTime() {
            return source.getDuration("node_close_time_ms", nodeCloseTime);
        }

        /**
         * Name: node_close_time_ms
         * @param nodeCloseTime - time gap between node address disappear from addressSupplier
         *                      an pool forgot about node.
         *                      Used to maxFailNodes and minNodesInDcRatio
         * @return
         */
        public B setNodeCloseTime(Duration nodeCloseTime) {
            this.nodeCloseTime = Objects.requireNonNull(nodeCloseTime);
            return getThis();
        }

        public String getPreferredDc() {
            return source.getString("preferred_dc", preferredDc);
        }


        /**
         * Name: preferred_dc
         * @param preferredDc - name of preferred dc. Pool send almost all (95%) load to proffered dc,
         *                    if load grow over 50%, pool begin seeding requests to other dc.
         * @return
         */
        public B setPreferredDc(String preferredDc) {
            this.preferredDc = preferredDc;
            return getThis();
        }

        public double getDcLoadFilterFactor() {
            return source.getDouble("dc_load_filter_factor", dcLoadFilterFactor);
        }

        public B setDcLoadFilterFactor(double dcLoadFilterFactor) {
            this.dcLoadFilterFactor = dcLoadFilterFactor;
            return getThis();
        }

        public double getDcLoadPredictFactor() {
            return source.getDouble("dc_load_predict_factor", dcLoadPredictFactor);
        }

        public B setDcLoadPredictFactor(double dcLoadPredictFactor) {
            this.dcLoadPredictFactor = dcLoadPredictFactor;
            return getThis();
        }

        public double getDcWeightFilterFactor() {
            return source.getDouble("dc_weight_filter_factor", dcWeightFilterFactor);
        }

        public B setDcWeightFilterFactor(double dcWeightFilterFactor) {
            this.dcWeightFilterFactor = dcWeightFilterFactor;
            return getThis();
        }

        public double getNodeLoadFilterFactor() {
            return source.getDouble("node_load_filter_factor", nodeLoadFilterFactor);
        }

        public B setNodeLoadFilterFactor(double nodeLoadFilterFactor) {
            this.nodeLoadFilterFactor = nodeLoadFilterFactor;
            return getThis();
        }

        public double getNodeLoadPredictFactor() {
            return source.getDouble("node_load_predict_factor", nodeLoadPredictFactor);
        }

        public B setNodeLoadPredictFactor(double nodeLoadPredictFactor) {
            this.nodeLoadPredictFactor = nodeLoadPredictFactor;
            return getThis();
        }

        public double getNodeWeightFilterFactor() {
            return source.getDouble("node_weight_filter_factor", nodeWeightFilterFactor);
        }

        public B setNodeWeightFilterFactor(double nodeWeightFilterFactor) {
            this.nodeWeightFilterFactor = nodeWeightFilterFactor;
            return getThis();
        }

        @Override
        public Predicate<TException> getNeedCircuitBreakOnException() {
            return super.getNeedCircuitBreakOnException();
        }

        /**
         *
         * @param errorRatio circuit break threshold for not application level errors
         * @param needCircuitBreakOnException return true if need circuit break on tested exception.
         *                              Connection related exceptions handled by pool, and not passed to checker
         * @return
         */
        public B enableCircuitBreak(double errorRatio, Predicate<TException> needCircuitBreakOnException) {
            setCircuitBreakerErrorRatio(errorRatio);
            return setNeedCircuitBreakOnException(needCircuitBreakOnException);
        }

        public abstract ThriftBalancerConfig build();

        public B fromBalancerConfig(ThriftBalancerConfig config) {
            fromAbstractConfig(config);
            this.failureHandling = config.getFailureHandling();
            this.serversSupplier = config.getServersSupplier();
            this.nodesHealthCheckPeriod = config.getNodesHealthCheckPeriod();
            this.ringReBalancePeriod = config.getRingReBalancePeriod();
            this.ringNodesReloadPeriod = config.getRingNodesReloadPeriod();
            this.nodeDisableTime = config.getNodeDisableTime();
            this.circuitBreakerErrorRatio = config.getCircuitBreakerErrorRatio();
            this.thresholdConnectionErrorRatio = config.getThresholdConnectionErrorRatio();
            this.maxNodeLoadGap = config.getMaxNodeLoadGap();
            this.minNodesInDcRatio = config.getMinNodesInDcRatio();
            this.minAliveNodes = config.getMinAliveNodes();
            this.maxFailNodes = config.getMaxFailNodes();
            this.nodeCloseTime = config.getNodeCloseTime();
            this.preferredDc = config.getPreferredDc();
            this.dcLoadFilterFactor = config.getDcLoadFilterFactor();
            this.dcLoadPredictFactor = config.getDcLoadPredictFactor();
            this.dcWeightFilterFactor = config.getDcWeightFilterFactor();
            this.nodeLoadFilterFactor = config.getNodeLoadFilterFactor();
            this.nodeLoadPredictFactor = config.getNodeLoadPredictFactor();
            this.nodeWeightFilterFactor = config.getNodeWeightFilterFactor();
            return getThis();
        }
    }

    public enum MethodOfFailureHandling {
        /**
         * if application can work without this pool
         * как только все ноды в пуле буду помечены circuit-breaker-ом как упавшие
         * пул на каждый запрос будет бросать TTransportException не обращаясь к серверам, пока работа нод не
         * нормализуется. Благодоря этом части приложения не связанные с пулом продолжат работать.
         */
        CIRCUIT_BREAK,
        /**
         * if pool critical to current application
         * Пул будет пытается продолжать работу пока на вызов isHealCheckOk будет отвечать число нод
         * заданных параметрами maxFailNodes, minAliveNodes, minNodesInRingRatio. При этом ноды отключенные по Circuit Breaker
         * будут возвращены в пул. Это приведет к тому что все потоки приложения могут быть заблокированны на операциях с пулом,
         * но позволит обработать часть запросов пользователей.
         */
        TRY_CONTINUE,
    }
}
