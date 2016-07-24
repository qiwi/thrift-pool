package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class NodeStatus<I, C extends ThriftClient<I>> implements Closeable, ThriftRequestReporter {
    private static final Logger log = LoggerFactory.getLogger(NodeStatus.class);
    /**
     * Correct value will set by dc balancer at end of re-configuration
     */
    public static final int INIT_CONNECTIONS_COUNT = 1;
    /**
     * Период времени за который собирается статистика
     */
    public static final int PERIODS_PER_RE_BALANCE = 4;
    public static final int STATISTIC_MAX_DEEP_IN_PERIODS = PERIODS_PER_RE_BALANCE * 4;

    private volatile ThriftBalancerConfig config;
    private final ThriftClientAddress address;
    private final C client;
    private final Clock clock;
    final StatsSlidingWindow stats;

    private volatile Instant closeTime = Instant.MIN;
    /**
     * Результат тестирования healCheck ноды.
     */
    private volatile boolean connected;
    /**
     * Отмечается - если нода пропадала из видимости
     */
    private volatile boolean closed = false;

    /**
     * Перед тем как добавить ноду в кольцо, на нее отправляется несколько тестовый запросов.
     * В первом цикле 1
     * Во втором 8
     */
    private final AtomicLong remainingTestRequest = new AtomicLong(0);
    private final AtomicLong finishedTestRequest = new AtomicLong(0);

    // ----- эти поля изменяются только потоком updateRingStatus, по этому не требуют синхронизации -----
    private Instant returnTime = Instant.MIN;
    private Instant maxTestTime = Instant.MIN;
    /**
     * Если тестирование показывает что нода рабочая, но при подаче нагрузки,
     * она сразу падает, то период возвращение в кольцо растет по экспоненте
     * Это позволяет вывести сбоящую ноду из кольца
     */
    private long disableTimeoutMillis = 0;
    /**
     * Статус конечного автомата, который управляет нодой
     */
    private volatile NodeStateMachineStatus status;
    /**
     * Если circuit breaker показывает что все ноды в пуле лежат, а пул критический для работы приложения
     * То мы подключаем все ноды, в надежде что хотя бы часть запросов будет обработана
     */
    private boolean recoveryMode = false;
    private final LoadBasedBalancer.LoadAccumulator<NodeStatus<I, C>> accumulator = new LoadBasedBalancer.LoadAccumulator<>(this);

    NodeStatus(
            ThriftBalancerConfig config,
            ThriftClientAddress address,
            Function<ThriftClientConfig, C> clientFactory,
            boolean initialCreate
    ) {
        this(config, address, clientFactory, initialCreate, Clock.systemUTC());
    }

    NodeStatus(
            ThriftBalancerConfig config,
            ThriftClientAddress address,
            Function<ThriftClientConfig, C> clientFactory,
            boolean initialCreate,
            Clock clock
    ) {
        this.config = config;
        this.address = address;
        this.client = clientFactory.apply(config.createClientConfig(
                address,
                INIT_CONNECTIONS_COUNT,
                this
        ));
        this.clock = clock;

        if (initialCreate) {
             connected = true;
             setStatus(NodeStateMachineStatus.WORKING);
        } else {
             connected = false;
             setStatus(NodeStateMachineStatus.DISCONNECTED);
        }
        stats = new StatsSlidingWindow(
                config.getRingReBalancePeriod().dividedBy(PERIODS_PER_RE_BALANCE),
                STATISTIC_MAX_DEEP_IN_PERIODS,
                clock
        );
    }

    @Override
    public void requestBegin(String serviceName, String methodName, ThriftCallType callType) {
        stats.getCurrent().requestBegin();
        // атомарная операция дорогая, оптимизируем.
        if (status.isTesting()) {
            remainingTestRequest.decrementAndGet();
        }
        config.getRequestReporter().requestBegin(serviceName, methodName, callType);
    }

    @Override
    public void requestEnd(
            String serviceName,
            String methodName,
            ThriftCallType callType,
            ThriftRequestStatus requestStatus,
            long latencyNanos,
            Optional<Throwable> exception
    ) {
        stats.getCurrent().requestEnd(requestStatus, latencyNanos);
        if (status.isTesting()) {
            finishedTestRequest.incrementAndGet();
        }
        config.getRequestReporter().requestEnd(
                serviceName,
                methodName,
                callType,
                requestStatus,
                latencyNanos,
                exception
        );
    }

    public ThriftClientAddress getAddress() {
        return address;
    }

    public double getLoadForTests(){
        return (client.getUsedConnections() + client.getNumWaiters()) / (double)client.getMaxConnections();
    }

    public double getLoad(){
        // Первый период пересекается со значения из предыдущей балансировки
        StatsSlidingWindow.Bucket stat = stats.getStat(PERIODS_PER_RE_BALANCE - 1);

        int maxConnections = client.getMaxConnections();
        int numWaiters = client.getNumWaiters();
        double loadByStat = stat.getLoad(maxConnections, numWaiters);
        return loadByStat;
    }


    public int getUsedConnections() {
        return getClient().getUsedConnections();
    }

    public int getOpenConnections() {
        return getClient().getOpenConnections();
    }

    public void setMaxConnections(int maxConnections) {
        client.setMaxConnections(maxConnections);
    }

    public C getClient() {
        return client;
    }

    public boolean isInRing(){
        return status.isWorking() || (recoveryMode && status.isConnected());
    }

    public boolean isConnected() {
        return status.isConnected();
    }

    public boolean isWorking(){
        return status.isWorking();
    }

    public boolean shouldSendTestRequest(){
        if (status.isTesting()) {
            return remainingTestRequest.get() > 0;
        } else {
            return false;
        }
    }

    public void setRecoveryMode(boolean recoveryMode) {
        this.recoveryMode = recoveryMode;
    }

    private void disableNodeOnCircuitBreaker(){
        if (status == NodeStateMachineStatus.AWAIT) {
            return;
        }
        if (!status.isFail()) {
            disableNode();
        }
        setStatus(NodeStateMachineStatus.AWAIT);
    }

    private void disableNodeOnDisconnect() {
        if (status == NodeStateMachineStatus.DISCONNECTED) {
            return;
        }
        if (!status.isFail()) {
            disableNode();
        }
        setStatus(NodeStateMachineStatus.DISCONNECTED);
    }

    @SuppressWarnings("ImplicitNumericConversion")
    private void disableNode() {
        long minDisableMillis = config.getNodeDisableTime().toMillis();
        if (disableTimeoutMillis <= minDisableMillis) {
            disableTimeoutMillis = minDisableMillis;
        } else if (disableTimeoutMillis > ThriftBalancerConfig.MAX_NODE_DISABLE_TIME.toMillis()) {
            disableTimeoutMillis = ThriftBalancerConfig.MAX_NODE_DISABLE_TIME.toMillis();
        }
        returnTime = clock.instant().plusMillis(disableTimeoutMillis);
        if (status == NodeStateMachineStatus.WORKING) {
            disableTimeoutMillis *= ThriftBalancerConfig.TIMEOUT_GROW_FACTOR_HARD_FAIL;
        } else {
            disableTimeoutMillis *= ThriftBalancerConfig.TIMEOUT_GROW_FACTOR_SOFT_FAIL;
        }
    }

    // ----====Методы машины состояний====----
    private boolean hasTestRequests(){
        return finishedTestRequest.get() < status.testRequestsCount;
    }

    private boolean isFail(double avgDcLoad) {
        StatsSlidingWindow.Bucket stat = stats.getStat();
        long requestCountGap = status.requestCountGap;
        if (stat.getErrorRatio(requestCountGap) > config.getCircuitBreakerErrorRatio()) {
            log.warn(
                    "{} Node circuit beaked by error ratio. Ratio: {}, max allowed: {}",
                    client,
                    stat.getErrorRatio(requestCountGap),
                    config.getCircuitBreakerErrorRatio()
            );
            return true;
        }
        if (stat.getDisconnectRatio(requestCountGap) > config.getThresholdConnectionErrorRatio()) {
            log.warn(
                    "{} Node circuit beaked by connection error ratio. Ratio: {}, max allowed: {}",
                    client,
                    stat.getDisconnectRatio(requestCountGap),
                    config.getThresholdConnectionErrorRatio()
            );
            return true;
        }
        if (accumulator.getLoad() > avgDcLoad + config.getMaxNodeLoadGap()) {
            log.warn(
                    "{} Node circuit beaked because it has too high load inside dc. Load: {}, avgDcLoad: {}, maxLoadGap: {}",
                    client,
                    accumulator.getLoad(),
                    avgDcLoad,
                    config.getMaxNodeLoadGap()
            );
            return true;
        }
        if (accumulator.getLoad() > ThriftBalancerConfig.MAX_NODE_LOAD) {
            log.warn(
                    "{} Node circuit beaked  because it has too high load. Load: {}, avgDcLoad: {}, max allowed: {}",
                    client,
                    accumulator.getLoad(),
                    avgDcLoad,
                    ThriftBalancerConfig.MAX_NODE_LOAD
            );
            return true;
        }
        return false;
    }

    private boolean isReturnTime(){
        return returnTime.isBefore(clock.instant());
    }

    private void decrementDisableTimeout(){
        disableTimeoutMillis -= config.getRingReBalancePeriod().toMillis();
    }

    private void setStatus(NodeStateMachineStatus nextStage) {
        finishedTestRequest.set(0);
        remainingTestRequest.set(nextStage.testRequestsCount);
        status = nextStage;
    }

    /**
     *
     * @param avgDcLoad
     * @return возвращает истину если изменение в статусе требует переконфигурацию кольца
     */
    public boolean stateMachine(double avgDcLoad) {
        if (!connected || closed) {
            closed = false;
            boolean inRing = isInRing();
            disableNodeOnDisconnect();
            return inRing;
        }

        switch (status) {
            case WORKING:
                if (isFail(avgDcLoad)) {
                    disableNodeOnCircuitBreaker();
                    return true;
                } else {
                    decrementDisableTimeout();
                    return false;
                }
            case HALF_TESTED:
            case TEST_BEGIN:
                if (!hasTestRequests() || maxTestTime.isBefore(Instant.now())) {
                    if (isFail(avgDcLoad)) {
                        log.warn("{} Node fail test sequence on stage {}", client, status);
                        disableNodeOnCircuitBreaker();
                    } else {
                        log.debug("{} Node finish test on stage {}", client, status);
                        maxTestTime = clock.instant().plus(ThriftBalancerConfig.MAX_NODE_TEST_TIME);
                        setStatus(status.getNextStage());
                        return !recoveryMode && status.isWorking();
                    }
                }
                return false;
            case AWAIT:
                if (isReturnTime()) {
                    log.debug("{} Node begin test sequence", client);
                    stats.reset();
                    accumulator.reset();
                    maxTestTime = clock.instant().plus(ThriftBalancerConfig.MAX_NODE_TEST_TIME);
                    setStatus(NodeStateMachineStatus.TEST_BEGIN);
                }
                return false;
            case DISCONNECTED:
                if (connected) {
                    log.debug("{} Node reconnected", client);
                    setStatus(status.getNextStage());
                    return recoveryMode;
                } else {
                    return false;
                }
            default:
                throw new IllegalStateException("Status " + status + " not implemented");
        }
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        @SuppressWarnings("unchecked")
        NodeStatus<?, ?> that = (NodeStatus<?, ?>) obj;

        return address.equals(that.address);

    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    public boolean isNeedClose() {
        return closeTime.isBefore(clock.instant());
    }

    /**
     * Устанавилвает время через которое нода должна быть закрыта, после того как пропадет из консула
     */
    public void setCloseTime() {
        closeTime = clock.instant().plus(config.getNodeCloseTime());
        closed = true;
    }

    @Override
    public void close() {
        closed = true;
        client.close();
    }

    public void doHealthCheck() {
        boolean result = false;
        try {
            result = client.isHealCheckOk();
        } catch (Exception ex1) {
            log.debug("{} Node respond with error", client, ex1);
            try {
                result = client.isHealCheckOk();
            } catch (Exception ex2) {
                log.warn("{} Node respond with error second time", client, ex2);
            }
        } finally {
            connected = result;
        }
    }

    @Override
    public String toString() {
        return "Node{" +
                "address=" + address +
                ", status=" + status +
                ", returnTime=" + returnTime +
                ", maxTestTime=" + maxTestTime +
                ", recoveryMode=" + recoveryMode +
                '}';
    }

    public LoadBasedBalancer.LoadAccumulator<NodeStatus<I, C>> getLoadAccumulator() {
        return accumulator;
    }

    public String getStatus() {
        return status.toString();
    }

    public int getStatusId() {
        return status.ordinal();
    }

    public double getAvgLatencyMs() {
        return stats.getStat().getRequestLatencyNanos(1) / TimeUnit.MICROSECONDS.toNanos(1);
    }

    public void reconfigure(ThriftBalancerConfig config) {
        this.config = config;
        // correct maxConnections set by ThriftDcBalancer in next node refresh cycle
        client.reconfigure(config.createClientConfig(address, client.getMaxConnections(), this));
    }

    private enum NodeStateMachineStatus {
        WORKING(0, 8),
        HALF_TESTED(8, 8),
        TEST_BEGIN(1, 1),
        AWAIT(0, 1),
        DISCONNECTED(0, 1),
        ;

        public final long testRequestsCount;
        public final long requestCountGap;

        NodeStateMachineStatus(long testRequestsCount, long requestCountGap) {
            this.testRequestsCount = testRequestsCount;
            this.requestCountGap = requestCountGap;
        }

        public NodeStateMachineStatus getNextStage() {
            switch (this){
                case WORKING:
                    return WORKING;
                case HALF_TESTED:
                    return WORKING;
                case TEST_BEGIN:
                    return HALF_TESTED;
                case AWAIT:
                    return TEST_BEGIN;
                case DISCONNECTED:
                    return AWAIT;
                default:
                    throw new IllegalStateException("State " + this + " not fully implemented");
            }
        }

        public final boolean isFail(){
            return this == AWAIT || this == DISCONNECTED;
        }

        public final boolean isConnected(){
            return this != DISCONNECTED;
        }

        public final boolean isWorking(){
            return this == WORKING;
        }

        public final boolean isTesting(){
            return this == TEST_BEGIN || this == HALF_TESTED;
        }
    }
}
