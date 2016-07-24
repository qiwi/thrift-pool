package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.balancer.Balancer;
import com.qiwi.thrift.balancer.WeightedBalancer;
import com.qiwi.thrift.metrics.MetricEnabledStatus;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Usage examples in com.qiwi.thrift.demo.BalancerSyncDemoTest
 * Spring integration examples ThriftConsulDemoTest
 * @param <I>
 * @param <C>
 */
public class ThriftLoadBalancer<I, C extends ThriftClient<I>> implements Closeable, Balancer<NodeStatus<I, C>> {
    private static final Logger log = LoggerFactory.getLogger(ThriftLoadBalancer.class);
    public static final Duration CONFIG_RELOAD_PERIOD = Duration.ofSeconds(30);

    private final Object updateStatusLock = new Object();
    private final Random random = new Random();

    private final String serviceName;
    private final ScheduledExecutorService reBalanceScheduler;
    private final ScheduledExecutorService reloadScheduler;
    private final NodesHolder<I, C> nodesHolder;

    private final MetricEnabledStatus metricStatus;

    private volatile ThriftBalancerConfig config;
    private volatile Balancer<ThriftDcBalancer<I, C>> balancer;
    private volatile Balancer<ThriftDcBalancer<I, C>> recoveryBalancer;

    private ScheduledFuture<?> reloadNodesFuture;
    private ScheduledFuture<?> verifyNodesFuture;
    private ScheduledFuture<?> checkNodesFuture;
    private ScheduledFuture<?> reBalanceFuture;

    protected ThriftLoadBalancer(
            String serviceName,
            ThriftBalancerConfig config,
            ScheduledExecutorService reBalanceScheduler,
            ScheduledExecutorService reloadScheduler,
            NodesHolder<I, C> nodesHolder
    ) {
        this.serviceName = serviceName;
        this.config = config;
        this.reBalanceScheduler = reBalanceScheduler;
        this.reloadScheduler = reloadScheduler;
        this.nodesHolder = nodesHolder;
        if (ThriftMonitoring.getMonitor().registerPool(
                serviceName,
                this::getLoad,
                this::getUsedConnections,
                this::getOpenConnections,
                this::getFailedNodesCount,
                () -> getFailedNodes()
                            .collect(Collectors.toList())
        )) {
            metricStatus = MetricEnabledStatus.ENABLED;
        } else {
            metricStatus = MetricEnabledStatus.DISABLED;
        }
        nodesHolder.init(config, metricStatus);


        reloadNodes(true);
        checkNodesRunning();
        reBalance();
        evict();

        onConfigUpdate();
    }

    public void reconfigure(ThriftBalancerConfig newConfig) {
        this.config = newConfig;
        nodesHolder.reconfigure(config);
        onConfigUpdate();

    }

    private void onConfigUpdate() {
        if (reloadNodesFuture != null) {
            reloadNodesFuture.cancel(false);
        }
        Duration nodesReloadPeriod = config.getRingNodesReloadPeriod();
        reloadNodesFuture = reloadScheduler.scheduleWithFixedDelay(
                () -> reloadNodes(false),
                nodesReloadPeriod.toMillis(),
                nodesReloadPeriod.toMillis(),
                TimeUnit.MILLISECONDS
        );
        if (checkNodesFuture != null) {
            checkNodesFuture.cancel(false);
        }
        Duration nodesCheckPeriod = config.getNodesHealthCheckPeriod();
        checkNodesFuture = reloadScheduler.scheduleWithFixedDelay(
                this::checkNodesRunning,
                nodesCheckPeriod.toMillis(),
                nodesCheckPeriod.toMillis(),
                TimeUnit.MILLISECONDS
        );
        if (reBalanceFuture != null) {
            reBalanceFuture.cancel(false);
        }
        Duration rebuildPeriod = config.getRingReBalancePeriod();
        reBalanceFuture = reBalanceScheduler.scheduleWithFixedDelay(
                this::reBalance,
                rebuildPeriod.toMillis(),
                rebuildPeriod.toMillis(),
                TimeUnit.MILLISECONDS
        );
        Duration connectionCheckPeriod = config.getConnectionCheckPeriod();
        reBalanceFuture = reloadScheduler.scheduleWithFixedDelay(
                this::evict,
                connectionCheckPeriod.toMillis(),
                connectionCheckPeriod.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }


    private void reloadNodes(boolean initialCreate) {
        nodesHolder.reloadNodes(initialCreate);
        recoveryBalancer = new WeightedBalancer<>(nodesHolder.getDcList(), dc -> 1);
    }


    private void checkNodesRunning() {
        try {
            nodesHolder.getDcList()
                    .forEach(ThriftDcBalancer::doHealthCheck);
        } catch (Exception ex) {
            log.error("Unable to check nodes. Service {}", serviceName, ex);
        }
    }

    @Override
    public void reBalance() {
        synchronized (updateStatusLock) {
            try {
                List<ThriftDcBalancer<I, C>> dcCopy = nodesHolder.getDcList();
                for (ThriftDcBalancer<I, C> dc : dcCopy) {
                    boolean preferred = dc.getDcName().equals(config.getPreferredDc());
                    dc.getLoadAccumulator().setPreferred(preferred);
                    dc.reBalance();
                }
                List<ThriftDcBalancer<I, C>> balancerList = dcCopy.stream()
                        .filter(ThriftDcBalancer::isWorking)
                        .collect(Collectors.toList());
                if (balancerList.isEmpty()) {
                    balancerList = dcCopy.stream()
                            .filter(ThriftDcBalancer::canProcessRequests)
                            .collect(Collectors.toList());
                }
                balancer = new LoadBasedBalancer<>(
                        balancerList,
                        ThriftDcBalancer::getLoadAccumulator,
                        ThriftDcBalancer::getLoad,
                        config.getDcLoadFilterFactor(),
                        config.getDcLoadPredictFactor(),
                        config.getDcWeightFilterFactor()
                );

            } catch (Exception ex) {
                log.error("Unable to update ring status. Service {}", serviceName, ex);
            }
        }
    }

    public void evict() {
        try {
            nodesHolder.getFullNodeList()
                    .forEach(node -> node.getClient().evict());
        } catch (Exception ex) {
            log.error("Error when eviction connections", ex);
        }
    }


    protected Balancer<ThriftDcBalancer<I, C>> getBalancer() {
        return balancer;
    }

    protected Balancer<ThriftDcBalancer<I, C>> getRecoveryBalancer() {
        return recoveryBalancer;
    }

    @Override
    public Optional<NodeStatus<I, C>> get() {
        Optional<NodeStatus<I, C>> optional = recoveryBalancer.get().flatMap(ThriftDcBalancer::getRecover);
        if (optional.isPresent()) {
            return optional;
        }
        return balancer.get().flatMap(ThriftDcBalancer::get);
    }


    public Optional<NodeStatus<I, C>> getWorking() {
        return balancer.get().flatMap(ThriftDcBalancer::get);
    }

    @Override
    public Stream<NodeStatus<I, C>> nodes() {
        return balancer.nodes()
                .flatMap(ThriftDcBalancer::nodes);
    }

    public Stream<ThriftDcBalancer<I, C>> dcList() {
        return balancer.nodes();
    }

    public double getLoad(){
        return nodesHolder.getDcList().stream()
                .mapToDouble(ThriftDcBalancer::getLoad)
                .sum();
    }

    public int getUsedConnections(){
        return nodesHolder.getDcList().stream()
                .mapToInt(ThriftDcBalancer::getUsedConnections)
                .sum();
    }

    public int getOpenConnections(){
        return nodesHolder.getDcList().stream()
                .mapToInt(ThriftDcBalancer::getOpenConnections)
                .sum();
    }

    public Stream<ThriftClientAddress> getFailedNodes() {
        return nodesHolder.getFullNodeList().stream()
                    .filter(node -> !node.isWorking())
                    .map(NodeStatus::getAddress);
    }

    public long getFailedNodesCount() {
        return getFailedNodes()
            .count();
    }

    @Override
    public void close() {
        try {
            reloadScheduler.shutdown();
            reBalanceScheduler.shutdown();
            reloadScheduler.awaitTermination(1, TimeUnit.SECONDS);
            reBalanceScheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Await terminated", e);
        }
        nodesHolder.close();
        if (metricStatus.isEnabled()) {
            ThriftMonitoring.getMonitor().unRegisterPool(serviceName);
        }
    }

    public String getServiceName() {
        return serviceName;
    }


    public void scheduleConfigReload(
            ThriftBalancerConfig.UntypedBuilder<?> builder,
            Consumer<ThriftBalancerConfig> toReconfigure
    ){
        reloadScheduler.scheduleWithFixedDelay(
                () -> reloadConfig(builder, toReconfigure),
                CONFIG_RELOAD_PERIOD.toMillis(),
                CONFIG_RELOAD_PERIOD.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    private void reloadConfig(
            ThriftBalancerConfig.UntypedBuilder<?> builder,
            Consumer<ThriftBalancerConfig> toReconfigure
    ) {
        try {
            builder.getParameterSource().refresh();
            ThriftBalancerConfig newConfig = builder.build();
            if (!newConfig.equals(config)) {
                toReconfigure.accept(newConfig);
            }
        } catch (Exception e) {
            log.error("Unable to update config. Service {}", serviceName, e);
        }
    }

    @Named
    @Singleton
    public static class Factory{

        public <I, C extends ThriftClient<I>> ThriftLoadBalancer<I, C> create(
                String serviceName,
                ThriftBalancerConfig config,
                Function<ThriftClientConfig, C> clientFactory
        ){
            ScheduledExecutorService reload = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "{\"balancer reload\":\"" + serviceName + "\"}")
            );
            ScheduledExecutorService reBalance = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "{\"balancer reBalance\":\"" + serviceName + "\"}")
            );
            NodesHolder<I, C> nodesHolder = new NodesHolder<>(clientFactory, serviceName);
            return newBalancer(serviceName, config, reBalance, reload, nodesHolder);
         }

        protected <I, C extends ThriftClient<I>> ThriftLoadBalancer<I, C> newBalancer(String serviceName,
                    ThriftBalancerConfig config,
                    ScheduledExecutorService reBalanceScheduler,
                    ScheduledExecutorService reloadScheduler,
                    NodesHolder<I, C> nodesHolder
        ){
            return new ThriftLoadBalancer<>(
                     serviceName,
                     config,
                     reBalanceScheduler,
                     reloadScheduler,
                     nodesHolder
            );
       }
    }

}
