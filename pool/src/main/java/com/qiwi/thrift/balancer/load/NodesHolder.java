package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.metrics.MetricEnabledStatus;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Загрузка и закрытие нод
 * @param <I>
 * @param <C>
 */
public class NodesHolder<I, C extends ThriftClient<I>> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NodesHolder.class);
    /**
     * Correct value will set by dc balancer at end of re-configuration
     */
    public static final int INIT_CONNECTIONS_COUNT = 1;

    private final Object loadNodesLock = new Object();

    private final Function<ThriftClientConfig, C> clientFactory;
    private final String serviceName;

    private final Map<String, ThriftDcBalancer<I, C>> dataCenters = new HashMap<>(4);
    private final Map<ThriftClientAddress, NodeStatus<I, C>> nodes = new HashMap<>(32);
    /**
     * Нужно закрывать с задержкой, ноды которые были остановлены, так как
     *  1) на них может выполнятся длительный запрос.
     *  2) нода также нода может "мигать", что будет приводит к сбросу статуса ноды в circuit breaker
     *  3) Нам нужно знать сколько нод было за последние n минут, чтобы если используется
     *     балансировке по ключу, при падении нод и переходе части нагрузки на новую ноду
     *     мы не начали посылать запросы на ноды которые еще не успели погрузить кэш.
     *
     */
    private final Map<ThriftClientAddress, NodeStatus<I, C>> toClose = new HashMap<>(16);

    private volatile ThriftBalancerConfig config;
    private volatile MetricEnabledStatus metricStatus;

    private volatile List<ThriftDcBalancer<I, C>> activeDc = new ArrayList<>();
    private volatile List<NodeStatus<I, C>> fullNodeList = new ArrayList<>();
    private volatile boolean hasChangesInRing;

    protected NodesHolder(
            Function<ThriftClientConfig, C> clientFactory,
            String serviceName
    ) {
        this.clientFactory = Objects.requireNonNull(clientFactory);
        this.serviceName = serviceName;
    }

    void init(ThriftBalancerConfig config, MetricEnabledStatus metricStatus){
        this.metricStatus = metricStatus;
        this.config = config;
    }

    public void reloadNodes(boolean initialCreate) {
        synchronized (loadNodesLock) {
            try {
                if (loadRing(initialCreate)) {
                    hasChangesInRing = true;
                }
                if (closeOldNodes()) {
                    hasChangesInRing = true;
                }
                if (hasChangesInRing) {
                    hasChangesInRing = false;
                    refreshDc();
                }
            } catch (Exception ex) {
                log.error("Service {}. Unable to reload nodes.", serviceName, ex);
            }
        }
    }


    @SuppressWarnings("resource")
    private boolean loadRing(boolean initialCreate){
        List<ThriftClientAddress> addresses = new ArrayList<>(config.getServersSupplier().get());
        Collections.sort(addresses);
        Map<ThriftClientAddress, NodeStatus<I, C>> nodesCopy = new HashMap<>(nodes);
        boolean hasChangesInRing = false;
        for (ThriftClientAddress address : addresses) {
            NodeStatus<I, C> node = nodesCopy.remove(address);
            if (node == null) {
                node = toClose.remove(address);
                if (node == null) {
                    log.debug("Service {}. Opening connection to node {}", serviceName, address);
                    node = initNodeStatus(initialCreate, address);
                } else {
                    log.info("Service {}. Reusing not completely closed connection to node {}", serviceName, node);
                }
                nodes.put(address, node);
                hasChangesInRing = true;
            }
        }
        for (NodeStatus<I, C> status : nodesCopy.values()) {
            status.setCloseTime();
            nodes.remove(status.getAddress());
            toClose.put(status.getAddress(), status);

            hasChangesInRing = true;
        }
        return hasChangesInRing;
    }

    protected NodeStatus<I, C> initNodeStatus(boolean initialCreate, ThriftClientAddress address) {
        NodeStatus<I, C> node = createNodeStatus(initialCreate, address);
        if (metricStatus.isEnabled()) {
            ThriftMonitoring.getMonitor().registerNode(
                    serviceName,
                    address,
                    node::getLoad,
                    () -> node.getLoadAccumulator().getWeight(),
                    node::getUsedConnections,
                    node::getOpenConnections,
                    node::getStatus,
                    node::getStatusId,
                    node::getAvgLatencyMs
            );
        }

        return node;
    }

    protected NodeStatus<I, C> createNodeStatus(boolean initialCreate, ThriftClientAddress address) {
        return new NodeStatus<>(
                config,
                address,
                clientFactory,
                initialCreate
        );
    }

    private boolean closeOldNodes(){
        boolean hasChangesInRing = false;
        for (Iterator<NodeStatus<I, C>> iterator = toClose.values().iterator(); iterator.hasNext(); ) {
            NodeStatus<I, C> closing = iterator.next();
            if (closing.isNeedClose()) {
                log.debug("Service {}. Close connection to node {}", closing, serviceName);
                try {
                    iterator.remove();
                    hasChangesInRing = true;
                    closing.close();
                    ThriftMonitoring.getMonitor().unRegisterNode(serviceName, closing.getAddress());
                } catch (Exception ex) {
                    log.warn("Unable to close node {}", closing.getAddress(), ex);
                }
            }
        }
        return hasChangesInRing;
    }

    private void refreshDc() {
        Map<String, List<NodeStatus<I, C>>> ring = nodes.values().stream()
                .collect(Collectors.groupingBy(node -> node.getAddress().getDc()));

        for (Map.Entry<String, List<NodeStatus<I, C>>> entry : ring.entrySet()) {
            ThriftDcBalancer<I, C> dc = dataCenters.computeIfAbsent(
                    entry.getKey(),
                    this::initDcBalancer
            );
            int closingNodes = (int) toClose.keySet().stream()
                    .filter(address -> entry.getKey().equals(address.getDc()))
                    .count();;
            dc.setDcNodes(entry.getValue(), closingNodes + entry.getValue().size());
        }
        List<ThriftDcBalancer<I, C>> list = new ArrayList<>(dataCenters.values());
        Collections.sort(list, Comparator.comparing(ThriftDcBalancer::getDcName));
        activeDc = list;

        List<NodeStatus<I, C>> newNodesList = new ArrayList<>(nodes.size() + toClose.size());
        newNodesList.addAll(nodes.values());
        newNodesList.addAll(toClose.values());
        fullNodeList = newNodesList;
    }

    protected ThriftDcBalancer<I, C> initDcBalancer(String name) {
        ThriftDcBalancer<I, C> balancer = createDcBalancer(name);
        if (metricStatus.isEnabled()) {
            ThriftMonitoring.getMonitor().registerDc(
                    serviceName,
                    name,
                    balancer::getLoad,
                    () -> balancer.getLoadAccumulator().getWeight(),
                    balancer::getUsedConnections,
                    balancer::getOpenConnections,
                    balancer::getFailNodesCount,
                    balancer::getStatus
            );
        }
        return balancer;
    }


    protected ThriftDcBalancer<I, C> createDcBalancer(String name) {
        return new ThriftDcBalancer<>(serviceName, name, config);
    }

    @Override
    public void close() {
        synchronized (loadNodesLock) {
            for (ThriftDcBalancer<I, C> dc : dataCenters.values()) {
                if (metricStatus.isEnabled()) {
                    ThriftMonitoring.getMonitor().unRegisterDc(serviceName, dc.getDcName());
                }
                dc.close();
            }
            Stream.concat(
                    toClose.values().stream(),
                    nodes.values().stream()
            ).forEach(status -> {
                if (metricStatus.isEnabled()) {
                    ThriftMonitoring.getMonitor().unRegisterNode(serviceName, status.getAddress());
                }
                status.close();
            });
        }
    }

    public List<ThriftDcBalancer<I, C>> getDcList() {
        return activeDc;
    }

    public List<NodeStatus<I, C>> getFullNodeList() {
        return fullNodeList;
    }

    public void reconfigure(ThriftBalancerConfig config) {
        this.config = config;
        fullNodeList.forEach(node -> node.reconfigure(config));
        activeDc.forEach(dc -> dc.reconfigure(config));
        hasChangesInRing = true;
    }

    protected String getServiceName() {
        return serviceName;
    }

    protected ThriftBalancerConfig getConfig() {
        return config;
    }
}
