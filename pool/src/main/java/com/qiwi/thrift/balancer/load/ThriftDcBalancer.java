package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.balancer.Balancer;
import com.qiwi.thrift.balancer.WeightedBalancer;
import com.qiwi.thrift.pool.ThriftClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThriftDcBalancer<I, C extends ThriftClient<I>> implements Balancer<NodeStatus<I, C>>, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ThriftDcBalancer.class);
    private final String serviceName;
    private final String dcName;

    private volatile ThriftBalancerConfig config;
    /**
     * Общее число нод которое сейчас в кольце, включая те которые остановлены
     * config.getNodeCloseTime()
     */
    private volatile int nodesInRing = 0;
    private volatile List<NodeStatus<I, C>> allDcNodes = new ArrayList<>();

    private volatile Balancer<NodeStatus<I, C>> balancer;
    private volatile Balancer<NodeStatus<I, C>> balancerRecovery;
    private volatile boolean hasChangesInRing = false;

    // ----- эти поля изменяются только потоком updateRingStatus, по этому не требуют синхронизации -----
    private volatile DcStatus dcStatus = DcStatus.WORKING;
    private final LoadBasedBalancer.LoadAccumulator<ThriftDcBalancer<I, C>> accumulator = new LoadBasedBalancer.LoadAccumulator<>(this);

    protected ThriftDcBalancer(String serviceName, String dcName, ThriftBalancerConfig config) {
        this.serviceName = serviceName;
        this.dcName = dcName;

        this.config = config;
        balancer = createBalancer(Collections.emptyList(), BalancerType.WORKING);
        balancerRecovery = createBalancer(Collections.emptyList(), BalancerType.RECOVERY);
    }

    public void setDcNodes(List<NodeStatus<I, C>> ring, int totalNodeCount){
        hasChangesInRing = true;
        this.nodesInRing = totalNodeCount;
        @SuppressWarnings("unchecked")
        List<NodeStatus<I, C>> ringCopy = new ArrayList<>(ring);
        Collections.sort(ringCopy, Comparator.comparing(NodeStatus::getAddress));

        int maxConnections = (int)Math.ceil(config.getMaxConnections() / (double)nodesInRing);
        ringCopy.forEach(node -> node.setMaxConnections(maxConnections));

        allDcNodes = ringCopy;
        balancerRecovery = createBalancer(ringCopy, BalancerType.RECOVERY);
    }

    public void doHealthCheck() {
        for (NodeStatus<I, C> status : allDcNodes) {
            status.doHealthCheck();
        }
    }

    private boolean isNodesEnoughToRunPool(int count){
        if (count < (int)(nodesInRing * config.getMinNodesInDcRatio())) {
            return false;
        }
        if (config.getMaxFailNodes() >= 0 && nodesInRing - count > config.getMaxFailNodes()) {
            return false;
        }
        if (count < config.getMinAliveNodes() || count == 0) {
            return false;
        }
        return true;
    }

    private DcStatus updateNodesStatuses() {
        List<NodeStatus<I, C>> fullRingCopy = allDcNodes;

        double dcLoad = fullRingCopy.stream()
                .mapToDouble(node -> node.getLoadAccumulator().getLoad())
                .average()
                .orElse(0);
        for (NodeStatus<I, C> node : fullRingCopy) {
            if (node.stateMachine(dcLoad)){
                hasChangesInRing = true;
            }
        }

        List<NodeStatus<I, C>> workingNodes = fullRingCopy.stream()
                .filter(NodeStatus::isWorking)
                .collect(Collectors.toList());

        DcStatus newDcStatus;
        if (isNodesEnoughToRunPool(workingNodes.size())) {
            newDcStatus = DcStatus.WORKING;
            if (dcStatus != DcStatus.WORKING) {
                log.debug("Dc {}/{} enabled. Available nodes: {}", serviceName, dcName, workingNodes.size());
                fullRingCopy.forEach(node -> node.setRecoveryMode(false));
            }
            if (hasChangesInRing) {
                hasChangesInRing = false;
                balancer = createBalancer(workingNodes, BalancerType.WORKING);
            } else {
                balancer.reBalance();
            }
        } else {
            if (config.getFailureHandling() == ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK){
                if (canProcessRequests()) {
                    log.error(
                            "Dc {}/{} failure because too many disabled by circuit breaker. Available nodes: {}. Non-critical pool. Disabling.",
                            serviceName,
                            dcName,
                            workingNodes.size()
                    );
                }
                accumulator.reset();
                newDcStatus = DcStatus.FAIL;
            } else {
                fullRingCopy.forEach(node -> node.setRecoveryMode(true));

                List<NodeStatus<I, C>> connectedNodes = fullRingCopy.stream()
                        .filter(NodeStatus::isConnected)
                        .collect(Collectors.toList());
                if (isNodesEnoughToRunPool(connectedNodes.size())) {
                    if (dcStatus == DcStatus.WORKING) {
                        log.error(
                                "{}/{} All nodes in dc disabled by circuit breaker. Available nodes: {}, connected nodes {}. Critical pool. Try to survive.",
                                serviceName,
                                dcName,
                                workingNodes.size(),
                                connectedNodes.size()
                        );
                    }
                    newDcStatus = DcStatus.TRY_RECOVER;
                    if (hasChangesInRing) {
                        hasChangesInRing = false;
                        balancer = createBalancer(workingNodes, BalancerType.WORKING);
                    } else {
                        balancer.reBalance();
                    }
                } else {
                    if (canProcessRequests()) {
                        log.error(
                                "Dc {}/{} failure because too many nodes disconnected. Available nodes: {}, connected nodes {}. Critical pool fail completely!",
                                serviceName,
                                dcName,
                                workingNodes.size(),
                                connectedNodes.size()
                        );
                    }
                    newDcStatus = DcStatus.FAIL;
                    accumulator.reset();
                }
            }
        }
        return newDcStatus;
    }

    @Override
     public void reBalance() {
         DcStatus newDcStatus = updateNodesStatuses();
         if (dcStatus != newDcStatus) {
              dcStatus = newDcStatus;
              hasChangesInRing = true;
         }
    }

    protected Balancer<NodeStatus<I, C>> createBalancer(List<NodeStatus<I, C>> items, BalancerType type){
        log.trace("Dc {}/{} create node balancer {}", serviceName, dcName, type);
        switch (type) {
            case RECOVERY:
                return new WeightedBalancer<>(
                        items,
                        node -> 1
                );
            case WORKING:
                return new LoadBasedBalancer<>(
                        items,
                        NodeStatus::getLoadAccumulator,
                        NodeStatus::getLoad,
                        config.getNodeLoadFilterFactor(),
                        config.getNodeLoadPredictFactor(),
                        config.getNodeWeightFilterFactor()
                );
            default:
                throw new IllegalArgumentException("Unsupported balancer type " + type);
        }
    }

    public boolean isWorking() {
        return dcStatus.isWorking();
    }

    public boolean canProcessRequests() {
        return dcStatus.canProcessRequests();
    }

    protected Balancer<NodeStatus<I, C>> getBalancer() {
        return balancer;
    }

    protected Balancer<NodeStatus<I, C>> getBalancerRecovery() {
        return balancerRecovery;
    }

    /**
     * @return Возвращает ноду. Гарантируется что будет возвращено не empty значение.
     */
    @Override
    public Optional<NodeStatus<I, C>> get() {
        return balancer.get();
    }

    public Optional<NodeStatus<I, C>> getRecover() {
        // Чтобы можно было проверить работоспособность ноды, на нее нужно отправить несколько запросов,
        // не смотря на то что она имеет статус упавшей, или находится в упавшем дц.
        Optional<NodeStatus<I, C>> nodeOpt = balancerRecovery.get();
        return nodeOpt.filter(NodeStatus::shouldSendTestRequest);
    }

    @Override
    public Stream<NodeStatus<I, C>> nodes() {
        return balancer.nodes();
    }

    public String getDcName() {
        return dcName;
    }

    public LoadBasedBalancer.LoadAccumulator<ThriftDcBalancer<I, C>> getLoadAccumulator() {
        return accumulator;
    }

    public double getLoad() {
        double load = nodes()
                .mapToDouble(NodeStatus::getLoad)
                .average()
                .orElse(0.0);
        return load;
    }

    public int getUsedConnections() {
        return allDcNodes.stream()
                .mapToInt(NodeStatus::getUsedConnections)
                .sum();
    }

    public int getOpenConnections() {
        return allDcNodes.stream()
                .mapToInt(NodeStatus::getOpenConnections)
                .sum();
    }

    @Override
    public void close() {
        // Nodes closed by NodesHolder
    }

    public long getFailNodesCount() {
        return nodesInRing - allDcNodes.stream()
                .filter(NodeStatus::isWorking)
                .count();
    }

    public String getStatus() {
        return dcStatus.toString();
    }

    protected ThriftBalancerConfig getConfig() {
        return config;
    }

    public void reconfigure(ThriftBalancerConfig config) {
        this.config = config;
        hasChangesInRing = true;
    }


    private enum DcStatus {
       WORKING,
       TRY_RECOVER,
       FAIL,

       ;


        public boolean isWorking() {
            return this == WORKING;
        }

        public boolean canProcessRequests() {
            return this != FAIL;
        }
    }

    public enum BalancerType {
        RECOVERY,
        WORKING,
    }
}
