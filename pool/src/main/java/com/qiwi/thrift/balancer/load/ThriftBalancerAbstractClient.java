package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;

public class ThriftBalancerAbstractClient<I, C extends ThriftClient<I>> implements ThriftClient<C> {
    protected final ThriftLoadBalancer<I, C> balancer;

    private volatile ThriftBalancerConfig config;
    private volatile long maxWaitMillis;

    public ThriftBalancerAbstractClient(
            ThriftLoadBalancer<I, C> balancer,
            ThriftBalancerConfig config
    ) {
        this.balancer = balancer;
        reconfigureAsBalancer(config);
    }

    @Override
    public void reconfigure(ThriftClientConfig clientConfig) {
        ThriftBalancerConfig newConfig = new ThriftBalancerConfig.Builder(config.getFailureHandling())
                .fromBalancerConfig(config)
                .fromAbstractConfig(clientConfig)
                .build();
        reconfigureAsBalancer(newConfig);
    }

    @Override
    public void evict() {
        balancer.evict();
    }

    public void reconfigureAsBalancer(ThriftBalancerConfig newConfig){
        balancer.reconfigure(newConfig);
        this.config = newConfig;
        maxWaitMillis = config.getMaxWaitForConnection().toMillis();
    }

    public ThriftLoadBalancer<I, C> getBalancer() {
        return balancer;
    }

    protected long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    @Override
    public int getUsedConnections() {
        return balancer.getUsedConnections();
    }

    @Override
    public int getOpenConnections() {
        return balancer.getOpenConnections();
    }

    @Override
    public int getNumWaiters() {
        return balancer.nodes().mapToInt(node -> node.getClient().getNumWaiters()).sum();
    }

    @Override
    public boolean isHealCheckOk() {
        return balancer.getWorking().isPresent();
    }

    @Override
    public int getMaxConnections() {
        return config.getMaxConnections();
    }

    @Override
    public void setMaxConnections(int maxConnections) {
        ThriftBalancerConfig newConfig = new ThriftBalancerConfig.Builder(config.getFailureHandling())
                .fromBalancerConfig(config)
                .setMaxConnections(maxConnections)
                .build();
        reconfigureAsBalancer(newConfig);
    }

    public ThriftBalancerConfig getConfig() {
        return config;
    }

    @Override
    public void close(){
        balancer.close();
    }
}
