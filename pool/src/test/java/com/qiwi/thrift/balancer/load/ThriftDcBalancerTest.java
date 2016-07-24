package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.balancer.Balancer;
import com.qiwi.thrift.balancer.WeightedBalancer;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class ThriftDcBalancerTest {
    private ThriftDcBalancer<String, ThriftClient<String>> balancer;
    private ThriftBalancerConfig config;
    private NodeStatus<String, ThriftClient<String>> node1;
    private NodeStatus<String, ThriftClient<String>> node2;
    private Random random;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        random = new Random(237850121L);

        ThriftClientAddress address = ThriftClientAddress.parse("test:123");
        ThriftBalancerConfig configReal = new ThriftBalancerConfig.Builder(ThriftBalancerConfig.MethodOfFailureHandling.TRY_CONTINUE)
                .setServers(Arrays.asList(address))
                .build();
        config = spy(configReal);
        balancer = new ThriftDcBalancer<String, ThriftClient<String>>("TstSrv", "dc1", config) {
            @Override
            protected Balancer<NodeStatus<String, ThriftClient<String>>> createBalancer(List<NodeStatus<String, ThriftClient<String>>> items, BalancerType type) {
                return new WeightedBalancer<>(items, node -> 1, () -> random.nextDouble());
            }
        };
        node1 = mock(NodeStatus.class);
        when(node1.isWorking()).thenReturn(true);
        when(node1.isConnected()).thenReturn(true);
        when(node1.getAddress()).thenReturn(address);
        when(node1.stateMachine(anyDouble())).thenReturn(true);
        when(node1.shouldSendTestRequest()).thenReturn(true);
        LoadBasedBalancer.LoadAccumulator<NodeStatus<String, ThriftClient<String>>> acc1 = mock(LoadBasedBalancer.LoadAccumulator.class);
        when(acc1.getLoad()).thenReturn(0.1);
        when(node1.getLoadAccumulator()).thenReturn(acc1);
        node2 = mock(NodeStatus.class);
        when(node2.isWorking()).thenReturn(true);
        when(node2.isConnected()).thenReturn(true);
        when(node2.getAddress()).thenReturn(address);
        when(node2.shouldSendTestRequest()).thenReturn(true);
        LoadBasedBalancer.LoadAccumulator<NodeStatus<String, ThriftClient<String>>> acc2 = mock(LoadBasedBalancer.LoadAccumulator.class);
        when(acc2.getLoad()).thenReturn(0.2);
        when(node2.getLoadAccumulator()).thenReturn(acc2);


        balancer.setDcNodes(Arrays.asList(node1, node2), 3);
        balancer.reBalance();
    }

    @Test(groups = "unit")
    public void setDcNodes() throws Exception {
        assertTrue(balancer.getRecover().isPresent());
        assertTrue(balancer.get().isPresent());
        balancer.setDcNodes(Collections.emptyList(), 5);
        balancer.reBalance();
        assertFalse(balancer.getRecover().isPresent());
        assertTrue(balancer.get().isPresent());

        verify(node2).setMaxConnections(22);
     }

    @Test(groups = "unit")
    public void doHealCheck() throws Exception {
        balancer.doHealthCheck();

        verify(node2).doHealthCheck();
    }

    @Test(groups = "unit")
    public void reBalanceOnFail() throws Exception {
        assertTrue(balancer.isWorking());
        when(node1.isConnected()).thenReturn(false);
        when(node1.isWorking()).thenReturn(false);
        when(node2.isConnected()).thenReturn(false);
        when(node2.isWorking()).thenReturn(false);
        balancer.reBalance();

        assertFalse(balancer.isWorking());
        assertFalse(balancer.canProcessRequests());
        // При отключении балансера - в нем должны сохрнятся ноды - чтоыб небыло проблем с тем,
        // что будет возращен пустой балансер без нод
        assertTrue(balancer.get().isPresent());
    }

    @Test(groups = "unit")
    public void reBalanceOnRecover() throws Exception {
        assertTrue(balancer.isWorking());
        when(node1.isConnected()).thenReturn(true);
        when(node1.isWorking()).thenReturn(false);
        when(node2.isConnected()).thenReturn(true);
        when(node2.isWorking()).thenReturn(false);
        balancer.reBalance();

        verify(node1, atLeast(1)).stateMachine((0.1d + 0.2d) / 2);
        assertFalse(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
        verify(node1).setRecoveryMode(true);
        verify(node1, times(1)).setRecoveryMode(anyBoolean());
        verify(node2).setRecoveryMode(true);
    }

    @Test(groups = "unit")
    public void reBalanceOnUnrecoverable() throws Exception {
        assertTrue(balancer.isWorking());
        when(config.getFailureHandling()).thenReturn(ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK);
        when(node1.isConnected()).thenReturn(true);
        when(node1.isWorking()).thenReturn(false);
        when(node2.isConnected()).thenReturn(true);
        when(node2.isWorking()).thenReturn(false);
        balancer.reBalance();

        assertFalse(balancer.isWorking());
        assertFalse(balancer.canProcessRequests());

        verify(node1, times(0)).setRecoveryMode(anyBoolean());
    }

    @Test(groups = "unit")
    public void reBalanceOnEnable() throws Exception {
        assertTrue(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
        when(node1.isConnected()).thenReturn(true);
        when(node1.isWorking()).thenReturn(false);
        when(node2.isConnected()).thenReturn(true);
        when(node2.isWorking()).thenReturn(false);
        balancer.reBalance();

        assertFalse(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());

        verify(node1).setRecoveryMode(true);
        verify(node1, times(1)).setRecoveryMode(anyBoolean());

        when(node1.isWorking()).thenReturn(true);
        when(node2.isWorking()).thenReturn(true);

        balancer.reBalance();

        verify(node1).setRecoveryMode(false);
        verify(node1, times(2)).setRecoveryMode(anyBoolean());

        assertTrue(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
    }

    @Test(groups = "unit")
    public void reBalanceOnChangeInRing() throws Exception {
        when(node1.stateMachine(anyDouble())).thenReturn(false);
        when(node2.stateMachine(anyDouble())).thenReturn(false);
        balancer.setDcNodes(Arrays.asList(node1), 3);
        balancer.reBalance();

        assertEquals(balancer.getRecover().get(), node1);
        assertEquals(balancer.getRecover().get(), node1);
        assertEquals(balancer.getRecover().get(), node1);
    }

    @Test(groups = "unit")
    public void reBalanceSingleNode() throws Exception {
        when(node1.isConnected()).thenReturn(false);
        when(node2.isWorking()).thenReturn(false);
        balancer.reBalance();

        assertTrue(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
    }

    @Test(groups = "unit")
    public void reBalanceFailRatio() throws Exception {
        when(node2.isWorking()).thenReturn(false);
        when(config.getMinNodesInDcRatio()).thenReturn(0.7);
        balancer.reBalance();

        assertFalse(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
    }

    @Test(groups = "unit")
    public void reBalanceMinNodes() throws Exception {
        when(node2.isWorking()).thenReturn(false);
        when(config.getMinAliveNodes()).thenReturn(2);
        balancer.reBalance();

        assertFalse(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
    }

    @Test(groups = "unit")
    public void reBalanceMaxFail() throws Exception {
        when(node2.isWorking()).thenReturn(false);
        when(config.getMaxFailNodes()).thenReturn(1);
        balancer.reBalance();

        assertFalse(balancer.isWorking());
        assertTrue(balancer.canProcessRequests());
    }

    @Test(groups = "unit", timeOut = 1000)
    public void getRecover() throws Exception {
        NodeStatus<String, ThriftClient<String>> result1 = balancer.getRecover().get();
        when(result1.shouldSendTestRequest()).thenReturn(false);

        Optional<NodeStatus<String, ThriftClient<String>>> result2;
        while (!(result2 = balancer.getRecover()).isPresent());
        when(result2.get().shouldSendTestRequest()).thenReturn(false);

        for (int i = 0; i < 20; i++) {
            assertFalse(balancer.getRecover().isPresent());
        }

        assertEquals(new HashSet<>(Arrays.asList(result1, result2.get())), new HashSet<>(Arrays.asList(node1, node2)));
    }

    @Test(groups = "unit")
    public void nodes() throws Exception {
        assertEquals(balancer.nodes().collect(Collectors.toSet()), new HashSet<>(Arrays.asList(node1, node2)));

        balancer.setDcNodes(Arrays.asList(node1), 3);
        balancer.reBalance();

        assertEquals(balancer.nodes().collect(Collectors.toSet()), new HashSet<>(Arrays.asList(node1)));
    }

    @Test(groups = "unit")
    public void getLoadAccumulator() throws Exception {
        assertSame(balancer.getLoadAccumulator(), balancer.getLoadAccumulator());
    }
}
