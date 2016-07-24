package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.pool.ThriftClient;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class ThriftLoadBalancerTest {
    private NodesHolder<String, ThriftClient<String>> holder;
    private ThriftLoadBalancer<String, ThriftClient<String>> balancer;
    private ThriftDcBalancer<String, ThriftClient<String>> dc1;
    private ThriftDcBalancer<String, ThriftClient<String>> dc2;
    private ThriftBalancerConfig config;
    private NodeStatus<String, ThriftClient<String>> status1;
    private NodeStatus<String, ThriftClient<String>> status2;
    private NodeStatus node1;
    private NodeStatus node2;
    private ThriftClient thriftClient1;
    private ThriftClient thriftClient2;
    private ScheduledExecutorService reBalanceScheduler;
    private ScheduledExecutorService reloadScheduler;
    private ArgumentCaptor<Runnable> reBalanceCaptor;
    private ArgumentCaptor<Runnable> reloadCaptor;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        holder = mock(NodesHolder.class);

        node1 = mock(NodeStatus.class);
        thriftClient1 = mock(ThriftClient.class);
        when(node1.getClient()).thenReturn(thriftClient1);
        node2 = mock(NodeStatus.class);
        thriftClient2 = mock(ThriftClient.class);
        when(node2.getClient()).thenReturn(thriftClient2);
        List<NodeStatus<String, ThriftClient<String>>> value = Arrays.asList(
                node1,
                node2
        );
        when(holder.getFullNodeList()).thenReturn(value);
        dc1 = mock(ThriftDcBalancer.class);
        when(dc1.getDcName()).thenReturn("dc1");
        when(dc1.isWorking()).thenReturn(true);
        when(dc1.canProcessRequests()).thenReturn(true);
        when(dc1.getLoad()).thenReturn(0.1);
        when(dc1.getRecover()).thenReturn(Optional.empty());
        status1 = mock(NodeStatus.class);
        when(dc1.get()).thenReturn(Optional.of(status1));
        when(dc1.getLoadAccumulator()).thenReturn(new LoadBasedBalancer.LoadAccumulator<>(dc1));
        dc1.getLoadAccumulator().setWeight(1);

        dc2 = mock(ThriftDcBalancer.class);
        when(dc2.getDcName()).thenReturn("dc2");
        when(dc2.isWorking()).thenReturn(true);
        when(dc2.canProcessRequests()).thenReturn(true);
        when(dc2.getLoad()).thenReturn(600.0);
        when(dc2.getRecover()).thenReturn(Optional.empty());
        status2 = mock(NodeStatus.class);
        when(dc2.get()).thenReturn(Optional.of(status2));
        when(holder.getDcList()).thenReturn(Arrays.asList(dc1, dc2));
        when(dc2.getLoadAccumulator()).thenReturn(new LoadBasedBalancer.LoadAccumulator<>(dc2));
        dc2.getLoadAccumulator().setWeight(0);
        dc2.getLoadAccumulator().setLoad(300);

        ThriftBalancerConfig configReal = new ThriftBalancerConfig.Builder(ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK)
                .setServers(Arrays.asList())
                .build();
        config = spy(configReal);
        reloadScheduler = mock(ScheduledExecutorService.class);
        reloadCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(reloadScheduler.scheduleWithFixedDelay(
                reloadCaptor.capture(),
                anyLong(),
                anyLong(),
                any()
        )).then(RETURNS_MOCKS);

        reBalanceScheduler = mock(ScheduledExecutorService.class);
        reBalanceCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(reBalanceScheduler.scheduleWithFixedDelay(
                reBalanceCaptor.capture(),
                anyLong(),
                anyLong(),
                any()
        )).then(RETURNS_MOCKS);

        balancer = new ThriftLoadBalancer<>(
                "testService",
                config,
                reBalanceScheduler,
                reloadScheduler,
                holder
        );


    }

    @Test(groups = "unit")
    public void reBalance() throws Exception {
        assertTrue(dc2.getLoadAccumulator().getLoad() > 320.0);
        verify(dc1).reBalance();
        verify(dc2).reBalance();

        assertFalse(dc1.getLoadAccumulator().isPreferred());
        assertFalse(dc2.getLoadAccumulator().isPreferred());
        when(config.getPreferredDc()).thenReturn("dc1");

        balancer.reBalance();

        assertTrue(dc1.getLoadAccumulator().isPreferred());
        assertFalse(dc2.getLoadAccumulator().isPreferred());

        assertEquals(balancer.get().get(), status1);
    }

    @Test(groups = "unit")
    public void reBalanceFail() throws Exception {
        when(dc1.isWorking()).thenReturn(false);
        when(dc1.canProcessRequests()).thenReturn(false);
        when(dc2.isWorking()).thenReturn(false);
        when(dc2.canProcessRequests()).thenReturn(true);

        balancer.reBalance();
        assertEquals(balancer.get().get(), status2);
    }

    @Test(groups = "unit")
    public void get() throws Exception {
        balancer.reBalance();

        assertEquals(balancer.get().get(), status1);
    }

    @Test(groups = "unit")
    public void getRecover() throws Exception {
        NodeStatus<String, ThriftClient<String>> statusRecover = mock(NodeStatus.class);
        when(dc2.getRecover()).thenReturn(Optional.of(statusRecover));

        boolean recoverFound = false;
        for (int i = 0; i < 100; i++) {
            NodeStatus<String, ThriftClient<String>> status = balancer.get().get();
            if (status == statusRecover) {
                recoverFound = true;
                break;
            } else if (status != status1) {
                fail("Unexpected node returned");
            }
        }
        assertTrue(recoverFound);
    }

    @Test(groups = "unit")
    public void schedules() throws Exception {
        for (Runnable runnable : reloadCaptor.getAllValues()) {
            runnable.run();
        }

        verify(holder, times(1)).reloadNodes(true);
        verify(holder, times(1)).reloadNodes(false);
        verify(dc1, times(2)).doHealthCheck();
        verify(dc2, times(2)).doHealthCheck();
        verify(thriftClient1, times(2)).evict();
        verify(thriftClient2, times(2)).evict();

        verify(dc1, times(1)).reBalance();
        verify(dc2, times(1)).reBalance();

        for (Runnable runnable : reBalanceCaptor.getAllValues()) {
            runnable.run();
        }
        verify(dc1, times(2)).reBalance();
        verify(dc2, times(2)).reBalance();
    }
}
