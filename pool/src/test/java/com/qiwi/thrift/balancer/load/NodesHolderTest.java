package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.metrics.MetricEnabledStatus;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MagicNumber")
public class NodesHolderTest {

    private NodesHolder<String, ThriftClient<String>> holder;

    private Map<ThriftClientAddress, NodeStatus<String, ThriftClient<String>>> addressToNode;
    private NodeStatus<String, ThriftClient<String>> node1dc1;
    private NodeStatus<String, ThriftClient<String>> node2dc1;
    private NodeStatus<String, ThriftClient<String>> node3dc2;
    private ThriftBalancerConfig config;
    private ThriftClientAddress address1dc1;
    private ThriftClientAddress address2dc1;
    private ThriftClientAddress address3dc2;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        addressToNode = new HashMap<>();
        address1dc1 = ThriftClientAddress.parse("test1:123,dc=dc1");
        node1dc1 = mock(NodeStatus.class);
        when(node1dc1.toString()).thenReturn("node1dc1");
        when(node1dc1.getAddress()).thenReturn(address1dc1);
        addressToNode.put(address1dc1, node1dc1);

        address2dc1 = ThriftClientAddress.parse("test2:123,dc=dc1");
        node2dc1 = mock(NodeStatus.class);
        when(node2dc1.toString()).thenReturn("node2dc1");
        when(node2dc1.getAddress()).thenReturn(address2dc1);
        addressToNode.put(address2dc1, node2dc1);

        address3dc2 = ThriftClientAddress.parse("test3:123,dc=dc2");
        node3dc2 = mock(NodeStatus.class);
        when(node3dc2.toString()).thenReturn("node3dc2");
        when(node3dc2.getAddress()).thenReturn(address3dc2);
        addressToNode.put(address3dc2, node3dc2);

        ThriftBalancerConfig configReal = new ThriftBalancerConfig.Builder(ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK)
                .setServers(Arrays.asList(address1dc1, address2dc1, address3dc2))
                .build();
        config = spy(configReal);

        holder = new NodesHolder<String, ThriftClient<String>>(
                node -> null,
                "testService"
        ) {
            @Override
            protected NodeStatus<String, ThriftClient<String>> initNodeStatus(
                    boolean initialCreate, ThriftClientAddress address
            ) {
                return addressToNode.computeIfAbsent(address, adr -> {throw new RuntimeException();});
            }

            @Override
            protected ThriftDcBalancer<String, ThriftClient<String>> initDcBalancer(String name) {
                ThriftDcBalancer<String, ThriftClient<String>> balancer = mock(ThriftDcBalancer.class);
                when(balancer.getDcName()).thenReturn(name);
                return balancer;
            }
        };
        holder.init(config, MetricEnabledStatus.DISABLED);

    }

    @Test(groups = "unit")
    public void reloadNewNodes() throws Exception {
        when(config.getServersSupplier()).thenReturn(() -> new HashSet<>(Arrays.asList(address1dc1)));

        holder.reloadNodes(true);

        List<ThriftDcBalancer<String, ThriftClient<String>>> dcList1 = holder.getDcList();
        assertEquals(dcList1.size(), 1);
        verify(dcList1.get(0)).setDcNodes(Arrays.asList(node1dc1), 1);
        when(config.getServersSupplier()).thenReturn(() -> new HashSet<>(Arrays.asList(address1dc1, address2dc1, address3dc2)));

        holder.reloadNodes(false);

        List<ThriftDcBalancer<String, ThriftClient<String>>> dcList2 = holder.getDcList();
        assertEquals(dcList2.size(), 2);
        verify(dcList2.get(0)).setDcNodes(Arrays.asList(node1dc1, node2dc1), 2);
        verify(dcList2.get(1)).setDcNodes(Arrays.asList(node3dc2), 1);
    }

    @Test(groups = "unit")
    public void reloadCloseNode() throws Exception {
        holder.reloadNodes(true);
        List<ThriftDcBalancer<String, ThriftClient<String>>> dcList1 = holder.getDcList();
        verify(dcList1.get(0)).setDcNodes(Arrays.asList(node1dc1, node2dc1), 2);

        when(config.getServersSupplier()).thenReturn(() -> new HashSet<>(Arrays.asList(address1dc1, address3dc2)));
        holder.reloadNodes(false);
        verify(dcList1.get(0), times(1)).setDcNodes(Arrays.asList(node1dc1), 2);

        holder.reloadNodes(false);
        verify(dcList1.get(0), times(1)).setDcNodes(Arrays.asList(node1dc1), 2);
        verify(dcList1.get(0), times(0)).setDcNodes(Arrays.asList(node1dc1), 1);

        when(node2dc1.isNeedClose()).thenReturn(true);
        holder.reloadNodes(false);
        verify(dcList1.get(0), times(1)).setDcNodes(Arrays.asList(node1dc1), 1);
    }

    @Test(groups = "unit")
    public void reloadReopenNode() throws Exception {
        holder.reloadNodes(true);
        List<ThriftDcBalancer<String, ThriftClient<String>>> dcList1 = holder.getDcList();
        verify(dcList1.get(0), times(1)).setDcNodes(Arrays.asList(node1dc1, node2dc1), 2);

        when(config.getServersSupplier()).thenReturn(() -> new HashSet<>(Arrays.asList(address1dc1, address3dc2)));
        holder.reloadNodes(false);
        verify(dcList1.get(0), times(1)).setDcNodes(Arrays.asList(node1dc1), 2);

        addressToNode.remove(address2dc1);
        when(config.getServersSupplier()).thenReturn(() -> new HashSet<>(Arrays.asList(address1dc1, address2dc1, address3dc2)));
        holder.reloadNodes(false);
        verify(dcList1.get(0), times(2)).setDcNodes(Arrays.asList(node1dc1, node2dc1), 2);
    }

    @Test(groups = "unit")
    public void close() throws Exception {
        holder.reloadNodes(true);
        when(config.getServersSupplier()).thenReturn(() -> new HashSet<>(Arrays.asList(address1dc1, address3dc2)));
        holder.reloadNodes(true);
        holder.close();
        verify(node1dc1).close();
        verify(node2dc1).close();
        verify(node3dc2).close();
    }

}
