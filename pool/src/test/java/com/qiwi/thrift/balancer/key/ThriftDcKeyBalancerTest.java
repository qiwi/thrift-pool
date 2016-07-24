package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.Balancer;
import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.pool.ThriftClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("MagicNumber")
public class ThriftDcKeyBalancerTest {

    private ThriftDcKeyBalancer<String, String, ThriftClient<String>>
            balancer;
    private ToLongFunction<String> keyMapper;
    private ThriftKeyBalancerConfig config;
    private BalancerConsistentHash recovery;
    private BalancerConsistentHash working;
    private NodeStatus<String, ThriftClient<String>> node1;
    private NodeStatus<String, ThriftClient<String>> node2;
    private List<NodeStatus<String, ThriftClient<String>>> nodes;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        keyMapper = key -> 3;
        config = mock(ThriftKeyBalancerConfig.class);
        node1 = mock(NodeStatus.class);
        node2 = mock(NodeStatus.class);
        recovery = mock(BalancerConsistentHash.class);
        nodes = Arrays.asList(node1, node2);
        when(recovery.getQuorum("key1")).thenAnswer(invocation -> nodes.stream());
        working = mock(BalancerConsistentHash.class);
        when(working.getQuorum("key1")).thenAnswer(invocation -> nodes.stream());
        balancer = new TestThriftClientThriftDcKeyBalancer();
    }

    @Test(groups = "unit")
    public void getQuorumByKeyRecovery() throws Exception {
        when(node1.shouldSendTestRequest()).thenReturn(true);
        when(node2.shouldSendTestRequest()).thenReturn(false);
        when(node2.isInRing()).thenReturn(false);
        when(node2.isInRing()).thenReturn(true);

        assertTrue(balancer.getQuorumByKeyRecovery("key1").isPresent());
        when(node1.shouldSendTestRequest()).thenReturn(false);
        assertFalse(balancer.getQuorumByKeyRecovery("key1").isPresent());
        when(node1.isInRing()).thenReturn(true);
        assertFalse(balancer.getQuorumByKeyRecovery("key1").isPresent());
    }

    private class TestThriftClientThriftDcKeyBalancer extends ThriftDcKeyBalancer<String, String, ThriftClient<String>> {
        public TestThriftClientThriftDcKeyBalancer() {
            super("tst", "dl", ThriftDcKeyBalancerTest.this.config, ThriftDcKeyBalancerTest.this.keyMapper);
        }

        @Override
        protected Balancer<NodeStatus<String, ThriftClient<String>>> createBalancer(
                List<NodeStatus<String, ThriftClient<String>>> items, BalancerType type
        ) {
            switch (type) {
                case WORKING:
                    return working;
                case RECOVERY:
                    return recovery;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }
}
