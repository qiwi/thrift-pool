package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.pool.ThriftAsyncFunction;
import com.qiwi.thrift.pool.ThriftPoolAsyncClient;
import com.qiwi.thrift.pool.server.DemoServer.AsyncClient;
import com.qiwi.thrift.pool.server.DemoServer.AsyncIface;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

@SuppressWarnings({"MagicNumber", "UnqualifiedInnerClassAccess"})
public class ThriftKeyAsyncClientImpTest {
    public static final Duration MAX_WAIT = Duration.ofDays(1);
    private ThriftKeyLoadBalancer<
            String,
            AsyncIface,
            ThriftPoolAsyncClient<AsyncIface, AsyncClient>
            > balancer;
    private ThriftKeyBalancerConfig config;
    private ThriftKeyAsyncClientImp<String, AsyncIface, AsyncClient> client;
    private List<NodeStatus<AsyncIface, ThriftPoolAsyncClient<AsyncIface, AsyncClient>>> statuses;
    private List<CompletableFuture<Long>> futures;
    private ThriftAsyncFunction function;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        statuses = new ArrayList<>();
        futures = new ArrayList<>();
        balancer = mock(ThriftKeyLoadBalancer.class);
        config = mock(ThriftKeyBalancerConfig.class);
        when(config.getMaxWaitForConnection()).thenReturn(MAX_WAIT);
        function = mock(ThriftAsyncFunction.class);
        client = new ThriftKeyAsyncClientImp<>(balancer, config);
        createQuorumNode();
        createQuorumNode();
    }

    public void createQuorumNode(){
        NodeStatus<AsyncIface, ThriftPoolAsyncClient<AsyncIface, AsyncClient>> status = mock(NodeStatus.class);
        CompletableFuture<Long> future = new CompletableFuture<>();
        ThriftPoolAsyncClient<AsyncIface, AsyncClient> client = mock(ThriftPoolAsyncClient.class);
        when(status.isWorking()).thenReturn(true);
        when(status.getClient()).thenReturn(client);
        when(client.execAsync(Long.class, function, MAX_WAIT.toMillis())).thenReturn(future);
        futures.add(future);
        statuses.add(status);
        when(balancer.getQuorum("TEst")).thenAnswer(invocation -> statuses.stream());
    }

    @Test(groups = "unit")
    public void execOnQuorum() throws Exception {
        CompletableFuture<Long> future = client.execOnQuorum(
                "TEst",
                Long.class,
                function
        );
        assertFalse(future.isDone());
        futures.get(0).completeExceptionally(new RuntimeException("Test ex1"));
        assertFalse(future.isDone());
        futures.get(1).complete(345L);
        assertEquals(future.get(0, TimeUnit.MILLISECONDS), (Object)345L);
    }

    @Test(groups = "unit")
    public void execOnQuorumFail() throws Exception {
        CompletableFuture<Long> future = client.execOnQuorum(
                "TEst",
                Long.class,
                function
        );
        assertFalse(future.isDone());
        futures.get(0).completeExceptionally(new RuntimeException("Test ex1"));
        assertFalse(future.isDone());
        futures.get(1).completeExceptionally(new RuntimeException("Test ex2"));
        try {
            future.get(0, TimeUnit.MILLISECONDS);
            fail("Not throw");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause().getMessage().startsWith("Test ex"));
        }
    }

}
