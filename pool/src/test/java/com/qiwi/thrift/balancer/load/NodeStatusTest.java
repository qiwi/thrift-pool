package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.test.TestClock;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class NodeStatusTest {
    NodeStatus<DemoServer.Iface, ThriftClient<DemoServer.Iface>> node;
    private ThriftClient client;
    private TestClock clock;
    private ThriftBalancerConfig config;
    private ThriftRequestReporter reporter;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        client = mock(ThriftClient.class);
        when(client.getUsedConnections()).thenReturn(1);
        when(client.getMaxConnections()).thenReturn(20);
        when(client.isHealCheckOk()).thenReturn(true);

        Function<ThriftClientConfig, ThriftClient<DemoServer.Iface>> clientFactory = mock(Function.class);
        when(clientFactory.apply(any())).thenReturn(client);

        clock = new TestClock();
        reporter = mock(ThriftRequestReporter.class);
        config = new ThriftBalancerConfig.Builder(ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK)
                .setServers(new ArrayList<>())
                .setCircuitBreakerErrorRatio(0.5)
                .setRingReBalancePeriod(Duration.ofSeconds(1))
                .setRequestReporter(reporter)
                .build();
        node = new NodeStatus<>(
                config,
                ThriftClientAddress.parse("test:123"),
                clientFactory,
                true,
                clock
        );

    }

    @Test(groups = "unit")
    public void getPoolLoad() throws Exception {
        clock.setCurrentTime(Instant.now().truncatedTo(ChronoUnit.MINUTES).plusSeconds(60));

        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        verify(reporter).requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos(2000), Optional.empty());
        verify(reporter).requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos(2000), Optional.empty());
        clock.setCurrentTime(clock.instant().plusMillis(250));
        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos(1500), null);
        clock.setCurrentTime(clock.instant().plusMillis(250));
        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos( 500), null);
        clock.setCurrentTime(clock.instant().plusMillis(250));
        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos(1000), null);
        clock.setCurrentTime(clock.instant().plusMillis(250));

        when(client.getUsedConnections()).thenReturn(4);

        assertEquals(node.getLoad(), 0.2, 0.005);
    }

    @Test(groups = "unit")
    public void setMaxConnections() throws Exception {
        node.setMaxConnections(30);
        verify(client).setMaxConnections(30);
    }

    @Test(groups = "unit")
    public void setRecoveryMode() throws Exception {
        node.getLoadAccumulator().setLoad(1.2);
        assertTrue(node.stateMachine(0.1));
        assertFalse(node.isInRing());
        node.setRecoveryMode(true);
        assertTrue(node.isInRing());
    }

    @Test(groups = "unit")
    public void stateMachineAllOk() throws Exception {
        for (int i = 0; i < 5; i++) {
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 110, null);
        }
        for (int i = 0; i < 3; i++) {
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.UNEXPECTED_ERROR, 80, Optional.of(new TTransportException()));
        }
         node.getLoadAccumulator().setLoad(0.1);

        assertFalse(node.stateMachine(0.1));
        assertTrue(node.isWorking());
        assertTrue(node.isConnected());
    }

    @Test(groups = "unit")
    public void stateMachineErrorRate() throws Exception {
        for (int i = 0; i < 3; i++) {
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 110, null);
        }
        for (int i = 0; i < 5; i++) {
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.UNEXPECTED_ERROR, 80, Optional.of(new TTransportException()));
        }
        node.getLoadAccumulator().setLoad(0.1);

        assertTrue(node.stateMachine(0.1));
        assertFalse(node.isWorking());
        assertTrue(node.isConnected());
    }

    @Test(groups = "unit")
    public void stateMachineConnectionFail() throws Exception {
        for (int i = 0; i < 5; i++) {
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 110, null);
        }
        for (int i = 0; i < 3; i++) {
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.CONNECTION_ERROR, 80, Optional.of(new TTransportException()));
        }
        node.getLoadAccumulator().setLoad(0.1);

        assertTrue(node.stateMachine(0.1));
        assertFalse(node.isWorking());
        assertTrue(node.isConnected());
   }

    @Test(groups = "unit")
    public void stateMachineLoadAverage() throws Exception {
        node.getLoadAccumulator().setLoad(1.2);

        assertTrue(node.stateMachine(0.1));
        assertFalse(node.isWorking());
        assertTrue(node.isConnected());

        assertFalse(node.stateMachine(0.1));
        assertFalse(node.isWorking());
        assertTrue(node.isConnected());
    }


    @Test(groups = "unit")
    public void stateMachineDisconnect() throws Exception {
        when(client.isHealCheckOk()).thenReturn(false);
        node.doHealthCheck();

        assertTrue(node.stateMachine(0.1));
        assertFalse(node.isWorking());
        assertFalse(node.isConnected());

        assertFalse(node.stateMachine(0.1));
        assertFalse(node.isConnected());
    }

    @Test(groups = "unit")
    public void stateMachineTestFail() throws Exception {
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.CONNECTION_ERROR, 100, Optional.of(new TTransportException()));
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.CONNECTION_ERROR, 100, Optional.of(new TTransportException()));
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.CONNECTION_ERROR, 100, Optional.of(new TTransportException()));
        assertTrue(node.stateMachine(0.1));
        clock.setCurrentTime(Instant.now().plusSeconds(60));
        node.getLoadAccumulator().setLoad(0.1);
        assertFalse(node.stateMachine(0.1));
        assertTrue(node.shouldSendTestRequest());
        assertFalse(node.isInRing());
        assertFalse(node.isWorking());

        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.CONNECTION_ERROR, 100, Optional.of(new TTransportException()));
        assertFalse(node.stateMachine(0.1));
        assertFalse(node.isInRing());
        assertFalse(node.shouldSendTestRequest());
    }

    @Test(groups = "unit")
    public void stateMachineTestSuccess() throws Exception {
        node.getLoadAccumulator().setLoad(1.2);
        assertTrue(node.stateMachine(0.1));

        node.getLoadAccumulator().setLoad(0.1);
        assertFalse(node.stateMachine(0.1));
        assertFalse(node.shouldSendTestRequest());

        clock.setCurrentTime(Instant.now().plusSeconds(60));
        assertFalse(node.stateMachine(0.1));
        assertTrue(node.shouldSendTestRequest());
        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 100, null);
        assertFalse(node.shouldSendTestRequest());

        assertFalse(node.stateMachine(0.1));
        while (node.shouldSendTestRequest()) {
            node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 100, null);
        }

        assertTrue(node.stateMachine(0.1));
        assertTrue(node.isInRing());
        assertFalse(node.shouldSendTestRequest());
    }

    @Test(groups = "unit")
    public void stateMachineReconnect() throws Exception {
        when(client.isHealCheckOk()).thenReturn(false);
        node.doHealthCheck();
        assertTrue(node.stateMachine(0.1));
        assertFalse(node.isWorking());
        assertFalse(node.isConnected());

        when(client.isHealCheckOk()).thenReturn(true);
        node.doHealthCheck();
        clock.setCurrentTime(Instant.now().plusSeconds(60));
        assertFalse(node.stateMachine(0.1));
        assertFalse(node.stateMachine(0.1));
        assertTrue(node.shouldSendTestRequest());
        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 100, null);

        assertFalse(node.stateMachine(0.1));
        assertFalse(node.isInRing());
        assertFalse(node.stateMachine(0.1));
        assertFalse(node.isInRing());
        while (node.shouldSendTestRequest()) {
            node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
            node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.SUCCESS, 100, null);
        }

        assertTrue(node.stateMachine(0.1));
        assertTrue(node.isInRing());
        assertFalse(node.shouldSendTestRequest());
    }

    @Test(groups = "unit")
    public void stateMachineTimeGrowOnFail() throws Exception {
        node.getLoadAccumulator().setLoad(1.2);
        assertTrue(node.stateMachine(0.1));

        node.getLoadAccumulator().setLoad(0.2);
        clock.setCurrentTime(Instant.now().plusSeconds(60));
        node.stateMachine(0.1);
        assertTrue(node.shouldSendTestRequest());
        node.requestBegin("srv", "fnc", ThriftCallType.ASYNC_CLIENT);
        node.requestEnd("srv", "fnc", ThriftCallType.ASYNC_CLIENT, ThriftRequestStatus.CONNECTION_ERROR, 100, Optional.of(new TTransportException()));

        node.getLoadAccumulator().setLoad(1.3);
        node.stateMachine(0.1);

        node.getLoadAccumulator().setLoad(0.1);
        clock.setCurrentTime(Instant.now().plusSeconds(120));
        node.stateMachine(0.1);
        assertFalse(node.shouldSendTestRequest());

        clock.setCurrentTime(Instant.now().plusSeconds(600));
        node.stateMachine(0.1);
        assertTrue(node.shouldSendTestRequest());
     }

    @Test(groups = "unit")
    public void setCloseTime() throws Exception {
        assertTrue(node.isNeedClose());
        node.setCloseTime();
        assertFalse(node.isNeedClose());
    }

    @Test(groups = "unit")
    public void close() throws Exception {
        node.close();
        verify(client).close();
    }

    @Test(groups = "unit")
    public void doHealCheck() throws Exception {
        when(client.isHealCheckOk()).thenReturn(true);
        node.doHealthCheck();
        verify(client).isHealCheckOk();

        node.stateMachine(0.1);
        assertTrue(node.isWorking());
    }

    @Test(groups = "unit")
    public void doHealCheckFail() throws Exception {
        when(client.isHealCheckOk()).thenThrow(new RuntimeException("Test"));
        node.doHealthCheck();
        verify(client, times(2)).isHealCheckOk();

        node.stateMachine(0.1);
        assertFalse(node.isWorking());
    }

    @Test(groups = "unit")
    public void getAccumulator() throws Exception {
        assertSame(node.getLoadAccumulator().getItem(), node);
    }

}
