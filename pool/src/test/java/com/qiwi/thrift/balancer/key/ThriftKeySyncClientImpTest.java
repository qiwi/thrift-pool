package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.NodeStatus;
import com.qiwi.thrift.pool.ThriftSyncClient;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.utils.LambdaUtils;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MagicNumber")
public class ThriftKeySyncClientImpTest {

    private ThriftKeyLoadBalancer<String, DemoServer.Iface, ThriftSyncClient<DemoServer.Iface, DemoServer.Client>> balancer;
    private NodeStatus<DemoServer.Iface, ThriftSyncClient<DemoServer.Iface, DemoServer.Client>> status;
    private ThriftSyncClient<DemoServer.Iface, DemoServer.Client> syncClient;
    private ThriftKeySyncClientImp<String, DemoServer.Iface, DemoServer.Client> keyClient;
    private Method method;
    private MapResult mapResult;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Throwable {
        method = DemoServer.Iface.class.getMethod("request", String.class, long.class);
        balancer = mock(ThriftKeyLoadBalancer.class);
        when(balancer.getFailedNodes()).thenAnswer(invocation -> Arrays.asList(ThriftClientAddress.parse("test:123")).stream());
        status = mock(NodeStatus.class);
        when(status.isWorking()).thenReturn(true);
        when(balancer.get("testKey")).thenReturn(Optional.of(status));
        when(balancer.getQuorum("testKey")).thenAnswer(invocation -> Stream.of(status));
        syncClient = mock(ThriftSyncClient.class);
        when(status.getClient()).thenReturn(syncClient);
        mapResult = mock(MapResult.class);
        when(syncClient.invoke(eq(method), aryEq(new Object[]{"testVal", 42L}), eq(142L))).thenReturn(mapResult);

        ThriftKeyBalancerConfig config = mock(ThriftKeyBalancerConfig.class);
        when(config.getMaxWaitForConnection()).thenReturn(Duration.ofMillis(142));
        keyClient = new ThriftKeySyncClientImp<>(
                balancer,
                config,
                DemoServer.Iface.class
        );
    }

    @Test(groups = "unit")
    public void get() throws Exception {
        DemoServer.Iface client = keyClient.get("testKey");
        MapResult result = client.request("testVal", 42);
        assertEquals(result, mapResult);
    }

    @Test(groups = "unit", expectedExceptions = TTransportException.class)
    public void getMissing() throws Exception {
        when(balancer.get("testKey")).thenReturn(Optional.empty());
        DemoServer.Iface client = keyClient.get("testKey");
    }

    @Test(groups = "unit")
    public void getQuorum() throws Exception {
        MapResult result = keyClient.getQuorum("testKey")
                .map(LambdaUtils.uncheckedF(iface -> iface.request("testVal", 42)))
                .findFirst()
                .get();
        assertEquals(result, mapResult);
    }

    @Test(groups = "unit", expectedExceptions = TTransportException.class)
    public void getQuorumMissing() throws Exception {
        when(balancer.getQuorum("testKey")).thenAnswer(invocation -> Stream.of());
        MapResult result = keyClient.getQuorum("testKey")
                .map(LambdaUtils.uncheckedF(iface -> iface.request("testVal", 42)))
                .findFirst()
                .get();
    }

}
