package com.qiwi.thrift.demo;

import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;
import com.qiwi.thrift.balancer.load.ThriftLoadBalancer;
import com.qiwi.thrift.balancer.load.ThriftLoadBalancerFactory;
import com.qiwi.thrift.pool.AsyncClientFactory;
import com.qiwi.thrift.pool.SyncClientFactory;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.server.ThriftEndpointConfig;
import com.qiwi.thrift.test.TestUtils;
import com.qiwi.thrift.test.Timer;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class BalancerSyncDemoTest{
    private static final Logger log = LoggerFactory.getLogger(BalancerSyncDemoTest.class);

    private DemoServer.Iface client;
    private ArrayList<ThriftClientAddress> addresses;
    private ArrayList<DemoThriftServer> thriftServers;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        addresses = new ArrayList<>();
        thriftServers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int thriftPort = TestUtils.genThriftPort();
            addresses.add(ThriftClientAddress.parse("127.0.0.1:" + thriftPort + ",dc=center" + i / 2));
            ServerImp serverImp = new ServerImp();
            thriftServers.add(AbstractThriftServerTest.startServer(
                    thriftPort,
                    new ThriftEndpointConfig.Builder().addEndpoint(DemoServer.Iface.class, serverImp).build(),
                    ThriftRequestReporter.NULL_REPORTER
            ));
        }

        // -----=====Создание клиента=====-----
        Properties config = new Properties();
        // emulating data loading from property file
        config.put(
                "thrift.test.client.servers",
                addresses.stream()
                        .map(ThriftClientAddress::toString)
                        .collect(Collectors.joining(";"))
        );

        ParameterSource parameterSource = ParameterSource.subpath(
                "thrift.test.client.",
                (name, defaultValue) -> config.containsKey(name)? config.getProperty(name): defaultValue
        );
        ThriftLoadBalancerFactory poolFactory = getFromContext();
        ThriftBalancerConfig.Builder configBuilder = new ThriftBalancerConfig.Builder(ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK)
                .setRingReBalancePeriod(Duration.ofMillis(1000))// 1000 rps / 100 request per period = 0.1 second
                .enableCircuitBreak(0.3, ex -> !(ex instanceof TestException))
                .fromParameters(parameterSource);
        client = poolFactory.createSyncClient(
                DemoServer.Iface.class,
                configBuilder
        );
    }

    /**
     * Used instead DI injector
     * @return
     */

    public ThriftLoadBalancerFactory getFromContext(){
        ReflectConfigurator reflectConfigurator = new ReflectConfigurator();
        return new ThriftLoadBalancerFactory(
                new ThriftLoadBalancer.Factory(),
                new SyncClientFactory(reflectConfigurator),
                new AsyncClientFactory(reflectConfigurator),
                reflectConfigurator
        );
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        ((Closeable)client).close();
        for (DemoThriftServer server : thriftServers) {
            server.stopServer();
        }
    }

    @Test(groups = "unit")
    public void syncTest() throws Exception {
        MapResult result = client.request("test", 42);
        assertEquals(result.getStatus(), Status.OK);
    }

    @Test(groups = "unit")
    public void syncLoadTest() throws Exception {
        SyncDemoTest.testSpeed(client, 10_000);
    }

    @Test(groups = "unit")
    public void syncThrowTest() throws Exception {
        final int COUNT = 256;
        Timer timer = new Timer();
        for (int i = 0; i < COUNT; i++) {
            long id = i;

            // -----=====Вызов клиента=====----
            // Тип возвращаемого значения нужно посмотреть вручную
            // Будет исправлено в thrift 0.10.0
            try {
                client.requestWithError("test", id);
                fail("Not throw exception");
            } catch (TestException ex) {
            }
        }
        timer.print(COUNT);
    }
}
