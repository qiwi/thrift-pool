package com.qiwi.thrift.consul;

import com.qiwi.thrift.pool.ThriftAsyncClient;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.tracing.ThriftLogContext;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.*;

/**
 * For correct working you must run Consul server locally
 */
@SuppressWarnings("MagicNumber")
public class ThriftConsulDemoTest {
    public static String clusterUUID = "V" + UUID.randomUUID();

    private AnnotationConfigApplicationContext server1Context;
    private TestThriftServer server1;
    private SecondServerImp2 server1Imp2;
    private AnnotationConfigApplicationContext clientContext;


    enum ServiceTypes{
        DEMO_SERVER,
        SECOND_SERVER,
    }

    public void initClient(ServiceTypes types, int... ports) throws Exception {
        clientContext = startClient();
        ConsulService bean = clientContext.getBean(ConsulService.class);
        int waitTime = 0;
        for (int port : ports) {
            while (true) {
                ThriftServiceDescription<?> description;
                if (types == ServiceTypes.DEMO_SERVER) {
                    description = new ThriftServiceDescription.Builder<>(DemoServer.Iface.class)
                            .build();
                } else {
                    description = new ThriftServiceDescription.Builder<>(SecondServer.Iface.class)
                            .setSubServiceName(Optional.of("imp2"))
                            .setClusterName(clusterUUID)
                            .build();
                }
                Set<ThriftClientAddress> services = bean.getServices(
                        description
                );
                if (services.stream().anyMatch(node -> node.getPort() == port)) {
                    break;
                }
                waitTime ++;
                if (waitTime > 410) {
                    fail("Service not registred in consul port " + port + " service " + services);
                }
                Thread.sleep(100);
            }
        }
    }

    // FIXME at now consul not available in test environment
    @BeforeMethod(groups = "integration")
    public void setUp() throws Exception {
        server1Context = startServer();
        server1 = server1Context.getBean(TestThriftServer.class);
        server1Imp2 = server1Context.getBean(SecondServerImp2.class);
        server1.startServer();
        server1.awaitStart();
    }


    @AfterMethod(groups = "integration")
    public void tearDown() throws Exception {
        clientContext.destroy();
        server1Context.destroy();
    }

    private AnnotationConfigApplicationContext startServer() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.getEnvironment().addActiveProfile("server");
        ctx.register(ServerConfiguration.class);
        ctx.refresh();
        return ctx;
    }


    private AnnotationConfigApplicationContext startClient() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.getEnvironment().addActiveProfile("client");
        ctx.register(ClientConfiguration.class);
        ctx.refresh();
        return ctx;
    }

    @Test(groups = "integration")
    public void singleServer() throws Exception {
        initClient(ServiceTypes.DEMO_SERVER, server1.getPort());
        DemoServer.Iface demoServer = clientContext.getBean(DemoServer.Iface.class);

        for (int i = 0; i < 50; i++) {
            MapResult result = demoServer.request("test", 42);
            assertEquals(result.getStatus(), Status.OK);
        }
        ThriftLogContext.setSpanId(0xCAFE);
        ThriftLogContext.setTraceId(0xBEEF);
        Status status = demoServer.loadTest();
        assertEquals(status, Status.OK);
    }

    @Test(groups = "integration")
    public void multiServer() throws Exception {
        try (AnnotationConfigApplicationContext server2Context = startServer()) {
            TestThriftServer server2;
            SecondServerImp2 server2Imp;

            server2 = server2Context.getBean(TestThriftServer.class);
            server2Imp = server2Context.getBean(SecondServerImp2.class);
            server2.startServer();
            server2.awaitStart();

            initClient(ServiceTypes.SECOND_SERVER, server1.getPort(), server2.getPort());
            SecondServer.Iface  secondServer = clientContext.getBean(SecondServer.Iface.class);

            for (int i = 0; i < 300; i++) {
                String result = secondServer.request("test", 42);
                assertEquals(result, "test_42");
            }
            assertEquals(server1Imp2.getRequestCount() + server2Imp.getRequestCount(), 300);
            assertTrue(server1Imp2.getRequestCount() > 100, "Request " + server1Imp2.getRequestCount());
            assertTrue(server2Imp.getRequestCount() > 100, "Request " + server2Imp.getRequestCount());
        }
    }

    @Test(groups = "integration")
    public void asyncClient() throws Exception {
        initClient(ServiceTypes.DEMO_SERVER, server1.getPort());
        ThriftAsyncClient<DemoServer.AsyncIface> demoServerAsyncClient  = clientContext.getBean("demoServerAsyncClient", ThriftAsyncClient.class);
        for (int i = 0; i < 50; i++) {
            CompletableFuture<MapResult> resultFuture = demoServerAsyncClient.execAsync(
                    MapResult.class,
                    (client, callback) -> client.request("test", 42, callback)
            );
            assertEquals(resultFuture.join().getStatus(), Status.OK);
        }
        ThriftLogContext.setSpanId(0xCAFE);
        ThriftLogContext.setTraceId(0xBEEF);
        CompletableFuture<Status> statusFuture = demoServerAsyncClient.execAsync(
                Status.class,
                (client, callback) -> client.loadTest(callback)
        );
        Status status = statusFuture.join();
        assertEquals(status, Status.OK);
    }

}
