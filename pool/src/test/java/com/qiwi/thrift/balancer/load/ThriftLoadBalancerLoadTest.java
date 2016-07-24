package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.demo.AbstractThriftServerTest;
import com.qiwi.thrift.demo.DemoThriftServer;
import com.qiwi.thrift.demo.ServerImp;
import com.qiwi.thrift.pool.SyncClientFactory;
import com.qiwi.thrift.pool.ThriftClient;
import com.qiwi.thrift.pool.ThriftSyncClient;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.server.ThriftEndpointConfig;
import com.qiwi.thrift.test.TestRateLimiter;
import com.qiwi.thrift.test.TestUtils;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.LambdaUtils;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

public class ThriftLoadBalancerLoadTest {
    private static final Logger log = LoggerFactory.getLogger(ThriftLoadBalancerLoadTest.class);

    private DemoServer.Iface client;
    private ArrayList<ThriftClientAddress> addresses;
    private ArrayList<ServerImp> servers;
    private ServerImp dc1node1;
    private ServerImp dc1node2;
    private ServerImp dc2node3;
    private ServerImp dc2node4;
    private ArrayList<DemoThriftServer> thriftServers;

    private ThriftBalancerSyncClient<DemoServer.Iface, DemoServer.Client> syncClient;
    private List<ThriftDcBalancer<DemoServer.Iface, ThriftSyncClient<DemoServer.Iface, DemoServer.Client>>> dcList;
    private ThriftBalancerConfig.Builder configBuilder;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        addresses = new ArrayList<>();
        servers = new ArrayList<>();
        thriftServers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int thriftPort = TestUtils.genThriftPort();
            addresses.add(ThriftClientAddress.parse("127.0.0.1:" + thriftPort + ",dc=center" + i / 2));
            ServerImp serverImp = new ServerImp();
            servers.add(serverImp);
            thriftServers.add(AbstractThriftServerTest.startServer(
                    thriftPort,
                    new ThriftEndpointConfig.Builder().addEndpoint(DemoServer.Iface.class, serverImp).build(),
                    ThriftRequestReporter.NULL_REPORTER
            ));
        }
        dc1node1 = servers.get(0);
        dc1node2 = servers.get(1);
        dc2node3 = servers.get(2);
        dc2node4 = servers.get(3);

        // -----=====Создание клиента=====-----
        Properties config = new Properties();
        config.put(
                "thrift.test.client.servers",
                addresses.stream()
                        .map(ThriftClientAddress::toString)
                        .collect(Collectors.joining(";"))
        );

        configBuilder = new ThriftBalancerConfig.Builder(ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK)
                .setRingReBalancePeriod(Duration.ofMillis(100))// 1000 rps / 100 request per period = 0.1 second
                .setNodeDisableTime(Duration.ofMillis(100))// test-only
                .setMaxConnections(200)
                .setRequestTimeout(Duration.ofMinutes(3))
                .setMaxNodeLoadGap(7.0)
                .setServersSupplier(() -> new HashSet<ThriftClientAddress>(addresses));
    }

    public ThriftLoadBalancerFactory getFromContext(){
        System.out.println("dc1_weight\tdc2_weight\tdc1_load_filtered\tdc1_load\tdc1_alt_load\tdc2_load_filtered\tdc2_load\tdc2_alt_load");
        return new ThriftLoadBalancerFactory(
                new ThriftLoadBalancer.Factory() {

                    @Override
                    protected <I, C extends ThriftClient<I>> ThriftLoadBalancer<I, C> newBalancer(
                            String serviceName,
                            ThriftBalancerConfig config,
                            ScheduledExecutorService reBalanceScheduler,
                            ScheduledExecutorService reloadScheduler,
                            NodesHolder<I, C> nodesHolder
                    ) {
                        return new ThriftLoadBalancer<I, C>(
                                serviceName,
                                config,
                                reBalanceScheduler,
                                reloadScheduler,
                                nodesHolder
                        ) {
                            @Override
                            public void reBalance() {
                                super.reBalance();
                                List<ThriftDcBalancer<I, C>> dcCopy = nodesHolder.getDcList();
                                double sum = dcCopy.stream().mapToDouble(dc -> dc.getLoadAccumulator().getWeight()).sum();
                                // Код выводит таблицу пригодную для импорта в excel
                                for (ThriftDcBalancer<I, C> dcBalancer : dcCopy) {
                                    System.out.printf("%4.3f\t", dcBalancer.getLoadAccumulator().getWeight() / sum);
                                }
                                for (ThriftDcBalancer<I, C> dcBalancer : dcCopy) {
                                    System.out.printf("%4.3f\t", dcBalancer.getLoadAccumulator().getLoad());
                                    System.out.printf("%4.3f\t", dcBalancer.getLoad());
                                    System.out.printf("%4.3f\t", dcBalancer.nodes().mapToDouble(nodes -> nodes.getLoadForTests()).sum());
                                }
                                System.out.println();

                                /*
                                for (ThriftDcBalancer<I, C> dcBalancer : dcCopy) {
                                    dcBalancer.nodes().forEach(node -> System.out.println(node.stats));
                                }*/
                            }
                        };
                    }
                },
                new SyncClientFactory(null),
                null,
                null
        );
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        if (client != null) {
            ((Closeable) client).close();
        }
        for (DemoThriftServer server : thriftServers) {
            server.stopServer();
        }
    }

    public void runServer(){
        ThriftLoadBalancerFactory poolFactory = getFromContext();
        syncClient = poolFactory.createSyncBalancer(
                DemoServer.Iface.class,
                configBuilder,
                new DemoServer.Client.Factory(),
                LambdaUtils.uncheckedP(DemoServer.Iface::healthCheck)
        );
        client = ThriftSyncClient.makeProxy(DemoServer.Iface.class, syncClient);
        dcList = syncClient.getBalancer().dcList().collect(Collectors.toList());
    }

    @Test(groups = "unit")
    public void dcSlow() throws Exception{
        configBuilder.setPreferredDc("center2");
        runServer();
        final int COUNT1 = 1_000;
        final int COUNT2 = 20_000;

        ExecutorService executor = Executors.newFixedThreadPool(100);
        try {
            TestRateLimiter limiter = new TestRateLimiter(1000);
            //limiter.setRandomFactor(TimeUnit.MILLISECONDS.toNanos(5));
            runLoadTest(COUNT1, executor, limiter);
            dc2node3.setMaxRps(67);
            dc2node4.setMaxRps(123);
            runLoadTest(COUNT2, executor, limiter);
            assertWeight(dcList.get(1), 0.2, 0.095);
            runLoadTest(COUNT2, executor, limiter);
            assertWeight(dcList.get(1), 0.2, 0.095);
        } finally {
            executor.shutdown();
            executor.awaitTermination(50, TimeUnit.SECONDS);
        }
    }

    @Test(groups = "integration")
    public void dcSlowLongPeriod() throws Exception{
        addresses.remove(3);
        addresses.remove(1);
        configBuilder.setRingReBalancePeriod(Duration.ofMillis(1000));
        runServer();
        final int COUNT1 = 5_000;
        final int COUNT2 = 40_000;

        ExecutorService executor = Executors.newFixedThreadPool(500);
        try {
            TestRateLimiter limiter = new TestRateLimiter(1000);
            limiter.setRandomFactor(TimeUnit.MILLISECONDS.toNanos(5));
            runLoadTest(COUNT1, executor, limiter);
            dc2node3.setMaxRps(200);
            runLoadTest(COUNT2, executor, limiter);
            assertWeight(dcList.get(1), 0.2, 0.09);
            runLoadTest(COUNT2, executor, limiter);
            assertWeight(dcList.get(1), 0.2, 0.09);
        } finally {
            executor.shutdown();
            executor.awaitTermination(50, TimeUnit.SECONDS);
        }
    }

    private void assertWeight(
            ThriftDcBalancer<DemoServer.Iface, ThriftSyncClient<DemoServer.Iface, DemoServer.Client>> dc,
            double weight,
            double eps
    ) {
        double weightNormalizer = dcList.stream().mapToDouble(val -> val.getLoadAccumulator().getWeight()).sum();
        double actualWeight = dc.getLoadAccumulator().getWeight() / weightNormalizer;
        if (actualWeight > weight + eps * 2 || actualWeight < weight - eps) {
            fail("expected " + weight + " but found " + actualWeight + " eps " + eps);
        }
    }


    @Test(groups = "unit")
    public void dcFail() throws Exception{
        runServer();
        final int COUNT1 = 1_000;
        final int COUNT2 = 2_000;
        ExecutorService executor = Executors.newFixedThreadPool(100);
        try {
            TestRateLimiter limiter = new TestRateLimiter(1000);
            runLoadTest(COUNT1, executor, limiter);

            dc2node3.setErrorRate(0.95);
            long errorCount = runLoadTest(COUNT2, executor, limiter);
            assertTrue(errorCount <= 250, "errorCount: " + errorCount);

            errorCount = runLoadTest(COUNT2, executor, limiter);
            assertTrue(errorCount <= 50, "errorCount: " + errorCount);

            dc2node4.setErrorRate(0.95);
            errorCount = runLoadTest(COUNT2, executor, limiter);
            assertTrue(errorCount <= 250, "errorCount: " + errorCount);
            assertEquals(syncClient.getBalancer().dcList().count(), 1);

            dc2node3.setErrorRate(0);
            dc2node4.setErrorRate(0);

            errorCount = runLoadTest(COUNT2, executor, limiter);
            assertTrue(errorCount <= 0, "errorCount: " + errorCount);
            assertEquals(syncClient.getBalancer().dcList().count(), 2);
        } finally {
            executor.shutdown();
            executor.awaitTermination(50, TimeUnit.SECONDS);
        }
    }

    @Test(groups = "unit")
    public void dcBusinessExceptions() throws Exception{
        configBuilder.enableCircuitBreak(0.3, ex -> !(ex instanceof TestException));
        runServer();
        final int COUNT1 = 6_000;
        ExecutorService executor = Executors.newFixedThreadPool(100);
        try {
            TestRateLimiter limiter = new TestRateLimiter(1000);
            runLoadTest(COUNT1, executor, limiter);

            dc1node1.setErrorRate(0.95);
            dc2node4.setErrorRate(0.95);
            long errorCount = runLoadTest(COUNT1, executor, limiter);

            assertTrue(errorCount > 2500 && errorCount < 3500, "errorCount: " + errorCount);
            assertEquals(syncClient.getBalancer().nodes().count(), 4);
        } finally {
            executor.shutdown();
            executor.awaitTermination(50, TimeUnit.SECONDS);
        }
    }

    private long runLoadTest(int count, ExecutorService executor, TestRateLimiter limiter) throws Exception {
        AtomicLong errorCount = new AtomicLong();
        AtomicReference<Exception> reference = new AtomicReference<>();
        for (int i = 0; i < count; i++) {
            limiter.await();
            executor.execute(() -> {
                try {
                    client.loadTest();
                } catch (TestException e) {
                    errorCount.incrementAndGet();
                } catch (TException e) {
                    reference.set(e);
                }
            });
        }
        if (reference.get() != null) {
            throw reference.get();
        }
        return errorCount.get();
    }
}
