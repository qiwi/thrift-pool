package com.qiwi.thrift.demo;

import com.qiwi.thrift.pool.SyncClientFactory;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.test.Timer;
import com.qiwi.thrift.utils.ParameterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

public class SyncDemoTest extends AbstractThriftServerTest {
    private static final Logger log = LoggerFactory.getLogger(SyncDemoTest.class);
    private static final int PARALLELS = 64;

    private DemoServer.Iface client;
    private ParameterSource parameterSource;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        // -----=====Создание клиента=====-----
        SyncClientFactory poolFactory = new SyncClientFactory(new ReflectConfigurator());
        parameterSource = ParameterSource.subpath(
                "thrift.test.client.",
                (name, defaultValue) -> config.containsKey(name) ? config.getProperty(name) : defaultValue
        );
        client = poolFactory.create(
                DemoServer.Iface.class,
                new ThriftClientConfig.Builder()
                        .fromParameters(parameterSource)
                        .build()
        );
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        ((Closeable)client).close();
    }

    @Test(groups = "unit")
    public void syncTest() throws Exception {
        MapResult result = client.request("test", 42);
        assertEquals(result.getStatus(), Status.OK);
    }

    @Test(groups = "unit")
    public void syncMultiplexTest() throws Exception {
        SyncClientFactory poolFactory = new SyncClientFactory(new ReflectConfigurator());

        SecondServer.Iface secondClient = poolFactory.create(
                SecondServer.Iface.class,
                new ThriftClientConfig.Builder()
                        .fromParameters(parameterSource)
                        .build()
        );

        MapResult result = client.request("test", 42);
        assertEquals(result.getStatus(), Status.OK);
        String result2 = secondClient.request("test", 42);
        assertEquals(result2, "test:42");
    }

    @Test(groups = "unit")
    public void syncLoadTest() throws Exception {
        testSpeed(client, 10_000);
    }


    public static void testSpeed(DemoServer.Iface iface, long count) throws Exception {
        Semaphore semaphore = new Semaphore(PARALLELS - 2);
        ExecutorService executor = Executors.newFixedThreadPool(PARALLELS);
        try {
            Timer timer = new Timer();
            AtomicReference<Throwable> exception = new AtomicReference<>();
            for (int i = 0; i < count; i++) {
                semaphore.acquire();
                Timer.MeasurementLatency latency = timer.startLatency();
                long id = i;
                executor.execute(() -> {
                    try {
                        latency.end();
                        MapResult test = iface.request("test", id);
                        if (test.getStatus() != Status.OK) {
                            System.err.println("Invalid status");
                        }
                        semaphore.release();
                    } catch (Exception e) {
                        exception.set(e);
                    }
                });
            }
            executor.shutdown();
            if (exception.get() != null) {
                fail("Test completed with error", exception.get());
            }
            if (!executor.awaitTermination(60, TimeUnit.MINUTES)){
                fail("Unable to complete test", exception.get());
            }
            if (exception.get() != null) {
                fail("Test completed with error", exception.get());
            }
            timer.print(count);
        } finally {
            executor.shutdown();
        }
    }

    @Test(groups = "unit")
    public void syncThrowTest() throws Exception {
        Timer timer = new Timer();
        final int COUNT = 256;
        for (int i = 0; i < COUNT; i++) {
            long id = i;

            // -----=====Вызов клиента=====----
            // Тип возвращаемого значения нужно посмотреть вручную
            // Будет исправлено в thrift 0.10.0
            try {
                client.requestWithError("test", id);
                fail("Not throw exception");
            } catch (TestException ex) {
                assertTrue(ex.getMessage().contains("Db"));
            }
        }
        timer.print(COUNT);
    }

}
