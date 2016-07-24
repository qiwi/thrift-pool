package com.qiwi.thrift.demo;

import com.qiwi.thrift.pool.AsyncClientFactory;
import com.qiwi.thrift.pool.ThriftAsyncClient;
import com.qiwi.thrift.pool.ThriftClientConfig;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.test.Timer;
import com.qiwi.thrift.utils.LambdaUtils;
import com.qiwi.thrift.utils.ParameterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class AsyncDemoTest extends AbstractThriftServerTest {
    private static final Logger log = LoggerFactory.getLogger(AsyncDemoTest.class);
    public static final int PARALLELS = 256;

    private ThriftAsyncClient<DemoServer.AsyncIface> client;
    private ParameterSource parameterSource;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        // -----=====Создание клиента=====-----
        AsyncClientFactory poolFactory = new AsyncClientFactory(new ReflectConfigurator());
        config.put("thrift.test.client.connect_timeout_millis", "5000");
        parameterSource = ParameterSource.subpath(
                "thrift.test.client.",
                (name, defaultValue) -> config.containsKey(name) ? config.getProperty(name) : defaultValue
        );
        client = poolFactory.create(
                DemoServer.AsyncIface.class,
                new ThriftClientConfig.Builder()
                        .setRequestTimeout(Duration.ofSeconds(5))
                        .fromParameters(parameterSource)
                        .build()
        );
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        client.close();
    }

    @Test(groups = "unit")
    public void asyncTest() throws Exception {
        CompletableFuture<MapResult> future = client.execAsync(
                MapResult.class,
                (client, callback) -> client.request("test", 42, callback)
        );
        MapResult result = future.get();
        assertEquals(result.getStatus(), Status.OK);
    }

    @Test(groups = "unit")
    public void asyncMultiplexTest() throws Exception {
        AsyncClientFactory poolFactory = new AsyncClientFactory(new ReflectConfigurator());
        ThriftAsyncClient<SecondServer.AsyncIface> secondClient = poolFactory.create(
                SecondServer.AsyncIface.class,
                new ThriftClientConfig.Builder()
                        .fromParameters(parameterSource)
                        .build()
        );


        CompletableFuture<MapResult> future = client.execAsync(
                MapResult.class,
                (client, callback) -> client.request("test", 42, callback)
        );
        MapResult result = future.get();
        assertEquals(result.getStatus(), Status.OK);
        CompletableFuture<String> future2 = secondClient.execAsync(
                String.class,
                (client, callback) -> client.request("test", 42, callback)
        );
        assertEquals(future2.get(), "test:42");
    }

    @Test(groups = "unit")
    public void asyncLoadTest() throws Exception {
        final int COUNT = 10_000;
        Timer timer = new Timer();
        Semaphore semaphore = new Semaphore(PARALLELS);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        for (int i = 0; i < COUNT && exception.get() == null; i++) {
            long id = i;
            semaphore.acquire();
            Timer.MeasurementLatency latency = timer.startLatency();

            // -----=====Вызов клиента=====----
            // Тип возвращаемого значения нужно посмотреть вручную
            // Будет исправлено в thrift 0.10.0
            CompletableFuture<MapResult> future = client.execAsync(
                    MapResult.class,
                    (client, callback) -> client.request("test", id, callback)
            );
            future.thenAccept(t -> {
                latency.end();
                semaphore.release();
            });
            future.exceptionally(e -> {
                exception.set(e);
                semaphore.release();
                return null;
            });

            future.thenAccept(LambdaUtils.unchecked(result -> {
                    Status status = result.getStatus();
                    if (status != Status.OK) {
                        System.err.println("Invalid status");
                    }
            }));
        }
        if (exception.get() != null) {
            fail("Test completed with error", exception.get());
        }
        if (!semaphore.tryAcquire(PARALLELS, 60, TimeUnit.SECONDS)) {
            fail("Unable to complete test", exception.get());
        }
        if (exception.get() != null) {
            fail("Test completed with error", exception.get());
        }

        timer.print(COUNT);
    }

    @Test(groups = "unit")
    public void asyncThrowTest() throws Exception {
        final int COUNT = 1000;
        Timer timer = new Timer();
        Semaphore semaphore = new Semaphore(PARALLELS);
        AtomicBoolean failFlag = new AtomicBoolean(false);
        for (int i = 0; i < COUNT; i++) {
            long id = i;
            semaphore.acquire();
            Timer.MeasurementLatency latency = timer.startLatency();

            // -----=====Вызов клиента=====----
            // Тип возвращаемого значения нужно посмотреть вручную
            // Будет исправлено в thrift 0.10.0
            CompletableFuture<MapResult> future = client.execAsync(
                    MapResult.class,
                    (client, callback) -> client.requestWithError("test", id, callback)
            );
            future.whenCompleteAsync((t, e) -> {
                latency.end();
                semaphore.release();
                if (e ==null) {
                    failFlag.set(true);
                    new Throwable("Method completed without exception").printStackTrace();
                } else if (!(e instanceof TestException)) {
                    failFlag.set(true);
                    System.out.println("Invalid exception type");
                    e.printStackTrace();
                }
            });

        }
        if (failFlag.get()) {
            fail("Test failed");
        }
        semaphore.acquire(PARALLELS);

        timer.startLatency();
    }
}
