package com.qiwi.thrift.pool;

import com.qiwi.thrift.demo.AbstractThriftServerTest;
import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.tracing.ThriftLogContext;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.tracing.ThriftTraceMode;
import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftConnectionException;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.hamcrest.Matcher;
import org.mockito.InOrder;
import org.mockito.internal.matchers.And;
import org.mockito.internal.matchers.GreaterOrEqual;
import org.mockito.internal.matchers.LessOrEqual;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class ThriftAsyncClientIntegrationTest extends AbstractThriftServerTest {
    private ThriftAsyncClient<DemoServer.AsyncIface> client;
    private ParameterSource parameterSource;
    private ThriftRequestReporter clientReporter;

    public ThriftAsyncClientIntegrationTest() {
        traceMocks = true;
    }

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        // -----=====Создание клиента=====-----
        AsyncClientFactory poolFactory = new AsyncClientFactory(new ReflectConfigurator());
        parameterSource = ParameterSource.subpath(
                "thrift.test.client.",
                (name, defaultValue) -> config.containsKey(name) ? config.getProperty(name) : defaultValue
        );
        if (traceMocks) {
            clientReporter = mock(ThriftRequestReporter.class, withSettings().verboseLogging());
        } else {
            clientReporter = mock(ThriftRequestReporter.class);
        }
        client = poolFactory.create(
                DemoServer.AsyncIface.class,
                new ThriftClientConfig.Builder()
                        .fromParameters(parameterSource)
                        .setRequestReporter(clientReporter)
                        .setTraceMode(Optional.of(ThriftTraceMode.BASIC))
                        .setConnectTimeout(Duration.ofSeconds(5))
                        .build()
        );
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        client.close();
    }

    @Test(groups = "unit")
    public void requestReporterTest() throws Exception {
        Matcher<Long> clientTime = new And(Arrays.asList(new GreaterOrEqual<>(100_000L), new LessOrEqual<>(2_000_000_000L)));
        Matcher<Long> serverTime = new And(Arrays.asList(new GreaterOrEqual<>( 10_000L), new LessOrEqual<>(  300_000_000L)));

        ThriftLogContext.setSpanId(0xDEADBEEFL);
        Answer<Void> serverSpawnCheck = invocation -> {
            assertEquals(ThriftLogContext.getParentSpanId().getAsLong(), 0xDEADBEEFL);
            return null;
        };
        doAnswer(serverSpawnCheck).when(serverReporter).requestBegin(anyString(), anyString(), any());
        doAnswer(serverSpawnCheck).when(serverReporter).requestEnd(
                anyString(),
                anyString(),
                any(),
                any(),
                anyLong(),
                any()
        );
        Answer<Void> clientSpawnCheck = invocation -> {
            assertEquals(ThriftLogContext.getSpanId(), 0xDEADBEEFL);
            return null;
        };
        doAnswer(clientSpawnCheck).when(clientReporter).requestBegin(anyString(), anyString(), any());
        doAnswer(clientSpawnCheck).when(clientReporter).requestEnd(
                anyString(),
                anyString(),
                any(),
                any(),
                anyLong(),
                any()
        );

        CompletableFuture<MapResult> future = client.execAsync(
                MapResult.class,
                (client, callback) -> client.request("test", 7, callback)
        );
        MapResult result = future.join();
        assertEquals(result.getStatus(), Status.OK);

        TestException exception;
        try {
            future = client.execAsync(
                    MapResult.class,
                    (client, callback) -> client.requestWithError("test", 7, callback)
            );
            future.join();
            exception = null;
            fail();
        } catch (CompletionException e) {
            exception = (TestException) e.getCause();
            assertTrue(exception.getMessage().contains("available"));
        }

        InOrder order = inOrder(clientReporter, serverReporter);
        order.verify(clientReporter).requestBegin("DemoServer", "async", ThriftCallType.ASYNC_CLIENT);

        order.verify(serverReporter).requestBegin("DemoServer", "healthCheck", ThriftCallType.SERVER);
        order.verify(serverReporter).requestEnd(
                eq("DemoServer"),
                eq("healthCheck"),
                eq(ThriftCallType.SERVER),
                eq(ThriftRequestStatus.SUCCESS),
                longThat(serverTime),
                eq(Optional.empty())
        );

        order.verify(serverReporter).requestBegin("DemoServer", "request", ThriftCallType.SERVER);
        order.verify(serverReporter).requestEnd(
                eq("DemoServer"),
                eq("request"),
                eq(ThriftCallType.SERVER),
                eq(ThriftRequestStatus.SUCCESS),
                longThat(serverTime),
                eq(Optional.empty())
        );
        order.verify(clientReporter).requestEnd(
                eq("DemoServer"),
                eq("request"),
                eq(ThriftCallType.ASYNC_CLIENT),
                eq(ThriftRequestStatus.SUCCESS),
                longThat(clientTime),
                eq(Optional.empty())
        );
        order = inOrder(clientReporter, serverReporter);
        order.verify(clientReporter).requestBegin("DemoServer", "async", ThriftCallType.ASYNC_CLIENT);
        order.verify(serverReporter).requestBegin("DemoServer", "requestWithError", ThriftCallType.SERVER);
        order.verify(serverReporter).requestEnd(
                eq("DemoServer"),
                eq("requestWithError"),
                eq(ThriftCallType.SERVER),
                eq(ThriftRequestStatus.UNEXPECTED_ERROR),
                longThat(serverTime),
                eq(Optional.of(exception))
        );
        order.verify(clientReporter).requestEnd(
                eq("DemoServer"),
                eq("requestWithError"),
                eq(ThriftCallType.ASYNC_CLIENT),
                eq(ThriftRequestStatus.UNEXPECTED_ERROR),
                longThat(clientTime),
                eq(Optional.of(exception))
        );
        order.verifyNoMoreInteractions();
     }

    @Test(groups = "unit")
    public void nullMapFieldTest() throws Exception {
        try {
            CompletableFuture<MapResult> future = client.execAsync(
                    MapResult.class,
                    (client, callback) -> client.requestWithMap(Collections.singletonMap(null, 24L), 12, callback)
            );
            MapResult result = future.join();
            fail();
        } catch (ThriftConnectionException ex) {
            // expected
        }
        CompletableFuture<MapResult> future = client.execAsync(
                MapResult.class,
                (client, callback) -> client.requestWithMap(Collections.singletonMap("test", 24L), 12, callback)
        );
        MapResult result = future.get();
        assertEquals(result.data, Collections.singletonMap("test", 24L));
        assertEquals(result.getStatus(), Status.OK);
    }
}
