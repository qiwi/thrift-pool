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
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.apache.thrift.transport.TTransportException;
import org.hamcrest.Matcher;
import org.mockito.InOrder;
import org.mockito.internal.matchers.And;
import org.mockito.internal.matchers.GreaterOrEqual;
import org.mockito.internal.matchers.LessOrEqual;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class ThriftSyncClientIntegrationTest extends AbstractThriftServerTest {
    private DemoServer.Iface client;
    private ParameterSource parameterSource;
    private ThriftRequestReporter clientReporter;

    public ThriftSyncClientIntegrationTest() {
         traceMocks = true;
    }

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        // -----=====Создание клиента=====-----
        SyncClientFactory poolFactory = new SyncClientFactory(new ReflectConfigurator());
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
                DemoServer.Iface.class,
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
        ((Closeable)client).close();
    }

    @Test(groups = "unit")
    public void requestReporterTest() throws Exception {
        Matcher<Long> clientTime = new And(Arrays.asList(new GreaterOrEqual<>(100_000L), new LessOrEqual<>(2_000_000_000L)));
        Matcher<Long> serverTime = new And(Arrays.asList(new GreaterOrEqual<>( 10_000L), new LessOrEqual<>(  200_000_000L)));

        ThriftLogContext.setSpanId(0xDEADBEEFL);
        Answer<Void> serverSpawnCheck = invocation -> {
            assertEquals(ThriftLogContext.getParentSpanId().getAsLong(), 0xDEADBEEFL);
            assertEquals(ThriftLogContext.getClientAddress(), "/127.0.0.1");
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

        MapResult result = client.request("test", 7);
        TestException exception;
        try {
            result = client.requestWithError("test", 7);
            exception = null;
            fail();
        } catch (TestException e) {
            exception = e;
            assertTrue(exception.getMessage().contains("available"));
        }
        InOrder order = inOrder(clientReporter, serverReporter);



        order.verify(clientReporter).requestBegin("DemoServer", "request", ThriftCallType.SYNC_CLIENT);

        order.verify(serverReporter).requestBegin("DemoServer", "healthCheck", ThriftCallType.SERVER);
        order.verify(serverReporter).requestEnd(
                eq("DemoServer"),
                eq("healthCheck"),
                eq(ThriftCallType.SERVER),
                eq(ThriftRequestStatus.SUCCESS),
                longThat(serverTime), eq(Optional.empty()));

        order.verify(serverReporter).requestBegin("DemoServer", "request", ThriftCallType.SERVER);
        order.verify(serverReporter).requestEnd(
                eq("DemoServer"),
                eq("request"),
                eq(ThriftCallType.SERVER),
                eq(ThriftRequestStatus.SUCCESS),
                longThat(serverTime),
                eq(Optional.empty()));
        order.verify(clientReporter).requestEnd(
                eq("DemoServer"),
                eq("request"),
                eq(ThriftCallType.SYNC_CLIENT),
                eq(ThriftRequestStatus.SUCCESS),
                longThat(clientTime), eq(Optional.empty()));

        order.verify(clientReporter).requestBegin("DemoServer", "requestWithError", ThriftCallType.SYNC_CLIENT);
        order.verify(serverReporter).requestBegin("DemoServer", "requestWithError", ThriftCallType.SERVER);
        order.verify(serverReporter).requestEnd(eq("DemoServer"),
                eq("requestWithError"),
                eq(ThriftCallType.SERVER),
                eq(ThriftRequestStatus.UNEXPECTED_ERROR),
                longThat(serverTime),
                eq(Optional.of(exception))
        );
        order.verify(clientReporter).requestEnd(
                eq("DemoServer"),
                eq("requestWithError"),
                eq(ThriftCallType.SYNC_CLIENT),
                eq(ThriftRequestStatus.UNEXPECTED_ERROR),
                longThat(clientTime),
                eq(Optional.of(exception)));
        order.verifyNoMoreInteractions();
     }

    @Test(groups = "unit")
    public void nullMapFieldTest() throws Exception {
        try {
            client.requestWithMap(Collections.singletonMap(null, 24L), 12);
            fail();
        } catch (TTransportException ex) {
            // expected
        }
        MapResult result = client.requestWithMap(Collections.singletonMap("test", 24L), 12);
        assertEquals(result.data, Collections.singletonMap("test", 24L));
        assertEquals(result.getStatus(), Status.OK);
    }
}
