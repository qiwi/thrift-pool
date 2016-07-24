package com.qiwi.thrift.pool;


import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ThriftApplicationException;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingTransport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class ThriftClientAsyncContainerTest {
    private ThriftClientAsyncContainer<DemoServer.AsyncClient> client;
    private ObjectPool<ThriftClientAsyncContainer<DemoServer.AsyncClient>> objectPool;
    private DemoServer.AsyncClient asyncClient;
    private TNonblockingTransport transport;
    private AtomicReference<AsyncMethodCallback> callbackRes;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        objectPool = mock(ObjectPool.class);
        asyncClient = mock(DemoServer.AsyncClient.class);
        transport = mock(TNonblockingTransport.class);
        client = new ThriftClientAsyncContainer<>(
                transport,
                objectPool,
                (ex) -> !(ex instanceof TestException),
                new ThriftClientAddress("lh", 42),
                "testSrv",
                asyncClient,
                ThriftCallType.ASYNC_CLIENT
        );
        callbackRes = new AtomicReference<>();
    }

    @Test(groups = "unit")
    public void execAsync() throws Exception {
        CompletableFuture<MapResult> future = client.execAsync(MapResult.class, (client, callback) -> callbackRes.set(callback), ThriftRequestReporter.NULL_REPORTER);
        assertSame(callbackRes.get(), future);

        DemoServer.AsyncClient.responseFullAsync_call call = mock(DemoServer.AsyncClient.responseFullAsync_call.class);
        MapResult mapResult = new MapResult();
        when(call.getResult()).thenReturn(mapResult);
        callbackRes.get().onComplete(call);

        assertEquals(future.getNow(null), mapResult);
        verify(objectPool).returnObject(client);

        ThriftRequestReporter timeReporter = mock(ThriftRequestReporter.class);
        CompletableFuture<MapResult> future2 = client.execAsync(
                MapResult.class,
                (client, callback) -> callbackRes.set(callback),
                timeReporter
        );
        assertSame(callbackRes.get(), future2);
        callbackRes.get().onComplete(mock(DemoServer.AsyncClient.responseFullAsync_call.class));
        verify(objectPool, times(2)).returnObject(client);
        verify(timeReporter, times(1)).requestEnd(
                eq("testSrv"),
                contains("responseFullAsync"),
                eq(ThriftCallType.ASYNC_CLIENT),
                eq(ThriftRequestStatus.SUCCESS),
                anyLong(),
                eq(Optional.empty())
        );
    }

    @Test(groups = "unit")
    public void execError() throws Exception {
        ThriftRequestReporter timeReporter = mock(ThriftRequestReporter.class);
        AtomicReference<AsyncMethodCallback> callbackRes = new AtomicReference<>();
        CompletableFuture<MapResult> future = client.execAsync(MapResult.class, (client, callback) -> callbackRes.set(callback), timeReporter);

        callbackRes.get().onError(new TException());

        assertTrue(future.isCompletedExceptionally());
        verify(objectPool).invalidateObject(client);
    }

    @Test(groups = "unit")
    public void execAppException() throws Exception {
        ThriftRequestReporter timeReporter = mock(ThriftRequestReporter.class);
        CompletableFuture<MapResult> future = client.execAsync(MapResult.class, (client, callback) -> callbackRes.set(callback), timeReporter);

        DemoServer.AsyncClient.responseFullAsync_call call = mock(DemoServer.AsyncClient.responseFullAsync_call.class);
        TestException exception = new TestException();
        when(call.getResult()).thenThrow(exception);
        callbackRes.get().onComplete(call);

        assertTrue(future.isCompletedExceptionally());
        verify(objectPool).returnObject(client);
        verify(timeReporter, times(1)).requestEnd(
                eq("testSrv"),
                anyString(),
                eq(ThriftCallType.ASYNC_CLIENT),
                eq(ThriftRequestStatus.APP_ERROR),
                anyLong(),
                eq(Optional.of(exception))
        );
    }

    @Test(groups = "unit")
    public void execUnexpectedException() throws Exception {
        ThriftRequestReporter timeReporter = mock(ThriftRequestReporter.class);
        CompletableFuture<MapResult> future = client.execAsync(MapResult.class, (client, callback) -> callbackRes.set(callback), timeReporter);

        DemoServer.AsyncClient.responseFullAsync_call call = mock(DemoServer.AsyncClient.responseFullAsync_call.class);
        ThriftApplicationException exception = new ThriftApplicationException("test");
        when(call.getResult()).thenThrow(exception);
        callbackRes.get().onComplete(call);

        assertTrue(future.isCompletedExceptionally());
        verify(objectPool).returnObject(client);
        verify(timeReporter, times(1)).requestEnd(
                eq("testSrv"),
                anyString(),
                eq(ThriftCallType.ASYNC_CLIENT),
                eq(ThriftRequestStatus.UNEXPECTED_ERROR),
                anyLong(),
                eq(Optional.of(exception))
        );
    }

    @Test(groups = "unit")
    public void close() throws Exception {
        AtomicReference<AsyncMethodCallback> callbackRes = new AtomicReference<>();
        CompletableFuture<MapResult> future = client.execAsync(MapResult.class, (client, callback) -> callbackRes.set(callback), ThriftRequestReporter.NULL_REPORTER);

        client.closeClient();

        verify(transport).close();
        assertTrue(future.isCompletedExceptionally());
    }

}
