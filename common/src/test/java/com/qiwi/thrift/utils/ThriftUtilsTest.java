package com.qiwi.thrift.utils;

import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class ThriftUtilsTest {
    @Test(groups = "unit")
    public void getThriftRootClass() throws Exception {
        assertEquals(ThriftUtils.getThriftRootClass(SecondServer.Iface.class), SecondServer.class);
        assertEquals(ThriftUtils.getThriftRootClass(SecondServer.class), SecondServer.class);
    }

    @Test(groups = "unit", expectedExceptions = ThriftRuntimeException.class)
    public void getThriftRootClassErr() throws Exception {
        ThriftUtils.getThriftRootClass(MapResult.class);
    }

    @Test(groups = "unit")
    public void getThriftServiceName() throws Exception {
        assertEquals(ThriftUtils.getThriftServiceName(SecondServer.Iface.class, Optional.empty()), "SecondServer");
        assertEquals(ThriftUtils.getThriftServiceName(SecondServer.Iface.class, Optional.of("ms")), "SecondServer__ms");
    }

    @Test(groups = "unit")
    public void getThriftFullServiceName() throws Exception {
        assertEquals(ThriftUtils.getThriftFullServiceName(SecondServer.Iface.class, Optional.empty()), "com_qiwi_thrift_pool_server_SecondServer");
        assertEquals(ThriftUtils.getThriftFullServiceName(SecondServer.Iface.class, Optional.of("ms")), "com_qiwi_thrift_pool_server_SecondServer__ms");
    }

    @Test(groups = "unit")
    public void isApplicationLevelException() throws Exception {
        assertTrue(ThriftUtils.isApplicationLevelException(new TestException()));
        assertTrue(ThriftUtils.isApplicationLevelException(new ThriftApplicationException("test")));
        assertFalse(ThriftUtils.isApplicationLevelException(new ThriftConnectionException("test")));
        assertFalse(ThriftUtils.isApplicationLevelException(new TException("test")));
        assertFalse(ThriftUtils.isApplicationLevelException(new TTransportException("test")));
        assertFalse(ThriftUtils.isApplicationLevelException(new RuntimeException("test")));
    }

    @Test(groups = "unit")
    public void firstSuccess() throws Exception {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();
        CompletableFuture<String> future3 = new CompletableFuture<>();
        CompletableFuture<String> result = ThriftUtils.firstSuccess(Stream.of(future1, future2, future3));
        assertFalse(result.isDone());
        future3.completeExceptionally(new RuntimeException());
        assertFalse(result.isDone());
        future2.complete("Test");
        assertTrue(result.isDone());
        assertEquals(result.join(), "Test");
    }

    @Test(groups = "unit")
    public void firstSuccessOnError() throws Exception {
        CompletableFuture<String> future1 = new CompletableFuture<>();
        CompletableFuture<String> future2 = new CompletableFuture<>();
        CompletableFuture<String> future3 = new CompletableFuture<>();
        CompletableFuture<String> result = ThriftUtils.firstSuccess(Stream.of(future1, future2, future3));
        assertFalse(result.isDone());
        future3.completeExceptionally(new RuntimeException("Test_msg"));
        assertFalse(result.isDone());
        future1.completeExceptionally(new RuntimeException("Test_msg"));
        assertFalse(result.isDone());
        future2.completeExceptionally(new RuntimeException("Test_msg"));
        assertTrue(result.isDone());
        try {
            result.get();
            fail();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getMessage(), "Test_msg");
        }
    }

    @Test(groups = "unit")
    public void empty() throws Exception {
        assertTrue(ThriftUtils.empty(null));
        assertTrue(ThriftUtils.empty(""));
        assertTrue(ThriftUtils.empty("   \t\t"));
        assertFalse(ThriftUtils.empty(" s "));
    }

}
