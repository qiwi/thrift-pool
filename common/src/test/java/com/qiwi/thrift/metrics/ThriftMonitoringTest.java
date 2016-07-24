package com.qiwi.thrift.metrics;

import com.qiwi.thrift.pool.server.DemoServer;
import org.apache.thrift.async.TAsyncClient;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MagicNumber")
public class ThriftMonitoringTest {
    @Test(groups = "unit")
    public void getMethodName() throws Exception {
        String name = ThriftMonitoring.getMethodName(new DemoServer.AsyncClient.responseFullAsync_call(
                0,
                null,
                mock(TAsyncClient.class),
                null,
                null
        ));
        assertEquals(name, "responseFullAsync");
    }

    @Test(groups = "unit")
    public void escapeDots() throws Exception {
        assertEquals(ThriftMonitoring.escapeDots("test.package.class"), "test_package_class");
    }

}
