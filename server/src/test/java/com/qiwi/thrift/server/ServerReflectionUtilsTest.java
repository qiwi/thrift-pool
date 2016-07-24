package com.qiwi.thrift.server;

import com.qiwi.thrift.pool.server.DemoServer;
import org.apache.thrift.TProcessor;
import org.testng.annotations.Test;

import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("MagicNumber")
public class ServerReflectionUtilsTest {
    @Test(groups = "unit")
    public void getProcessorFactory() throws Exception {
        Function<DemoServer.Iface, TProcessor> factory = ServerReflectionUtils.getProcessorFactory(DemoServer.Iface.class, DemoServer.class);
        TProcessor processor = factory.apply(mock(DemoServer.Iface.class));
        assertTrue(processor instanceof DemoServer.Processor);
    }

}
