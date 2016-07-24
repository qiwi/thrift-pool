package com.qiwi.thrift.pool;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import org.apache.thrift.transport.TTransportException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@SuppressWarnings("MagicNumber")
public class MissingServerTest {
    private AnnotationConfigApplicationContext clientContext;
    private DemoServer.Iface demoServer;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        clientContext = startClient();
        demoServer = clientContext.getBean(DemoServer.Iface.class);
    }

    private AnnotationConfigApplicationContext startClient() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.getEnvironment().addActiveProfile("client");
        ctx.register(ClientConfiguration.class);
        ctx.refresh();
        return ctx;
    }

    @Test(groups = "unit")
    public void withoutServer() throws Exception {
        try {
            MapResult result = demoServer.request("test", 42);
            assertEquals(result.getStatus(), Status.OK);// another test run in same time
        } catch (TTransportException e) {
            // expected - no node available
        }


    }

}
