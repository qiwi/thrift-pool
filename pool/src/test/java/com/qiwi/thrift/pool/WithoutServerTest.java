package com.qiwi.thrift.pool;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.reflect.ReflectConfigurator;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;

public class WithoutServerTest {
    private static final Logger log = LoggerFactory.getLogger(WithoutServerTest.class);

    private DemoServer.Iface client;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        // -----=====Создание клиента=====-----
        SyncClientFactory poolFactory = new SyncClientFactory(new ReflectConfigurator());
        client = poolFactory.create(
                DemoServer.Iface.class,
                new ThriftClientConfig.Builder()
                        .setAddress(ThriftClientAddress.parse("127.0.0.1:9090"))
                        .build()
        );
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        ((Closeable)client).close();
    }


    @Test(groups = "unit")
    public void withoutSync() throws Exception {
        client.toString();
        client.equals("test");
        client.hashCode();
    }

}
