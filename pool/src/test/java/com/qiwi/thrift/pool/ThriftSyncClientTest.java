package com.qiwi.thrift.pool;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ThriftSyncClientTest {

    private DemoServer.Iface iface;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        GenericObjectPool<ThriftClientSyncContainer<DemoServer.Iface>> pool = mock(GenericObjectPool.class);
        Supplier<ThriftClientAddress> addressSupplier = () -> new ThriftClientAddress("test", 123);

        ThriftSyncClient<DemoServer.Iface, DemoServer.Client> handler = new ThriftSyncClient<>(
                DemoServer.Iface.class,
                "DemoServer",
                pool,
                addressSupplier,
                new ThriftClientConfig.Builder().setAddress(new ThriftClientAddress("test", 1243)).build(),
                client -> true,
                mock(ThriftSyncClient.PoolObjectFactory.class)
        );
        iface = ThriftSyncClient.makeProxy(DemoServer.Iface.class, handler);
    }

    @Test(groups = "unit")
    public void testToString() throws Exception {
        assertTrue(iface.toString().contains("DemoServer"));
        assertTrue(iface.toString().contains("test:123"));
    }

    @Test(groups = "unit")
    public void testEquals() throws Exception {
        assertTrue (iface.equals(iface));
        assertFalse(iface.equals(this));
    }
}
