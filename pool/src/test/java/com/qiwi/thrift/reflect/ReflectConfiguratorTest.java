package com.qiwi.thrift.reflect;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.utils.LambdaUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("MagicNumber")
public class ReflectConfiguratorTest {
    private static final Logger log = LoggerFactory.getLogger(LambdaUtils.class);

    private ReflectConfigurator configurator;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        configurator = new ReflectConfigurator();
    }

    @Test(groups = "unit")
    public void createSync() throws Exception {
        SyncClientClassInfo<DemoServer.Iface, DemoServer.Client> info =
                configurator.createSync(DemoServer.Iface.class);
        assertEquals(info.getClientInterfaceClazz(), DemoServer.Iface.class);
        assertEquals(info.getClientFactory().getClass(), DemoServer.Client.Factory.class);

        DemoServer.Iface mock = mock(DemoServer.Iface.class);
        when(mock.healthCheck()).thenReturn(true);
        assertTrue(info.getValidator().get().test(mock));
        verify(mock, times(1)).healthCheck();

        SyncClientClassInfo<SecondServer.Iface, SecondServer.Client> info2 =
                configurator.createSync(SecondServer.Iface.class);
        SecondServer.Iface mock2 = mock(SecondServer.Iface.class);
        assertTrue(info2.getValidator().get().test(mock2));
        verify(mock2, times(1)).test();
    }

    @Test(groups = "unit")
    public void createAsync() throws Exception {
        TProtocolFactory protocolFactory = mock(TProtocolFactory.class);
        TAsyncClientManager clientManager = mock(TAsyncClientManager.class);
        TNonblockingTransport transport = mock(TNonblockingTransport.class);

        AsyncClientClassInfo<DemoServer.AsyncIface, DemoServer.AsyncClient> info = configurator.createAsync(DemoServer.AsyncIface.class);
        assertEquals(info.getClientInterfaceClazz(), DemoServer.AsyncIface.class);
        assertEquals(
                info.getClientFactory().apply(clientManager, protocolFactory)
                        .getAsyncClient(transport).getClass(),
                DemoServer.AsyncClient.class
        );

        DemoServer.AsyncClient mock = mock(DemoServer.AsyncClient.class);
        doAnswer(invocation -> {
            DemoServer.AsyncClient.healthCheck_call call = new DemoServer.AsyncClient.healthCheck_call(
                    (AsyncMethodCallback) invocation.getArguments()[0],
                    mock,
                    protocolFactory,
                    transport
            ) {
                    @Override
                    public boolean getResult() throws TException {
                        return true;
                    }
            };
            ((AsyncMethodCallback)invocation.getArguments()[0]).onComplete(call);
            return null;
        })
                .when(mock).healthCheck(any());
        assertTrue(info.getValidator().get().test(mock));
        verify(mock, times(1)).healthCheck(any());

        AsyncClientClassInfo<SecondServer.AsyncIface, SecondServer.AsyncClient> info2 = configurator.createAsync(SecondServer.AsyncIface.class);
        SecondServer.AsyncClient mock2 = mock(SecondServer.AsyncClient.class);
        doAnswer(invocation -> {
            SecondServer.AsyncClient.test_call call = new SecondServer.AsyncClient.test_call(
                    (AsyncMethodCallback) invocation.getArguments()[0],
                    mock,
                    protocolFactory,
                    transport
            ) {
                    @Override
                    public void getResult() throws TException {
                    }
            };
            ((AsyncMethodCallback)invocation.getArguments()[0]).onComplete(call);
            return null;
        })
                .when(mock2).test(any());
        assertTrue(info2.getValidator().get().test(mock2));
        verify(mock2, times(1)).test(any());
    }

}
