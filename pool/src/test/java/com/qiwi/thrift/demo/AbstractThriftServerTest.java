package com.qiwi.thrift.demo;

import com.qiwi.thrift.pool.SecondServerImp;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.server.AbstractThriftServer;
import com.qiwi.thrift.server.ThriftEndpointConfig;
import com.qiwi.thrift.test.TestUtils;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class AbstractThriftServerTest {
    private AbstractThriftServer thriftServer;
    protected Properties config;
    protected int thriftPort;
    protected ThriftRequestReporter serverReporter;
    protected boolean traceMocks = false;


    @BeforeMethod(groups = "unit")
    public void startServer() throws Exception {
        thriftPort = TestUtils.genThriftPort();
        config = new Properties();
        config.put("thrift.test.client.address", "127.0.0.1:" + thriftPort);
        if (traceMocks) {
            serverReporter = mock(ThriftRequestReporter.class, withSettings().verboseLogging());
        } else {
            serverReporter = mock(ThriftRequestReporter.class);
        }
        thriftServer = startServer(
                thriftPort,
                new ThriftEndpointConfig.Builder()
                        .addEndpoint(DemoServer.Iface.class,  new ServerImp())
                        .addEndpoint(SecondServer.Iface.class,  new SecondServerImp())
                        .build(),
                serverReporter
        );
    }


    @AfterMethod(groups = "unit")
    public void stopServer() throws Exception {
         thriftServer.stopServer();
    }

    /**
     * This code imitate Dependency Injection framework
     * Best create different child class for every server.
     * @param thriftPort
     * @param endpointConfig
     * @param <I>
     * @return
     */
    public static <I> DemoThriftServer startServer(int thriftPort, ThriftEndpointConfig endpointConfig, ThriftRequestReporter requestReporter){
        Properties serverConfig = new Properties();
        serverConfig.put("thrift.test.server.port", "" + thriftPort);
        serverConfig.put("thrift.test.server.pool_size", "200");

        DemoThriftServer thriftServer = new DemoThriftServer(serverConfig, requestReporter) {
            @Override
            public ThriftEndpointConfig createEndpointConfig() {
                return endpointConfig;
            }
        };
        thriftServer.startServer();
        thriftServer.awaitStart();
        return thriftServer;
    }


}
