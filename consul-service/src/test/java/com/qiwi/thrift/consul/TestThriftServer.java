package com.qiwi.thrift.consul;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.server.AbstractThriftServer;
import com.qiwi.thrift.server.ThriftEndpoint;
import com.qiwi.thrift.server.ThriftEndpointConfig;
import com.qiwi.thrift.server.ThriftServerConfig;
import com.qiwi.thrift.test.TestUtils;
import org.springframework.context.annotation.Profile;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.Optional;

@Named
@Profile("server")
public class TestThriftServer extends AbstractThriftServer {
    public final DemoServerImp demoServerImp;
    public final SecondServerImp secondServerImp;
    public final SecondServerImp2 secondServerImp2;

    @Inject
    public TestThriftServer(DemoServerImp serverImp, SecondServerImp secondServerImp, SecondServerImp2 secondServerImp2) {
        this.demoServerImp = serverImp;
        this.secondServerImp = secondServerImp;
        this.secondServerImp2 = secondServerImp2;
    }

    @Override
    public ThriftEndpointConfig createEndpointConfig() {
        ThriftEndpointConfig config = new ThriftEndpointConfig.Builder()
                .addEndpoint(DemoServer.Iface.class, demoServerImp)
                .addEndpoint(
                        new ThriftEndpoint.Builder<>(SecondServer.Iface.class, secondServerImp)
                                .setClusterName(ThriftConsulDemoTest.clusterUUID)
                                .addTag("scope", "test")
                                .build()
                )
                .addEndpoint(
                        new ThriftEndpoint.Builder<>(SecondServer.Iface.class, secondServerImp2)
                                .setClusterName(ThriftConsulDemoTest.clusterUUID)
                                .setSubServiceName(Optional.of("imp2"))
                                .addTag("scope", "test")
                                .build()
                )
                .build();
        return config;
    }

    @Override
    public ThriftServerConfig createConfig() {
        int serverThriftPort = TestUtils.genThriftPort();
        System.out.println("Server port: " + serverThriftPort);
        return new ThriftServerConfig.Builder()
                .setServerThriftPort(serverThriftPort)
                .build();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
