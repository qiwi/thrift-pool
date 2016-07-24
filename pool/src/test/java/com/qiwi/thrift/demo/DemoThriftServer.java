package com.qiwi.thrift.demo;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.server.AbstractThriftServer;
import com.qiwi.thrift.server.ThriftEndpointConfig;
import com.qiwi.thrift.server.ThriftServerConfig;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ParameterSource;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Properties;

@Singleton
@Named
public class DemoThriftServer extends AbstractThriftServer {
     private ThriftServerConfig config;

    @Inject
    public DemoThriftServer(Properties serverConfig, ThriftRequestReporter requestReporter) {
        config = new ThriftServerConfig.Builder()
                .fromParameters(ParameterSource.subpath(
                        "thrift.test.server.",
                        (path, defaultValue) -> (String) serverConfig.getOrDefault(path, defaultValue)
                ))
                .setRequestReporter(requestReporter)
                .build();
    }

    @Override
    public ThriftServerConfig createConfig() {
        return config;
    }

    @Override
    public ThriftEndpointConfig createEndpointConfig() {
        return new ThriftEndpointConfig.Builder()
                .addEndpoint(DemoServer.Iface.class, new ServerImp())
                .build();
    }
}
