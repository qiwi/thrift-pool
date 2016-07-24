package com.qiwi.thrift.server;


/**
 * Recommend create children of AbstractThriftServer - to manage life-cycle via DI Framework
 */
public class ThriftServer extends AbstractThriftServer {
    private final ThriftServerConfig serverConfig;
    private final ThriftEndpointConfig endpointConfig;

    public ThriftServer(ThriftServerConfig serverConfig, ThriftEndpointConfig endpointConfig) {
        this.serverConfig = serverConfig;
        this.endpointConfig = endpointConfig;
    }

    @Override
    public ThriftServerConfig createConfig() {
        return serverConfig;
    }

    @Override
    public ThriftEndpointConfig createEndpointConfig() {
        return endpointConfig;
    }
}
