package com.qiwi.thrift.pool.imp;

import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

public class MultiplexProtocolFactory implements TProtocolFactory {
    private final TProtocolFactory parentProtocol;
    private final String serviceName;

    public MultiplexProtocolFactory(TProtocolFactory parentProtocol, String serviceName) {
        this.parentProtocol = parentProtocol;
        this.serviceName = serviceName;
    }

    @Override
    public TProtocol getProtocol(TTransport trans) {
        return new TMultiplexedProtocol(parentProtocol.getProtocol(trans), serviceName);
    }
}
