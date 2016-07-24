package com.qiwi.thrift.consul;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.tracing.ThriftLogContext;
import org.apache.thrift.TException;
import org.springframework.context.annotation.Profile;

import javax.inject.Named;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;

@Named
@Profile("server")
public class DemoServerImp implements DemoServer.Iface {

    @Override
    public boolean healthCheck() throws TException {
        return true;
    }

    @Override
    public MapResult request(String text, long id) throws TException {
        return new MapResult(Status.OK);
    }

    @Override
    public MapResult requestWithError(String text, long id) throws TestException, TException {
        return null;
    }

    @Override
    public void requestFullAsync(long requestId, String text, long id) throws TException {

    }

    @Override
    public MapResult responseFullAsync(long requestId) throws TException {
        return null;
    }

    @Override
    public MapResult crash(ByteBuffer trash, String text) throws TException {
        return null;
    }

    @Override
    public MapResult requestWithMap(Map<String, Long> data, long id) throws TException {
        return null;
    }

    @Override
    public Status loadTest() throws TestException, TException {
        OptionalLong parentSpanId = ThriftLogContext.getParentSpanId();
        if (parentSpanId.isPresent() && parentSpanId.getAsLong() == 0xCAFE && ThriftLogContext.getTraceId() == 0xBEEF) {
            return Status.OK;
        } else {
            throw new TestException("No traces");
        }
    }


}
