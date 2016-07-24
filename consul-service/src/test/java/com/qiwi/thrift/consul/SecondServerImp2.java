package com.qiwi.thrift.consul;

import com.qiwi.thrift.pool.server.SecondServer;
import org.apache.thrift.TException;
import org.springframework.context.annotation.Profile;

import javax.inject.Named;
import java.util.concurrent.atomic.AtomicLong;

@Named
@Profile("server")
public class SecondServerImp2 implements SecondServer.Iface {
    private final AtomicLong requestCount = new AtomicLong();

    public long getRequestCount() {
        return requestCount.get();
    }

    @Override
    public void test() throws TException {
    }

    @Override
    public String request(String text, long id) throws TException {
        requestCount.incrementAndGet();
        return text + "_" + id;
    }
}
