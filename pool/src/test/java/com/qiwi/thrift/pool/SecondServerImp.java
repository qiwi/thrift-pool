package com.qiwi.thrift.pool;

import com.qiwi.thrift.pool.server.SecondServer;
import org.apache.thrift.TException;

public class SecondServerImp implements SecondServer.Iface {

    @Override
    public void test() throws TException {
    }

    @Override
    public String request(String text, long id) throws TException {
        return text + ":" + id;
    }
}
