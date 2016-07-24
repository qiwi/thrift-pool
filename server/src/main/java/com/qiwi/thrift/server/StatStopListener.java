package com.qiwi.thrift.server;

public interface StatStopListener {

    void onStart(AbstractThriftServer server);

    void onStop(AbstractThriftServer server);
}
