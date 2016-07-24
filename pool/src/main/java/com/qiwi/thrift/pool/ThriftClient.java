package com.qiwi.thrift.pool;

import java.io.Closeable;

public interface ThriftClient<I> extends Closeable {

    boolean isHealCheckOk();

    @Override
    void close();

    int getUsedConnections();

    int getOpenConnections();

    int getNumWaiters();

    int getMaxConnections();

    void setMaxConnections(int maxConnections);

    String toString();

    void reconfigure(ThriftClientConfig clientConfig);

    void evict();
}
