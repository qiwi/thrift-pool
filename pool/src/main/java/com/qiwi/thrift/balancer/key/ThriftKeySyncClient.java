package com.qiwi.thrift.balancer.key;

import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.util.stream.Stream;

public interface ThriftKeySyncClient<K, I> extends Closeable {

    I get() throws TTransportException;

    I get(K key) throws TTransportException;

    Stream<I> getQuorum(K key) throws TTransportException;

}
