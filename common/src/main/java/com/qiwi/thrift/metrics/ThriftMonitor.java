package com.qiwi.thrift.metrics;


import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;

import java.io.Closeable;
import java.util.List;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public interface ThriftMonitor extends Closeable{
    MetricEnabledStatus getEnableStatus();

    void logMethodCall(
            ThriftCallType type,
            ThriftClientAddress address,
            String serviceName,
            String method,
            long nanos,
            ThriftRequestStatus requestStatus
    );

    boolean registerPool(
            String serviceName,
            DoubleSupplier load,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            LongSupplier failNodes,
            Supplier<List<ThriftClientAddress>> failNodeList
    );

    void unRegisterPool(
        String serviceName
    );

    void registerDc(
            String serviceName,
            String dcName,
            DoubleSupplier load,
            DoubleSupplier weight,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            LongSupplier failNodes,
            Supplier<String> status
    );

    void unRegisterDc(
            String serviceName,
            String dcName
    );

    void registerNode(
            String serviceName,
            ThriftClientAddress address,
            DoubleSupplier load,
            DoubleSupplier weight,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            Supplier<String> status,
            LongSupplier statusId,
            DoubleSupplier latencyMs
    );

    void unRegisterNode(
            String serviceName,
            ThriftClientAddress address
    );

    void connectToGraphite(ThriftGraphiteConfig config);

    void close();


}
