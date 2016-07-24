package com.qiwi.thrift.metrics;

import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;

import java.util.List;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

class ThriftMonitorStub implements ThriftMonitor {
    @Override
    public MetricEnabledStatus getEnableStatus() {
        return MetricEnabledStatus.DISABLED;
    }

    @Override
    public void logMethodCall(
            ThriftCallType type,
            ThriftClientAddress address,
            String serviceName,
            String method,
            long nanos,
            ThriftRequestStatus requestStatus
    ) {

    }

    @Override
    public boolean registerPool(
            String serviceName,
            DoubleSupplier load,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            LongSupplier failNodes,
            Supplier<List<ThriftClientAddress>> failNodeList
    ) {
        return false;
    }

    @Override
    public void unRegisterPool(
            String serviceName
    ) {

    }

    @Override
    public void registerDc(
            String serviceName,
            String dcName,
            DoubleSupplier load,
            DoubleSupplier weight,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            LongSupplier failNodes,
            Supplier<String> status
    ) {

    }

    @Override
    public void unRegisterDc(
            String serviceName,
            String dcName
    ) {

    }

    @Override
    public void registerNode(
            String serviceName,
            ThriftClientAddress address,
            DoubleSupplier load,
            DoubleSupplier weight,
            LongSupplier usedConnections,
            LongSupplier openConnections,
            Supplier<String> status,
            LongSupplier statusId,
            DoubleSupplier latency
    ) {

    }

    @Override
    public void unRegisterNode(
            String serviceName,
            ThriftClientAddress address
    ) {

    }

    @Override
    public void connectToGraphite(ThriftGraphiteConfig config) {

    }

    @Override
    public void close() {

    }
}
