package com.qiwi.thrift.metrics;

import java.util.EnumSet;

public enum ThriftCallType {
    SYNC_CLIENT("Client", false),
    SYNC_BALANCER("Client", true),
    ASYNC_CLIENT("Client", false),
    ASYNC_BALANCER("Client", true),
    SERVER("Server", false),

    ;
    private static final EnumSet<ThriftCallType> ASYNC_CALL_TYPES = EnumSet.of(ASYNC_BALANCER, ASYNC_CLIENT);
    private static final EnumSet<ThriftCallType> BALANCER_CALL_TYPES = EnumSet.of(SYNC_BALANCER, ASYNC_BALANCER);

    private final String name;
    private final boolean logAllHostStat;

    ThriftCallType(String name, boolean logAllHostStat) {
        this.name = name;
        this.logAllHostStat = logAllHostStat;
    }

    @Override
    public String toString() {
        return name;
    }

    public boolean isLogAllHostStat() {
        return logAllHostStat;
    }

    public boolean isClientCall() {
        return this != SERVER;
    }

    public boolean isAsync() {
        return ASYNC_CALL_TYPES.contains(this);
    }

    public boolean isBalancer() {
        return BALANCER_CALL_TYPES.contains(this);
    }
}
