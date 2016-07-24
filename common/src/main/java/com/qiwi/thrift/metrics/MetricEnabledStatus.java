package com.qiwi.thrift.metrics;

public enum MetricEnabledStatus {
    ENABLED,
    DISABLED,

    ;
    public final boolean isEnabled() {
        return this == ENABLED;
    }
}
