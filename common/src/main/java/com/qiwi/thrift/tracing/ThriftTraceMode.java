package com.qiwi.thrift.tracing;

import com.qiwi.thrift.utils.ParameterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public enum ThriftTraceMode {
    BASIC,
    DISABLED,
    ;

    private static final Logger log = LoggerFactory.getLogger(ThriftTraceMode.class);
    public static Optional<ThriftTraceMode> parse(ParameterSource source, String name, Optional<ThriftTraceMode> defaultValue) {
        String val = source.getString(name, ParameterSource.NOT_DEFINED);
        if (ParameterSource.NOT_DEFINED.equals(val)) {
            return defaultValue;
        }
        try {
            return Optional.of(valueOf(val));
        } catch (IllegalArgumentException ex) {
            log.error("Unable to parse parameter {} with value {}", source.getFullPath(name), val, ex);
            return defaultValue;
        }
    }

    public static Optional<ThriftTraceMode> parse(String value) {
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(value));
        } catch (IllegalArgumentException ex) {
            log.error("Unable to parse value {}", value, ex);
            return Optional.empty();
        }
    }
}
