package com.qiwi.thrift.utils;


public class ThriftConnectionException extends ThriftRuntimeException {
    public ThriftConnectionException(Throwable cause) {
        super(cause);
    }

    public ThriftConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftConnectionException(String message) {
        super(message);
    }
}
