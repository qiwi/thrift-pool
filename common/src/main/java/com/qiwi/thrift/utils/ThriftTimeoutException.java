package com.qiwi.thrift.utils;


public class ThriftTimeoutException extends ThriftConnectionException {
    public ThriftTimeoutException(Throwable cause) {
        super(cause);
    }

    public ThriftTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftTimeoutException(String message) {
        super(message);
    }
}
