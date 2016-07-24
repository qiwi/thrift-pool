package com.qiwi.thrift.utils;


public class ThriftApplicationException extends ThriftRuntimeException {
    public ThriftApplicationException(Throwable cause) {
        super(cause);
    }

    public ThriftApplicationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftApplicationException(String message) {
        super(message);
    }
}
