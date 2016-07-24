package com.qiwi.thrift.utils;

public class ThriftRuntimeException extends RuntimeException {
    public ThriftRuntimeException(Throwable cause) {
        super(cause);
    }

    public ThriftRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftRuntimeException(String message) {
        super(message);
    }

}
