package com.qiwi.thrift.utils;

public class ThriftTooLongMessageException extends ThriftConnectionException {
    public ThriftTooLongMessageException(String message) {
        super(message);
    }

    public ThriftTooLongMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
