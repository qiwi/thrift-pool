package com.qiwi.thrift.serialize;

import com.qiwi.thrift.utils.ThriftRuntimeException;

public class ThriftSerializeException extends ThriftRuntimeException {
    public ThriftSerializeException(String message) {
        super(message);
    }

    public ThriftSerializeException(String message, Throwable cause) {
        super(message, cause);
    }
}
