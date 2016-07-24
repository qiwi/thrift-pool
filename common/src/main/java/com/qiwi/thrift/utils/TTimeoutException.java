package com.qiwi.thrift.utils;

import org.apache.thrift.TException;


/**
 * Превышено время ожидания коннекта до сервера: сервер очень тормозит
 */
public class TTimeoutException extends TException {
    public TTimeoutException() {
    }

    public TTimeoutException(String message) {
        super(message);
    }

    public TTimeoutException(Throwable cause) {
        super(cause);
    }

    public TTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
