package com.qiwi.thrift.utils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.Test;

import javax.imageio.IIOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.qiwi.thrift.utils.LambdaUtils.*;
import static org.testng.Assert.assertEquals;

@SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "NewExceptionWithoutArguments"})
public class LambdaUtilsTest {
    @Test(groups = "unit", expectedExceptions = NoSuchFieldError.class)
    public void errorUnchecked() throws Exception {
        Supplier<Object> supplier = LambdaUtils.unchecked(() -> {
            throw new NoSuchFieldError();
        });
        supplier.get();
    }


    @Test(groups = "unit", expectedExceptions = ThriftConnectionException.class)
    public void exceptionUnchecked() throws Exception {
        Supplier<Object> supplier = unchecked(() -> {
            throw new TProtocolException();
        });
        supplier.get();
    }

    @Test(groups = "unit", expectedExceptions = NoSuchFieldError.class)
    public void errorPopagate() throws Exception {
        throw propagate(new NoSuchFieldError());
    }

    @Test(groups = "unit")
    public void propagation() throws Exception {
        assertEquals(getPropagation(new IIOException("")).getClass(), RuntimeException.class);
        assertEquals(getPropagation(new IndexOutOfBoundsException()).getClass(), IndexOutOfBoundsException.class);
        assertEquals(getPropagation(new InvocationTargetException(null)).getClass(), RuntimeException.class);
        assertEquals(getPropagation(new InvocationTargetException(new IndexOutOfBoundsException())).getClass(), IndexOutOfBoundsException.class);
        assertEquals(getPropagation(new ExecutionException(null)).getClass(), RuntimeException.class);
        assertEquals(getPropagation(new ExecutionException(new IndexOutOfBoundsException())).getClass(), IndexOutOfBoundsException.class);
    }

    @Test(groups = "unit")
    public void thriftExceptions() throws Exception {
        assertEquals(getPropagation(new TestEx()).getClass(), ThriftApplicationException.class);
        assertEquals(getPropagation(new TException()).getClass(), ThriftConnectionException.class);

        assertEquals(
                getPropagation(new TProtocolException("Test")).getClass(),
                ThriftConnectionException.class
        );
        assertEquals(
                getPropagation(new TProtocolException(TProtocolException.SIZE_LIMIT, "Test")).getClass(),
                ThriftTooLongMessageException.class
        );

        assertEquals(
                getPropagation(new TTransportException("Test")).getClass(),
                ThriftConnectionException.class
        );
        assertEquals(
                getPropagation(new TTransportException("Frame size (" + 100 + ") larger than max length (" + 10 + ")!")).getClass(),
                ThriftTooLongMessageException.class
        );

    }

    static class TestEx extends TException{

    }
}
