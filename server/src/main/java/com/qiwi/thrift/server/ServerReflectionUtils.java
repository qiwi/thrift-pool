package com.qiwi.thrift.server;

import com.qiwi.thrift.utils.LambdaUtils;
import com.qiwi.thrift.utils.ThriftRuntimeException;
import com.qiwi.thrift.utils.ThriftUtils;
import org.apache.thrift.TProcessor;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.function.Function;

class ServerReflectionUtils {
    static <I> Function<I, TProcessor> getProcessorFactory(Class<I> interfaceClass, Class<?> serviceClass) {
        try {
            Class<?> processorClass = ThriftUtils.getClassByName(serviceClass, "Processor");
            MethodHandle constructor = MethodHandles.lookup().findConstructor(
                    processorClass,
                    MethodType.methodType(void.class, interfaceClass)
            );
            return LambdaUtils.uncheckedF(serverImp -> (TProcessor) constructor.invoke(serverImp));
        } catch (Exception e) {
            throw new ThriftRuntimeException(
                    "Unable to find processor to serviceClass " + serviceClass.getName() + " and interface "
                            + interfaceClass.getName(), e);
        }
    }
}
