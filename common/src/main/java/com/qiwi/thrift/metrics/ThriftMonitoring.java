package com.qiwi.thrift.metrics;

import com.qiwi.thrift.utils.ThriftClientAddress;
import org.apache.thrift.async.TAsyncMethodCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ThriftMonitoring {
    private static final Logger log = LoggerFactory.getLogger(ThriftMonitoring.class);
    public static final String POOL_JMX_BASE = "thrift:type=" + ThriftCallType.ASYNC_CLIENT + ",server=";
    public static final String METHOD_CLASS_SUFFIX = "_call";

    private static final ConcurrentMap<Class<? extends TAsyncMethodCall>, String> methodNameCache = new ConcurrentHashMap<>();
    private static volatile ThriftMonitor thriftMonitor;


    static {
        try {
            thriftMonitor = new ThriftMonitorImp();
        } catch (Throwable ex) {
            disable();
            log.error("Unable to start thrift metric", ex);
        }
    }

    public static ThriftMonitor getMonitor(){
        return thriftMonitor;
    }

    public static synchronized void setThriftMonitor(ThriftMonitor thriftMonitor) {
        ThriftMonitor thriftMonitorOld = ThriftMonitoring.thriftMonitor;
        ThriftMonitoring.thriftMonitor = thriftMonitor;
        if (thriftMonitorOld != null && thriftMonitorOld != thriftMonitor) {
            try {
                thriftMonitorOld.close();
            } catch (Throwable ex) {
            }
        }
    }

    public static void disable(){
        setThriftMonitor(new ThriftMonitorStub());
    }

    public static boolean isDisabled(){
        return thriftMonitor instanceof ThriftMonitorStub;
    }

    public static String formatPoolJmxPrefix(
            String thriftServiceName,
            ThriftClientAddress address,
            ThriftCallType callType
    ) {
        return thriftServiceName + ",address=" + ObjectName.quote(address.toEscapedString()) + ",class=pool,pool=" + callType.name() + "Pool";
    }

    public static String getMethodName(TAsyncMethodCall methodCall){
        return methodNameCache.computeIfAbsent(methodCall.getClass(), clazz -> {
            String simpleName = clazz.getSimpleName();
            if (simpleName.endsWith(METHOD_CLASS_SUFFIX) && simpleName.length() > METHOD_CLASS_SUFFIX.length()) {
                return simpleName.substring(0, simpleName.length() - METHOD_CLASS_SUFFIX.length());
            } else {
                return simpleName;
            }
        });
    }

    public static String escapeDots(String str){
        return str.replace('.', '_');
    }
}
