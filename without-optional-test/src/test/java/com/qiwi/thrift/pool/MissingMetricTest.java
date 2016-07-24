package com.qiwi.thrift.pool;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class MissingMetricTest {

    @Test(groups = "unit")
    public void missingMetric() throws Exception {
        ThriftMonitoring.getMonitor().logMethodCall(
                ThriftCallType.SERVER,
                new ThriftClientAddress("server.com", 42),
                "DemoServer",
                "ping",
                300,
                ThriftRequestStatus.SUCCESS
        );
        assertTrue(ThriftMonitoring.isDisabled());
    }

}
