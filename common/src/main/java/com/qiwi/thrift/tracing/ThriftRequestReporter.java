package com.qiwi.thrift.tracing;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.utils.ThriftRequestStatus;

import java.util.Optional;

/**
 * Для служебного использования
 */
public interface ThriftRequestReporter {
    ThriftRequestReporter NULL_REPORTER = new ThriftRequestReporter() {
        @Override
        public void requestBegin(String serviceName, String methodName, ThriftCallType callType) {
        }

        @Override
        public void requestEnd(
                String serviceName,
                String methodName,
                ThriftCallType callType,
                ThriftRequestStatus requestStatus,
                long latencyNanos,
                Optional<Throwable> exception
        ) {
        }
    };

    void requestBegin(String serviceName, String methodName, ThriftCallType callType);

    void requestEnd(
            String serviceName,
            String methodName,
            ThriftCallType callType,
            ThriftRequestStatus requestStatus,
            long latencyNanos,
            Optional<Throwable> exception
    );
}
