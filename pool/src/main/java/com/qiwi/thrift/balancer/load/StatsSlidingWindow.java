package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.utils.ThriftRequestStatus;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StatsSlidingWindow {
    private final long periodMillis;
    private final int size;
    private final Clock clock;

    private volatile Bucket currentBucket;
    private final Bucket[] window;
    private long idx;

    public StatsSlidingWindow(Duration period, int size) {
        this(period, size, Clock.systemUTC());
    }

    public StatsSlidingWindow(Duration period, int size, Clock clock) {
        this.periodMillis = Objects.requireNonNull(period).toMillis();
        this.size = size;
        this.clock = clock;
        this.idx = size * 2L;
        this.window = new Bucket[size];
        reset();
    }

    public Bucket getCurrent() {
        long millis = clock.millis();
        long periodId = millis / periodMillis;
        Bucket bucket = currentBucket;
        if (bucket.id < periodId) {
            synchronized (window) {
                bucket = currentBucket;
                if (bucket.id < periodId) {
                    bucket = new Bucket(periodId, periodMillis);
                    currentBucket = bucket;
                    idx++;
                    window[(int) (idx % size)] = bucket;
                }
            }
        }
        return bucket;
    }



    public Bucket getStat() {
        return getStat(size);
    }


    public Bucket getStat(int count){
        if (count > size) {
            throw new IndexOutOfBoundsException("Size " + size + " count " + count);
        }
        long currentTimeMillis = clock.millis();
        synchronized (window) {
            long id = idx;
            long statPeriod = currentTimeMillis - window[(int) ((id - count + 1) % size)].id * periodMillis;
            Bucket result = new Bucket(id - count, statPeriod);
            for (int i = 0; i < count; i++) {
                Bucket bucket = window[(int) ((id - i) % size)];
                result.addOther(bucket);
            }
            return result;
        }
    }

    public void reset() {
        long periodId = clock.millis() / periodMillis;
        synchronized (window) {
            for (int i = 0; i < window.length; i++) {
                window[i] = new Bucket(periodId, periodMillis);
            }
            currentBucket = window[(int) (idx % size)];
        }
    }

    public long getPeriodMillis() {
        return periodMillis;
    }

    public int getSize() {
        return size;
    }

    public static class Bucket {
        private final long id;

        private final AtomicLong requestsBeginCount = new AtomicLong(0);
        private final AtomicLong requestsEndCount = new AtomicLong(0);
        private final AtomicLong requestsLatencySumNanos = new AtomicLong(0);
        private final AtomicLong requestsError = new AtomicLong(0);
        private final AtomicLong requestsDisconnect = new AtomicLong(0);
        private final long periodMills;


        private Bucket(long id, long periodMillis) {
            this.id = id;
            this.periodMills = periodMillis;
        }

        private Bucket addOther(Bucket other) {
            requestsBeginCount.addAndGet(other.requestsBeginCount.get());
            requestsDisconnect.addAndGet(other.requestsDisconnect.get());
            requestsLatencySumNanos.addAndGet(other.requestsLatencySumNanos.get());
            requestsError.addAndGet(other.requestsError.get());
            requestsEndCount.addAndGet(other.requestsEndCount.get());
            return this;
        }

        public void requestBegin() {
            requestsBeginCount.incrementAndGet();
        }

        public void requestEnd(ThriftRequestStatus requestStatus, long latencyNanos) {
            requestsEndCount.incrementAndGet();
            requestsLatencySumNanos.addAndGet(latencyNanos);
            switch (requestStatus) {
                case SUCCESS:
                case APP_ERROR:// Ошибки уровня приложения не считаются ошибками
                    break;
                case INTERNAL_ERROR:
                case UNEXPECTED_ERROR:
                    requestsError.incrementAndGet();
                    break;
                case CONNECTION_ERROR:
                    requestsError.incrementAndGet();
                    requestsDisconnect.incrementAndGet();
                    break;
                default:
                    throw new IllegalArgumentException("Status " + requestStatus + " not supported");
            }
        }

        public double getErrorRatio(long minRequestCount){
            return requestsError.get() / (double)  getRequestEndCount(minRequestCount);
        }

        public double getDisconnectRatio(long minRequestCount){
            return requestsDisconnect.get() / (double) getRequestEndCount(minRequestCount);
        }

        public double getRequestLatencyNanos(long minRequestCount){
            return requestsLatencySumNanos.get() / (double) getRequestEndCount(minRequestCount);
        }

        public double getLoad(long connectionCount, long waitersCount){
            double timeProducedNanos = connectionCount * TimeUnit.MILLISECONDS.toNanos(periodMills);
            double requestLatencyNanos = getRequestLatencyNanos(1);
            double timeConsumedNanos = (requestLatencyNanos + ThriftBalancerConfig.LATENCY_OFFSET_NANOS) * (requestsBeginCount.get() + waitersCount);
            return timeConsumedNanos / timeProducedNanos;
        }

        public long getRequestEndCount(long minRequestCount){
            return Math.max(requestsEndCount.get(), minRequestCount);
        }

        @Override
        public String toString() {
            return "Bucket{" +
                    "id=" + id +
                    ", requestsBeginCount=" + requestsBeginCount +
                    ", requestsEndsCount=" + requestsEndCount +
                    ", requestsLatencySumNanos=" + requestsLatencySumNanos +
                    ", requestsError=" + requestsError +
                    ", requestsDisconnect=" + requestsDisconnect +
                    ", periodMills=" + periodMills +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "StatsSlidingWindow{" +
                "periodMillis=" + periodMillis +
                ", window=" + Arrays.toString(window) +
                ", idx=" + idx +
                '}';
    }
}

