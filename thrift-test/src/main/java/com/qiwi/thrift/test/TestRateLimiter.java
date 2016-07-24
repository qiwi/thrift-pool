package com.qiwi.thrift.test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TestRateLimiter {
    public static final int NANOS_IN_SECOND = 1_000_000_000;
    public static final int NANOS_IN_MS = 1_000_000;
    private long maxRps;
    private double randomFactor;

    public TestRateLimiter(long maxRps) {
        this.maxRps = maxRps;
    }

    public void setMaxRps(long maxRps) {
        this.maxRps = maxRps;
    }

    public void setRandomFactor(double randomFactor) {
        this.randomFactor = randomFactor;
    }

    private final AtomicLong nextResponseTime = new AtomicLong(System.nanoTime());
    private final Random random = new Random(32057848934567320L);

    public void await() {
        long startTime = System.nanoTime();
        long requestTime = NANOS_IN_SECOND / maxRps + (long)(random.nextGaussian() * randomFactor);
        long current;
        long nextTime;
        do {
            current = nextResponseTime.get();
            nextTime = current + requestTime;
            if (nextTime < startTime - NANOS_IN_SECOND) {
                nextTime = startTime + requestTime;
            }
        } while (!nextResponseTime.compareAndSet(current, nextTime));
        if (current - startTime > NANOS_IN_MS) {
            try {
                Thread.sleep((current - startTime) / NANOS_IN_MS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
