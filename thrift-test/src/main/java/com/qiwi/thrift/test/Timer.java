package com.qiwi.thrift.test;

import java.util.concurrent.atomic.AtomicLong;

public class Timer {

    private final long start;
    private final AtomicLong latency = new AtomicLong();

    public Timer() {
        start = System.nanoTime();
    }

    public MeasurementLatency startLatency(){
        return new MeasurementLatency();
    }


    public void print(long count) {
        long time = System.nanoTime() - start;

        System.out.println("Exec time (sec): " + (time / 1_000_000_000d));
        System.out.println("Rps: " + count / (time / 1_000_000_000d));

        long latencyVal = latency.get();
        if (latencyVal == 0) {
            latencyVal = time;
        }
        System.out.println("latency: " + (latencyVal / 1_000_000d / count));
    }

    public class MeasurementLatency {
        private final long start;

        public MeasurementLatency() {
            start = System.nanoTime();
        }

        public void end() {
            latency.addAndGet(System.nanoTime() - start);
        }
    }

}
