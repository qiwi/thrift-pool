package com.qiwi.thrift.test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class TestClock extends Clock {
    private Instant currentTime;


    public Instant getCurrentTime() {
        return currentTime == null? Instant.now(): currentTime;
    }

    public void setCurrentTime(Instant currentTime) {
        this.currentTime = currentTime;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return getCurrentTime();
    }
}
