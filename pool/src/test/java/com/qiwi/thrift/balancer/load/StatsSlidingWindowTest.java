package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.test.TestClock;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@SuppressWarnings("MagicNumber")
public class StatsSlidingWindowTest {

    @Test(groups = "unit")
    public void getStat() throws Exception {
        Instant start = Instant.now();
        TestClock clock = new TestClock();
        clock.setCurrentTime(start.plusSeconds(23));
        StatsSlidingWindow slidingWindow = new StatsSlidingWindow(Duration.ofSeconds(1), 10, clock);

        slidingWindow.getCurrent()
                .requestEnd( ThriftRequestStatus.SUCCESS, 20);
        slidingWindow.getCurrent()
                .requestEnd(ThriftRequestStatus.SUCCESS, 40);

        StatsSlidingWindow.Bucket stat = slidingWindow.getStat();
        assertEquals(stat.getRequestEndCount(1), 2);
        assertEquals(stat.getRequestLatencyNanos(1), 30.0, 0.001);
        assertEquals(stat.getErrorRatio(1), 0.0, 0.001);

        clock.setCurrentTime(start.plusSeconds(25));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);

        stat = slidingWindow.getStat();
        assertEquals(stat.getRequestEndCount(1), 3);
        assertEquals(stat.getRequestLatencyNanos(1), 40.0, 0.001);
        assertEquals(stat.getErrorRatio(1), 0.3333, 0.001);

        clock.setCurrentTime(start.plusSeconds(26));
        slidingWindow.getCurrent()
                .requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(27));
        slidingWindow.getCurrent()
                .requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(28));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(29));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(30));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(31));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(32));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(33));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);
        clock.setCurrentTime(start.plusSeconds(34));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.CONNECTION_ERROR, 60);

        stat = slidingWindow.getStat();
        assertEquals(stat.getRequestEndCount(1), 10);
        assertEquals(stat.getRequestLatencyNanos(1), 60.0, 0.001);
        assertEquals(stat.getErrorRatio(1), 1.0, 0.001);
    }

    @Test(groups = "unit")
    public void getLoad() throws Exception {
        Instant start = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        TestClock clock = new TestClock();
        clock.setCurrentTime(start);
        StatsSlidingWindow slidingWindow = new StatsSlidingWindow(Duration.ofSeconds(1), 10, clock);

        slidingWindow.getCurrent().requestBegin();
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos( 600));
        clock.setCurrentTime(start.plusSeconds(1));

        slidingWindow.getCurrent().requestBegin();
        slidingWindow.getCurrent().requestBegin();
        slidingWindow.getCurrent().requestBegin();
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos(2000));
        clock.setCurrentTime(start.plusSeconds(2));
        slidingWindow.getCurrent().requestBegin();
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos( 500));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos( 500));
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, TimeUnit.MILLISECONDS.toNanos(1000));

        clock.setCurrentTime(start.plusSeconds(2).plusMillis(600));

        assertEquals(slidingWindow.getStat(2).getLoad(10, 0), 0.25, 0.001);
    }


    @Test(groups = "unit")
    public void reset() throws Exception {
        StatsSlidingWindow slidingWindow = new StatsSlidingWindow(Duration.ofSeconds(1), 10);
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, 20);
        slidingWindow.getCurrent().requestEnd(ThriftRequestStatus.SUCCESS, 40);
        slidingWindow.reset();
        StatsSlidingWindow.Bucket bucket = slidingWindow.getStat();
        assertEquals(bucket.getRequestEndCount(0), 0);
    }
}
