package com.qiwi.thrift.demo;

import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import com.qiwi.thrift.test.TestRateLimiter;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class ServerImp implements DemoServer.Iface {
    private ConcurrentHashMap<Long, MapResult> results = new ConcurrentHashMap<>();

    @Override
    public boolean healthCheck() throws TException {
        return true;
    }

    @Override
    public MapResult request(String text, long id) throws TException {
        MapResult mapResult = new MapResult(Status.OK);
        mapResult.setData(doWork(text, id));
        return mapResult;
    }


    @Override
    public void requestFullAsync(long requestId, String text, long id) throws TException {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new TException(e);
        }
        MapResult mapResult = new MapResult(Status.OK);
        //mapResult.setData(doWork(text, id));
        results.put(requestId, mapResult);
    }

    @Override
    public MapResult requestWithError(String text, long id) throws TException {
        throw new TestException("Db not available!");
    }

    @Override
    public MapResult responseFullAsync(long requestId) throws TException {
        MapResult result = results.remove(requestId);
        if (result == null) {
            return new MapResult(Status.AWAIT);
        } else {
            return result;
        }
    }

    @Override
    public MapResult crash(ByteBuffer trash, String text) throws TException {
        throw new RuntimeException();
    }

    @Override
    public MapResult requestWithMap(Map<String, Long> data, long id) throws TException {
        MapResult result = new MapResult(Status.OK);
        result.setData(data);
        return result;
    }

    private static final Random random = new Random();
    private double errorRate = 0.0;

    public void setErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }

    private final TestRateLimiter rateLimiter = new TestRateLimiter(2000);
    public void setMaxRps(long maxRps) {
        rateLimiter.setMaxRps(maxRps);
    }

    @Override
    public Status loadTest() throws TestException {
        rateLimiter.await();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (random.nextDouble() < errorRate) {
            throw new TestException();
        } else {
            return Status.OK;
        }
    }

    private Map<String, Long> doWork(String text, long id) {
        Map<String, Long> result = new HashMap<>(10);
        for (long i = id; i < id + 10; i++) {
            result.put(text + "_" + i, i);
        }
        return result;
    }
}
