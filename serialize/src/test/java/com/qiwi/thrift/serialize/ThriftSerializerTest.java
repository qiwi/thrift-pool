package com.qiwi.thrift.serialize;

import com.qiwi.thrift.pool.types.MapResult;
import com.qiwi.thrift.pool.types.Status;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class ThriftSerializerTest {
    private ThriftSerializer serializer;
    private MapResult mapResult;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        serializer = new ThriftSerializer();
        mapResult = new MapResult(Status.AWAIT);
        Map<String, Long> data = new HashMap<>();
        data.put("Key42", 42L);
        mapResult.setData(data);
    }

    @Test(groups = "unit")
    public void cycle() throws Exception {
        byte[] bytes = serializer.toByte(mapResult);
        MapResult mapResult2 = serializer.fromBytes(MapResult.class, bytes);

        assertTrue(bytes.length < 20);
        assertEquals(mapResult2, mapResult);
        assertNotSame(mapResult2, mapResult);
    }

    @Test(groups = "unit")
    public void toBytes() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        for (int i = 0; i < 5000; i++) {
            serializer.toByte(mapResult);
        }
        try {
            final int COUNT = 30_000;
            long start = System.nanoTime();
            for (int i = 0; i < COUNT; i++) {
                long id = i;
                executor.execute(() -> serializer.toByte(mapResult));
            }
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)){
                fail("Unable to complete test");
            }

            long time = System.nanoTime() - start;
            System.out.println("toBytes Exec time (sec): " + (time / 1_000_000_000d));
            System.out.println("toBytes Rps: " + COUNT / (time / 1_000_000_000d));
        } finally {
            executor.shutdown();
        }
    }

    @Test(groups = "unit")
    public void fromBytes() throws Exception {
        byte[] bytes = serializer.toByte(mapResult);
        for (int i = 0; i < 5000; i++) {
            serializer.fromBytes(MapResult.class, bytes);
        }
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            final int COUNT = 30_000;
            long start = System.nanoTime();
            for (int i = 0; i < COUNT; i++) {
                long id = i;
                executor.execute(() -> serializer.fromBytes(MapResult.class, bytes));
            }
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)){
                fail("Unable to complete test");
            }

            long time = System.nanoTime() - start;
            System.out.println("fromBytes Exec time (sec): " + (time / 1_000_000_000d));
            System.out.println("fromBytes Rps: " + COUNT / (time / 1_000_000_000d));
        } finally {
            executor.shutdown();
        }
    }
}
