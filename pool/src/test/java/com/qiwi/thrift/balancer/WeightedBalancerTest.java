package com.qiwi.thrift.balancer;

import com.qiwi.thrift.balancer.key.ConsistentHashTest;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

public class WeightedBalancerTest {
    private Random random;

    @Test(groups = "unit")
    public void normalize(){
        WeightedBalancer.Weight<String> weight1 = new WeightedBalancer.Weight<>("");
        WeightedBalancer.Weight<String> weight2 = new WeightedBalancer.Weight<>("");
        WeightedBalancer.Weight<String> weight3 = new WeightedBalancer.Weight<>("");
        List<WeightedBalancer.Weight<String>> items = Arrays.asList(weight1, weight2, weight3);

        weight1.setWeight(1);
        weight2.setWeight(1);
        weight3.setWeight(0);
        WeightedBalancer.normalize(items);
        assertEquals(weight1.getWeight(), 0.5, 0.000001);
        assertEquals(weight2.getWeight(), 0.5, 0.000001);
        assertEquals(weight3.getWeight(), 0.0, 0.000001);

        weight1.setWeight(3);
        weight2.setWeight(1);
        weight3.setWeight(1);
        WeightedBalancer.normalize(items);
        assertEquals(weight1.getWeight(), 0.6, 0.000001);
        assertEquals(weight2.getWeight(), 0.2, 0.000001);
        assertEquals(weight3.getWeight(), 0.2, 0.000001);

        weight1.setWeight(3);
        weight2.setWeight(2);
        weight3.setWeight(Double.NaN);
        WeightedBalancer.normalize(items);
        assertEquals(weight1.getWeight(), 0.6, 0.000001);
        assertEquals(weight2.getWeight(), 0.4, 0.000001);
        assertEquals(weight3.getWeight(), 0.0, 0.000001);

        weight1.setWeight(Double.POSITIVE_INFINITY);
        weight2.setWeight(Double.POSITIVE_INFINITY);
        weight3.setWeight(Double.NaN);
        WeightedBalancer.normalize(items);
        assertEquals(weight1.getWeight(), 0.5, 0.000001);
        assertEquals(weight2.getWeight(), 0.5, 0.000001);
        assertEquals(weight3.getWeight(), 0.0, 0.000001);
    }

    @Test(groups = "unit")
    public void getFromEmpty() throws Exception {
        random = new Random(72049230523L);
        WeightedBalancer<String> balancer = new WeightedBalancer<>(
                Collections.emptyList(),
                name -> 0.1,
                () -> random.nextDouble()
        );
        assertFalse(balancer.get().isPresent());
    }

    @Test(groups = "unit")
    public void getFromZeroWeight() throws Exception {
        int nodesCount = 3;
        double factor = 1.05;
        int requestCount = 100_000;
        List<String> nodesNames = ConsistentHashTest.genNodesName(3);
        random = new Random(72049230523L);
        WeightedBalancer<String> balancer = new WeightedBalancer<>(
                 nodesNames,
                name -> -1,
                () -> random.nextDouble()
        );

        Map<String, AtomicLong> map = new HashMap<>(nodesCount);
        for (int i = 0; i < requestCount; i++) {
            map.computeIfAbsent(balancer.get().get(), t -> new AtomicLong()).incrementAndGet();
        }
        List<Map.Entry<String, AtomicLong>> list = map.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> entry.getValue().get()))
                .collect(Collectors.toList());
        long min = list.get(0).getValue().get();
        long max = list.get(list.size() - 1).getValue().get();
        assertTrue(
                min * factor >= max,
                "min nodes " + min + " max nodes " + max + " factor " + max / (double)min
        );
    }

    @Test(groups = "unit")
    public void getLoadTest() throws Exception {
        int nodesCount = 12;
        int requestCount = 10_100_000;
        double factor = 1.05;
        List<String> nodesNames = ConsistentHashTest.genNodesName(nodesCount);
        random = new Random(72049230523L);
        WeightedBalancer<String> balancer = new WeightedBalancer<>(
                nodesNames,
                name -> {
                    switch (name) {
                        case "Node-0005":
                            return 0.00000000000000000001;
                        case "Node-0006":
                            return 0.01;
                        default:
                            return 0.1;
                    }
                },
            () -> random.nextDouble()
        );
        Map<String, AtomicLong> map = new HashMap<>(nodesCount);
        long nanos = System.nanoTime();

        for (int i = 0; i < requestCount; i++) {
            map.computeIfAbsent(balancer.get().get(), t -> new AtomicLong()).incrementAndGet();
        }

        assertFalse(map.containsKey("Node-0005"));
        assertEquals(map.size(), nodesCount - 1);
        AtomicLong nodeWithLimitLoad = map.remove("Node-0006");
        List<Map.Entry<String, AtomicLong>> list = map.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> entry.getValue().get()))
                .collect(Collectors.toList());

        long min = list.get(0).getValue().get();
        long max = list.get(list.size() - 1).getValue().get();
        double avg = list.stream().mapToDouble(t -> t.getValue().get()).average().getAsDouble();

        System.out.printf("Time  ms: %14.1f%n", ((System.nanoTime() - nanos) / 1000_000.0));
        System.out.printf("Rate tps: %,14.0f%n", ((double) requestCount) / ((System.nanoTime() - nanos) / 1_000_000_000.0));
        System.out.println("min nodes " + min + " max nodes " + max + " factor " + max / (double)min);
        System.out.println("node6 " + nodeWithLimitLoad.get() + " avg on other nodes " + (long)avg + " factor node6 " + nodeWithLimitLoad.get() / avg);

        assertTrue(
                min * factor >= max,
                "min nodes " + min + " max nodes " + max + " factor " + max / (double)min
        );
        assertTrue(
                nodeWithLimitLoad.get() * factor * 10.0 >= avg && nodeWithLimitLoad.get()* 10.0 <= avg * factor,
                "min node6 " + nodeWithLimitLoad.get() + " max nodes " + max + " factor " + nodeWithLimitLoad.get() * 10.0 / (double)avg
        );
    }

}
