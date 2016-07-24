package com.qiwi.thrift.balancer.key;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

public class ConsistentHashTest {

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
    }

    public static List<String> genNodesName(int count) {
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(String.format("Node-%04d", i));
        }
        return result;
    }


    /**
     * Такие сервисы как diproc сломаются - если изменится этот порядок
     * @throws Exception
     */
    @Test(groups = "unit")
    public void nodeNotReordered() throws Exception {
        List<String> nodesList = genNodesName(16);
        ConsistentHash<String, String> hash1 = ConsistentHash.buildWithMd5(
                nodesList,
                String::toString,
                String::toString,
                1,
                false
        );

        // Здесь хардкод поскольку если изменятся возвращаемые значнения, то сломаятся вся балансировка
        nodeListEquals(hash1, "t1", "Node-0001");
        nodeListEquals(hash1, "t2", "Node-0011");
        nodeListEquals(hash1, "t3", "Node-0004");
        nodeListEquals(hash1, "t4", "Node-0006");
        nodeListEquals(hash1, "t5", "Node-0011");
        nodeListEquals(hash1, "t6", "Node-0013");
        nodeListEquals(hash1, "t7", "Node-0006");
        nodeListEquals(hash1, "t8", "Node-0007");
        ConsistentHash<String, String> hash3 = ConsistentHash.buildWithMd5(
                nodesList,
                String::toString,
                String::toString,
                3,
                false
        );
        nodeListEquals(hash3, "t1", "Node-0001", "Node-0010", "Node-0011");
        nodeListEquals(hash3, "t2", "Node-0011", "Node-0006", "Node-0005");
        nodeListEquals(hash3, "t3", "Node-0004", "Node-0005", "Node-0014");
        nodeListEquals(hash3, "t4", "Node-0006", "Node-0012", "Node-0010");
        nodeListEquals(hash3, "t5", "Node-0011", "Node-0003", "Node-0009");
        nodeListEquals(hash3, "t6", "Node-0013", "Node-0014", "Node-0012");
        nodeListEquals(hash3, "t7", "Node-0006", "Node-0010", "Node-0007");
        nodeListEquals(hash3, "t8", "Node-0007", "Node-0013", "Node-0000");
    }

    @Test(groups = "unit")
    public void noNodes() throws Exception {
        List<String> nodesList = genNodesName(2);
        ConsistentHash<String, String> hash2 = ConsistentHash.buildWithMd5(
                nodesList,
                String::toString,
                String::toString,
                5,
                false
        );
        nodeListEquals(hash2, "t1", "Node-0001", "Node-0000");
        nodeListEquals(hash2, "t2", "Node-0001", "Node-0000");
        nodeListEquals(hash2, "t3", "Node-0001", "Node-0000");
        nodeListEquals(hash2, "t4", "Node-0000", "Node-0001");
        nodeListEquals(hash2, "t5", "Node-0000", "Node-0001");    }

    private void nodeListEquals(ConsistentHash<String, String> hash, String key, String... nodes) {
        List<String> list = hash.getQuorum(key).collect(Collectors.toList());
        List<String> expected = Arrays.asList(nodes);
        System.out.println("        nodeListEquals(hash, \"" + key + "\", \"" + String.join("\", \"", list) + "\");");
        assertEquals(list, expected);
    }

    @Test(groups = "unit")
    public void missingNode() throws Exception {
        int nodeCount = 128;
        int quorumSize = 5;
        int testCount = 100000;
        List<String> nodesList = genNodesName(nodeCount);
        ConsistentHash<String, String> consistentHash1 = ConsistentHash.buildWithMd5(
                nodesList,
                String::toString,
                String::toString,
                quorumSize,
                false
        );
        String victim = nodesList.get(nodeCount / 2);
        HashMap<String, List<String>> listOfQuorum = new HashMap<>();
        for (int i = 0; i < testCount; i++) {
            String key = "test-" + i;
            List<String> quorum = new ArrayList<>(quorumSize);
            consistentHash1.getQuorum(key)
                    .forEach(quorum::add);
            listOfQuorum.put(key, quorum);
        }

        nodesList.remove(victim);
        ConsistentHash<String, String> consistentHash2 = ConsistentHash.buildWithMd5(
                nodesList,
                String::toString,
                String::toString,
                quorumSize,
                false
        );

        long victimCount = 0;
        Set<String> quorum = new HashSet<>(quorumSize);
        for (Map.Entry<String, List<String>> entry : listOfQuorum.entrySet()) {
            consistentHash2.getQuorum(entry.getKey()).forEach(quorum::add);
            boolean isContainsVictim = false;
            for (String node : entry.getValue()) {
                if (node.equals(victim)) {
                    assertFalse(quorum.remove(node), entry.getKey());
                    isContainsVictim = true;
                } else {
                    assertTrue(quorum.remove(node), entry.getKey());
                }
            }
            if (isContainsVictim) {
                assertEquals(quorum.size(), 1, entry.getKey());
                victimCount++;
            } else {
                assertEquals(quorum.size(), 0, entry.getKey());
            }
            quorum.clear();
        }
        assertTrue(victimCount * 0.5 < testCount * quorumSize / ((double)nodeCount), "to many victims " + victimCount);
        assertTrue(victimCount * 2 > testCount * quorumSize / ((double)nodeCount), "to less victims " + victimCount);
    }

    public enum DistributionFunction{
        QUERIES,
        QUORUM_NODES,
        TOKENS_PER_NODE,
    }

    @Test(groups = "unit", dataProvider = "getDistributionTestDataProvider")
    public void distributionTest(int nodesCount, int requestCount, int quorumSize, double factor, DistributionFunction function) throws Exception {
        ConsistentHash<String, String> consistentHash = ConsistentHash.buildWithMd5(
                genNodesName(nodesCount),
                String::toString,
                String::toString,
                quorumSize,
                true
        );

        Map<String, AtomicLong> map = new HashMap<>(nodesCount);
        long nanos = System.nanoTime();

        switch (function){
            case QUERIES:
                if (quorumSize == 1) {
                    for (int i = 0; i < requestCount; i++) {
                        map.computeIfAbsent(consistentHash.getNode("test-" + i), t -> new AtomicLong()).incrementAndGet();
                    }
                } else {
                    for (int i = 0; i < requestCount; i++) {
                        consistentHash.getQuorum("test-"  + i)
                                .forEach(node -> map.computeIfAbsent(node, t -> new AtomicLong()).incrementAndGet());
                    }
                }
                break;
            case QUORUM_NODES:
                Arrays.stream(consistentHash.getQuorumNodes(), 0, consistentHash.getQuorumNodes().length - quorumSize)
                    .forEach(node -> map.computeIfAbsent((String) node, t-> new AtomicLong()).incrementAndGet());
                break;
            case TOKENS_PER_NODE:
                assertTrue(nodesCount / quorumSize > 5, "Integer overflow");
                long[] hashKeys = consistentHash.getHashKeys();
                Object[] quorumNodes = consistentHash.getQuorumNodes();
                for (int i = 0; i < quorumNodes.length - quorumSize; i++) {
                    Object node = quorumNodes[i];
                    long lastIdx;
                     if (i < quorumSize) {
                        lastIdx = hashKeys[hashKeys.length - 1];
                    } else {
                        lastIdx = hashKeys[i / quorumSize - 1];
                    }
                    long newIdx = hashKeys[i / quorumSize];
                    map.computeIfAbsent((String) node, t-> new AtomicLong()).addAndGet(newIdx - lastIdx);
                }
                break;
        }



        List<Map.Entry<String, AtomicLong>> list = map.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> entry.getValue().get()))
                .collect(Collectors.toList());
        assertEquals(map.size(), nodesCount);

        long min = list.get(0).getValue().get();
        long max = list.get(list.size() - 1).getValue().get();

        System.out.println("Time: " + ((System.nanoTime() - nanos) / 1000_000.0));
        System.out.println("Rate: " + ((double) requestCount) / ((System.nanoTime() - nanos) / 1_000_000_000.0));
        System.out.println("min nodes " + min + " max nodes " + max + " factor " + max / (double)min);
        /*for (Map.Entry<String, AtomicLong> entry : list) {
            System.out.println(entry.getValue() + " -> " + entry.getKey());
        }
*/
        assertTrue(
                min * factor >= max,
                "min nodes " + min + " max nodes " + max + " factor " + max / (double)min
        );
    }

    @DataProvider
    private Object[][] getDistributionTestDataProvider(){
        return new Object[][] {
                {  5,   5 * 128, 1, 1.5, DistributionFunction.QUERIES},
                {128, 128 * 512, 1, 1.5, DistributionFunction.QUERIES},
                {128, 128 * 128, 1, 1.0, DistributionFunction.QUORUM_NODES},
                {128, 128 * 128, 1, 1.5, DistributionFunction.TOKENS_PER_NODE},
                {  3,   3 *  64, 3, 1.5, DistributionFunction.QUERIES},
                {  5,   5 *  64, 3, 1.5, DistributionFunction.QUERIES},
                {  8,   8 *  64, 3, 1.5, DistributionFunction.QUERIES},
                {  8,   8 *  64, 5, 1.5, DistributionFunction.QUERIES},
                {128, 128 *  64, 5, 1.5, DistributionFunction.QUERIES},
                {128, 128 *  64, 5, 1.1, DistributionFunction.QUORUM_NODES},
                {128, 128 *  64, 5, 1.2, DistributionFunction.TOKENS_PER_NODE},
               // {128,10_000_000, 5, 1.2, DistributionFunction.QUERIES},
        };
    }

    @Test(groups = "manual")
    public void createSpeedTest() throws Exception {
        int COUNT = 500;
        List<String> nodesName = genNodesName(128);
        for (int i = 0; i < COUNT >> 1; i++) {
            ConsistentHash.buildWithMd5(
                    nodesName,
                    String::toString,
                    String::toString,
                    5,
                    true
            );
        }
        long nanos = System.nanoTime();
        for (int i = 0; i < COUNT; i++) {
            ConsistentHash.buildWithMd5(
                    nodesName,
                    String::toString,
                    String::toString,
                    5,
                    true
            );
        }
        System.out.println("Time: " +  ((System.nanoTime() - nanos) / 1000_000.0));
        System.out.println("Rate: " + ((double)COUNT) / ((System.nanoTime() - nanos) / 1_000_000_000.0));
    }

    @Test(groups = "manual")
    public void calcProbabilityOfHalfMd5HashCollisions() throws Exception {
        // Расчет вероятности колизии двух хэшей
        // В случае коллизи алгорим consistensy hash не гаратирует возращение одинаковых нод при кворуме, при добавлении/удалении нод в кольцо.
        final int NODE_COUNT = 128;
        final int TOKEN_PER_NODE = 256;
        double n = NODE_COUNT * TOKEN_PER_NODE;
        double hashRange = 2.0 * Long.MAX_VALUE;
        System.out.println(1.0 - Math.exp(-n*(n-1) / (2.0 * hashRange)));
    }

}
