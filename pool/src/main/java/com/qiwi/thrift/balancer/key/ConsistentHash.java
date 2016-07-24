package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.utils.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

public class ConsistentHash<K, V> {
    private static final Logger log = LoggerFactory.getLogger(ConsistentHash.class);
    private static final int HASH_COUNT = 256;
    private static final MessageDigest MD5;

    private final long[] hashKeys;
    private final Object[] nodes;
    private final Object[] quorumNodes;
    private final ToLongFunction<K> hashFunction;
    private final int quorumSize;

    static {
        try {
            MD5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not exist", e);
        }
    }


    /**
     *
     * @param nodesList отсортированный список нод
     */
    public static <K, V> ConsistentHash<K, V> buildWithMd5(
            List<V> nodesList,
            Function<V, String> nodeNameGetter,
            Function<K, String> keyString
    ){
        return buildWithMd5(nodesList, nodeNameGetter, keyString, 1, false);
    }

    /**
     * @param nodesList отсортированный список нод
     */
    public static <K, V> ConsistentHash<K, V> buildWithMd5(
            List<V> nodesList,
            Function<V, String> nodeName,
            Function<K, String> keyString,
            int quorumSize,
            boolean throwForCollision
    ){
        return buildWithMd5ForNodeNames(
                nodesList,
                nodeName,
                key -> halfMd5Hash(keyString.apply(key)),
                quorumSize,
                throwForCollision
        );
    }

    /**
     * @param nodesList отсортированный список нод
     */
    public static <K, V> ConsistentHash<K, V> buildWithMd5ForNodeNames(
            List<V> nodesList,
            Function<V, String> nodeName,
            ToLongFunction<K> keyMapper,
            int quorumSize,
            boolean throwForCollision
    ){
        return new ConsistentHash<K, V>(
                nodesList,
                new FastNameHash<>(nodeName, HASH_COUNT),
                keyMapper,
                HASH_COUNT,
                quorumSize,
                throwForCollision
        );
    }

    /**
     *
     * @param nodesList отсортированный список нод
     */
    protected ConsistentHash(
            List<V> nodesList, 
            NodeHashFunction<V> nodeHashFunction,
            ToLongFunction<K> keyHashFunction, 
            int hashCount,
            int quorumSize, 
            boolean throwForCollision
    ) {
        this.hashFunction = Objects.requireNonNull(keyHashFunction, "hashFunction");
        this.quorumSize = Math.min(quorumSize, nodesList.size());
        TreeMap<Long, V> valueMap = new TreeMap<>();
        int realHashCount = hashCount / nodeHashFunction.hash(nodesList.get(0), 0).length;
        for (V node : nodesList) {
            for (int i = 0; i < realHashCount; i++) {
                long[] hashes = nodeHashFunction.hash(node, i);
                for (long hash : hashes) {
                    V put = valueMap.put(hash, node);
                    if (put != null/* && quorumSize > 1*/) {
                        log.error("Hash collision in ring between " + node + " and " + put);
                        if (throwForCollision) {
                            throw new RuntimeException("Hash collision in ring between " + node + " and " + put);
                        }
                    }
                }
            }
        }

        hashKeys = new long[valueMap.size()];
        // дополнительный элемент добавлен для зацикливания списка
        nodes = new Object[valueMap.size() + 1];

        int i = 0;
        for (Map.Entry<Long, V> entry : valueMap.entrySet()) {
            hashKeys[i] = entry.getKey();
            nodes[i] = entry.getValue();
            i++;
        }
        nodes[nodes.length - 1] = nodes[0];
        if (quorumSize == 1) {
            quorumNodes = nodes;
        } else {
            quorumNodes = buildQuorumNodes();
        }
    }

    private Object[] buildQuorumNodes() {
        int length = hashKeys.length;
        // дополнительный элемент добавлен для зацикливания списка
        Object[] result = new Object[(length + 1) * quorumSize];

        //HashSet<Object> unc = new LinkedHashSet<>(quorumSize * 2);
        List<Object> unc = new ArrayList<>(quorumSize * 2);
        Object[] tmpArray = new Object[quorumSize];
        for (int i = 0; i < length; i++) {
            int allRing = length + i;
            for (int k = i; k < allRing && unc.size() < quorumSize; k++) {
                Object node = nodes[k % length];
                if (!unc.contains(node)) {
                    unc.add(node);
                }
            }
            System.arraycopy(unc.toArray(tmpArray), 0, result, i * quorumSize, quorumSize);
            unc.clear();
        }
        System.arraycopy(result, 0, result, length * quorumSize, quorumSize);
        return result;
    }

    @SuppressWarnings("unchecked")
    public V getNode(K key){
        return (V) nodes[getRingPosition(key)];
    }

    @SuppressWarnings("unchecked")
    public Stream<V> getQuorum(K key){
        int ringPosition = getRingPosition(key);
        int startInclusive = ringPosition * quorumSize;
        return (Stream<V>) Arrays.stream(quorumNodes, startInclusive, startInclusive + quorumSize);
    }

    protected final int getRingPosition(K key){
        long hash = hashFunction.applyAsLong(key);
        int idx = Arrays.binarySearch(hashKeys, hash);
        if (idx < 0){
            idx = -idx - 1;
        }
        return idx;
    }

    public static String getHashString(String nodeName, long hashNumber){
        return nodeName + '-' + hashNumber;
    }

    private static class FastNameHash<V> implements NodeHashFunction<V> {
        private final Function<V, String> nodeName;
        private final byte[][] longToStringData;

        private V lastNode;
        private MessageDigest nodeNameHash;

        private FastNameHash(Function<V, String> nodeName, int hashPerNode) {
            this.nodeName = nodeName;
            longToStringData = new byte[hashPerNode][];
            for (int i = 0; i < hashPerNode; i++) {
                longToStringData[i] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            }
        }

        /**
         * @param node
         * @param hashNumber return halfMd5HashDouble(nodeName.apply(node) + '-' + hashNumber);
         * @return
         */
        @Override
        public long[] hash(V node, int hashNumber) {
            if (lastNode != node) {
                try {
                    nodeNameHash = (MessageDigest) MD5.clone();
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException("Unable to clone md5", e);
                }
                nodeNameHash.update(nodeName.apply(node).getBytes(StandardCharsets.UTF_8));
                nodeNameHash.update((byte) '-');
                lastNode = node;
            }
            byte[] str = longToStringData[hashNumber];
            //byte[] str = Long.toString(hashNumber).getBytes(StandardCharsets.UTF_8);
            MessageDigest digest;
            try {
                digest = (MessageDigest) nodeNameHash.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException("Unable to clone md5", e);
            }

            byte[] bytes = digest.digest(str);
            long[] longs = new long[2];
            longs[0] = ThriftUtils.parseLong(bytes);
            longs[1] = ThriftUtils.parseLong(bytes, 8);
            return longs;
        }
    }

    // Только для тестов
    long[] getHashKeys() {
        return hashKeys;
    }

    // Только для тестов
    Object[] getQuorumNodes() {
        return quorumNodes;
    }


    @SuppressWarnings({"OverlyComplexBooleanExpression", "MagicNumber"})
    public static long halfMd5Hash(String value){
        byte[] bytes = getMd5(value.getBytes(StandardCharsets.UTF_8));
        return ThriftUtils.parseLong(bytes);
    }

    @SuppressWarnings({"OverlyComplexBooleanExpression", "MagicNumber"})
    public static long[] halfMd5HashDouble(String value){
        byte[] bytes = getMd5(value.getBytes(StandardCharsets.UTF_8));
        long[] longs = new long[2];
        longs[0] = ThriftUtils.parseLong(bytes);
        longs[1] = ThriftUtils.parseLong(bytes, 8);
        return longs;
    }

    public static byte[] getMd5(byte[] value){
        try {
            MessageDigest md5 = (MessageDigest) MD5.clone();
            return md5.digest(value);
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Unable to clone md5", e);
        }
    }

    @FunctionalInterface
    public interface NodeHashFunction<V> {
        long[] hash(V node, int hashNumber);
    }

}
