package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.Balancer;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@SuppressWarnings("MagicNumber")
public class BalancerConsistentHashTest {
    @Test(groups = "unit")
    public void get() throws Exception {
        BalancerConsistentHash<Long, String> hash1 = new BalancerConsistentHash<>(
                Arrays.asList("node1", "node2"),
                Balancer.empty(),
                node -> node,
                key -> ConsistentHash.halfMd5Hash(key.toString()),
                1
        );
        assertEquals(hash1.get(42L).get(), "node1");

        BalancerConsistentHash<Long, String> hash2 = new BalancerConsistentHash<>(
                Arrays.asList(),
                Balancer.empty(),
                node -> node,
                key -> ConsistentHash.halfMd5Hash(key.toString()),
                1
        );
        assertFalse(hash2.get(42L).isPresent());
    }

}
