package com.qiwi.thrift.test;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TestUtils {
    public static final Random RANDOM = new Random();
    private static Set<Integer> usedPorts = Collections.newSetFromMap(new ConcurrentHashMap<>(128));

    public static int genThriftPort() {
        int port;
        do {
            port = RANDOM.nextInt(16000) + 16000;
        } while (!usedPorts.add(port));
        return port;
    }

}
