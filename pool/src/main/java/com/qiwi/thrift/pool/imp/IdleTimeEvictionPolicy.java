package com.qiwi.thrift.pool.imp;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.EvictionConfig;
import org.apache.commons.pool2.impl.EvictionPolicy;

public class IdleTimeEvictionPolicy<T> implements EvictionPolicy<T> {
    @Override
    public boolean evict(EvictionConfig config, PooledObject<T> underTest, int idleCount) {
        long createTime = System.currentTimeMillis() - underTest.getCreateTime();
        if (config.getIdleSoftEvictTime() < createTime ||
                (config.getIdleEvictTime() < underTest.getIdleTimeMillis() && config.getMinIdle() < idleCount)) {
            return true;
        }
        return false;
    }
}
