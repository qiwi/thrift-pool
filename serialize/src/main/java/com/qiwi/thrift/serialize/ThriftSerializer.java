package com.qiwi.thrift.serialize;

import com.qiwi.thrift.utils.ThriftTooLongMessageException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Named
@Singleton
public class ThriftSerializer {
    private static final Logger log = LoggerFactory.getLogger(ThriftSerializer.class);
    public static final int DEFAULT_MAX_FRAME_SIZE = 10 * 1024 * 1024;
    private static final ConcurrentMap<Class<?>, Constructor<?>> constructorCache = new ConcurrentHashMap<>();

    private final ObjectPool<TDeserializer> deserializerPool;
    private final ObjectPool<TSerializer> serializerPool;
    private final long maxFrameSize;


    public ThriftSerializer() {
        this(DEFAULT_MAX_FRAME_SIZE);
    }

    public ThriftSerializer(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        poolConfig.setMinIdle(poolSize);
        poolConfig.setMaxIdle(poolSize);
        poolConfig.setMaxTotal(poolSize);
        TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory(maxFrameSize, maxFrameSize / 8);

        deserializerPool = new GenericObjectPool<>(
                new BasePooledObjectFactory<TDeserializer>() {
                    @Override
                    public TDeserializer create() throws Exception {
                        return new TDeserializer(protocolFactory);
                    }

                    @Override
                    public PooledObject<TDeserializer> wrap(TDeserializer obj) {
                        return new DefaultPooledObject<>(obj);
                    }
                },
                poolConfig
        );
        serializerPool = new GenericObjectPool<>(
                new BasePooledObjectFactory<TSerializer>() {
                    @Override
                    public TSerializer create() throws Exception {
                        return new TSerializer(protocolFactory);
                    }

                    @Override
                    public PooledObject<TSerializer> wrap(TSerializer obj) {
                        return new DefaultPooledObject<>(obj);
                    }
                },
                poolConfig
        );
    }

    public static <T> Constructor<T> getConstructor(Class<T> objectClass) {
        return (Constructor<T>)constructorCache.computeIfAbsent(objectClass, clazz -> {
            if (!TBase.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + clazz.getName() + " is not thrift class");
            }
            if (!Serializable.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + clazz.getName() + " is not serializable");
            }
            try {
                Constructor<T> constructor = (Constructor<T>)clazz.getConstructor();
                constructor.setAccessible(true);
                return constructor;
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Class " + clazz.getName() + " is not public");
            }
        });

    }

    /**
     *
     * @param clazz
     * @param data
     * @param <T>
     * @return
     * @throws ThriftSerializeException - при ошибке десериализации
     * @throws IllegalArgumentException - Если класс clazz не является объектом thrift
     */
    public <T extends TBase<T, ?>> T fromBytes(Class<T> clazz, byte[] data) throws ThriftSerializeException,
            IllegalArgumentException {
        Objects.requireNonNull(data, "data");
        if (data.length > maxFrameSize) {
            throw new ThriftTooLongMessageException("Unable to read object " + clazz.getName()
                    + ". It overrun size limit " + data.length);
        }
        Constructor<T> constructor = getConstructor(clazz);
        try {
            T object = constructor.newInstance();
            TDeserializer deserializer = deserializerPool.borrowObject();
            try {
                deserializer.deserialize(object, data);
                deserializerPool.returnObject(deserializer);
                return object;
            } catch (Exception ex) {
                try {
                    deserializerPool.invalidateObject(deserializer);
                } catch (Exception ex2) {
                    log.error("Unable to return object to pool", ex2);
                }
                throw ex;
            }
        } catch (TProtocolException e) {
           if (e.getType() == TProtocolException.SIZE_LIMIT) {
               throw new ThriftTooLongMessageException("Object contain too long field", e);
           } else {
               throw new ThriftSerializeException("Unable to deserialize " + clazz.getName(), e);
           }
        } catch (Exception e) {
            throw new ThriftSerializeException("Unable to deserialize " + clazz.getName(), e);
        }
    }

    public <T extends TBase<T, ?>> byte[] toByte(T data) throws ThriftSerializeException {
        Objects.requireNonNull(data, "data");
        try {
            TSerializer serializer = serializerPool.borrowObject();
            try {
                byte[] bytes = serializer.serialize(data);
                if (bytes.length >= maxFrameSize) {
                    throw new ThriftTooLongMessageException(
                            "Unable to write object " + data.getClass().getName()
                                    + ", it's too big. Size: " + bytes.length
                    );
                }
                return bytes;
            } finally {
                try {
                    serializerPool.returnObject(serializer);
                } catch (Exception ex) {
                    log.error("Unable to return object to pool", ex);
                }
            }
        } catch (ThriftTooLongMessageException e) {
            throw e;
        } catch (Exception e) {
            throw new ThriftSerializeException("Unable to serialize " + data.getClass().getName(), e);
        }

    }

}
