package com.qiwi.thrift.server;

import com.qiwi.thrift.tracing.ThriftLogContext;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class TThreadedSelectorServerWithIpLogging extends TThreadedSelectorServer {
    private static final Logger log = LoggerFactory.getLogger(TThreadedSelectorServerWithIpLogging.class);


    private static final MethodHandle socketGetter;

    static {
        MethodHandle getter = null;
        try {
            Field transport = FrameBuffer.class.getDeclaredField("trans_");
            transport.setAccessible(true);
            Field socketChanel = TNonblockingSocket.class.getDeclaredField("socketChannel_");
            socketChanel.setAccessible(true);
            MethodHandle transportGetter = MethodHandles.lookup().unreflectGetter(transport);
            MethodHandle transportGetterCasted = transportGetter.asType(MethodType.methodType(
                    TNonblockingSocket.class,
                    FrameBuffer.class
            ));
            MethodHandle socketChanelGetter = MethodHandles.lookup().unreflectGetter(socketChanel);
            getter = MethodHandles.filterArguments(socketChanelGetter, 0, transportGetterCasted);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.error("Unable to create socket address get", e);
        }
        socketGetter = getter;
    }


    /**
     * Create the server with the specified Args configuration
     *
     * @param args
     */
    public TThreadedSelectorServerWithIpLogging(Args args) {
        super(args);
    }



    @Override
    protected Runnable getRunnable(FrameBuffer frameBuffer) {
        return () -> {
            if (socketGetter != null) {
                try {
                    SocketChannel socketChannel = (SocketChannel) socketGetter.invoke(frameBuffer);
                    InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
                    String adr = remoteAddress.getAddress().toString();
                    ThriftLogContext.setClientAddress(adr);
                } catch (Throwable ex) {
                    log.error("Unable to resolve remote address", ex);
                }
            }
            frameBuffer.invoke();
        };
    }

}
